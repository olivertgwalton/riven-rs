-- Moves Redis written by apalis-redis 1.0.0-rc.8 to 1.0's layout. Only rc.8
-- writes `core::apalis::queues::list`, and this deletes it last, so on a Redis
-- already migrated (or new) it does nothing.
-- Keeps every task that is queued, scheduled or claimed (claims are requeued)
-- and drops everything else: history, worker registrations, and payloads rc.8
-- leaked that no list or set refers to.
-- Returns {queues, kept, requeued, dropped}.
if redis.call("EXISTS", "core::apalis::queues::list") == 0 then
    return {0, 0, 0, 0}
end

local queues = {}
for _, pattern in ipairs({"riven:*:data", "riven:*:workers", "riven:*:active", "riven:*:scheduled"}) do
    for _, key in ipairs(redis.call("KEYS", pattern)) do
        queues[string.match(key, "^(.*):[a-z]+$")] = true
    end
end

local count, kept, requeued, dropped = 0, 0, 0, 0
for q in pairs(queues) do
    count = count + 1
    local data, active, meta = q .. ":data", q .. ":active", q .. ":meta"

    -- rc.8 registered a worker under its inflight set's name, 1.0 under the
    -- worker's own, so 1.0 would never reap an rc.8 claim: requeue them.
    for _, inflight in ipairs(redis.call("KEYS", q .. ":inflight:*")) do
        for _, id in ipairs(redis.call("SMEMBERS", inflight)) do
            redis.call("RPUSH", active, id)
            redis.call("HSET", meta .. ":" .. id, "status", "Pending")
            redis.call("HDEL", meta .. ":" .. id, "locked_at", "locked_by")
            requeued = requeued + 1
        end
        redis.call("DEL", inflight)
    end
    for _, key in ipairs(redis.call("KEYS", q .. ":workers*")) do
        redis.call("DEL", key)
    end
    -- rc.8 kept history in sorted sets under the keys 1.0 writes plain sets to.
    redis.call("DEL", q .. ":done", q .. ":dead", q .. ":failed", meta .. ":result")

    local keep = {}
    for _, id in ipairs(redis.call("LRANGE", active, 0, -1)) do
        if redis.call("HEXISTS", data, id) == 1 then
            keep[id] = true
        else
            -- An id with no data fails 1.0's whole fetch.
            redis.call("LREM", active, 0, id)
        end
    end
    for _, id in ipairs(redis.call("ZRANGE", q .. ":scheduled", 0, -1)) do
        keep[id] = true
    end
    for _, id in ipairs(redis.call("HKEYS", data)) do
        if keep[id] then
            kept = kept + 1
        else
            redis.call("HDEL", data, id)
            dropped = dropped + 1
        end
    end
    local prefix = meta .. ":"
    for _, key in ipairs(redis.call("KEYS", prefix .. "*")) do
        if not keep[string.sub(key, #prefix + 1)] then
            redis.call("DEL", key)
        end
    end

    if redis.call("LLEN", active) > 0 then
        redis.call("DEL", q .. ":signal")
        redis.call("LPUSH", q .. ":signal", 1)
    end
end
redis.call("DEL", "core::apalis::queues::list")

return {count, kept, requeued, dropped}
