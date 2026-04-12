-- KEYS[1]: job data hash       (e.g. "{queue}:data")
-- KEYS[2]: job meta hash prefix (e.g. "{queue}:meta")
-- KEYS[3]: active jobs list     (e.g. "{queue}:active")   — for Pending
-- KEYS[4]: done jobs sorted set (e.g. "{queue}:done")     — for Done
-- KEYS[5]: failed jobs zset     (e.g. "{queue}:failed")   — for Failed
-- KEYS[6]: dead/killed jobs zset(e.g. "{queue}:dead")     — for Killed
-- KEYS[7]: workers sorted set   (e.g. "{queue}:workers")  — for Running
-- ARGV[1]: status filter ("Pending","Done","Failed","Killed","Running", or "")
-- ARGV[2]: page number (1-based)
-- ARGV[3]: page size

local status_filter = ARGV[1]
local page = tonumber(ARGV[2])
local page_size = tonumber(ARGV[3])

if not page or not page_size then
  return redis.error_reply("missing pagination parameters")
end

local start = (page - 1) * page_size
local stop = start + page_size - 1

local task_ids = {}

if status_filter == "Done" then
    -- Done jobs are in a sorted set (score = completion timestamp), newest first
    task_ids = redis.call("zrevrange", KEYS[4], start, stop)
elseif status_filter == "Failed" then
    task_ids = redis.call("zrevrange", KEYS[5], start, stop)
elseif status_filter == "Killed" then
    task_ids = redis.call("zrevrange", KEYS[6], start, stop)
elseif status_filter == "Running" then
    -- Running jobs live in per-worker inflight sets; worker set keys are stored
    -- as the member values of the workers sorted set
    local workers = redis.call("zrange", KEYS[7], 0, -1)
    local all_running = {}
    for _, worker in ipairs(workers) do
        local ids = redis.call("smembers", worker)
        for _, id in ipairs(ids) do
            table.insert(all_running, id)
        end
    end
    for i = start + 1, math.min(stop + 1, #all_running) do
        table.insert(task_ids, all_running[i])
    end
else
    -- Pending / Queued / no filter: read from the active list
    task_ids = redis.call("lrange", KEYS[3], start, stop)
end

if not task_ids or #task_ids == 0 then
  return { {}, {} }
end

local job_data_list = {}
local meta_list = {}

for _, task_id in ipairs(task_ids) do
  local meta_key = KEYS[2] .. ":" .. task_id
  local meta_fields = redis.call("hgetall", meta_key)
  local data = redis.call("hmget", KEYS[1], task_id)
  table.insert(job_data_list, data[1])
  table.insert(meta_fields, 1, task_id)
  table.insert(meta_list, meta_fields)
end

return { job_data_list, meta_list }
