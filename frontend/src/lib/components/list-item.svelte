<script lang="ts">
    import PortraitCard from "$lib/components/media/portrait-card.svelte";
    import ItemRequest from "$lib/components/media/riven/item-request.svelte";
    import StatusBadge from "$lib/components/media/status-badge.svelte";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { cn } from "$lib/utils";
    import { resolve } from "$app/paths";
    import {
        getLibraryStatus,
        markLibraryStatusRequested,
        getResolvedLibraryId
    } from "$lib/stores/library-status.svelte";

    const badgeVariantClasses: Record<string, string> = {
        success: "bg-green-600/90 text-white border-0",
        error: "bg-red-600/90 text-white border-0",
        default: "bg-yellow-600/90 text-white border-0"
    };

    let {
        data = $bindable(),
        indexer = $bindable<string | undefined>(),
        type = $bindable<string | undefined>(),
        isSelectable = false,
        selectStore = undefined,
        class: className = ""
    } = $props();

    // Normalize type for different indexers
    let normalizedType = $derived.by(() => {
        let t = type;
        if (indexer === "anilist" && !t) t = data.media_type;
        if (indexer === "anilist" && t) {
            // Anilist's raw MediaFormat enum (TV, TV_SHORT, MOVIE, SPECIAL,
            // OVA, ONA, MUSIC, ...) has no movie/tv split of its own — Riven
            // only distinguishes Movie vs Show, and everything that isn't a
            // standalone MOVIE is episodic/serialized content, which maps to
            // a Show. Passed straight through from the raw GraphQL response
            // (see lists-cache.svelte.ts), so it's uppercase, not "tv"/"movie".
            t = t.toUpperCase() === "MOVIE" ? "movie" : "tv";
        }
        if ((indexer === "tvdb" || indexer === "tmdb") && t === "show") t = "tv";
        // Ensure type is set if only in data
        if (!t && data.media_type) t = data.media_type;
        return t;
    });

    // Mirrors the fallback used elsewhere in this component: an absent
    // indexer has always meant "assume tmdb" for link-building (the
    // SearchStore-backed trending pages never tag their raw TMDB results
    // with one at all).
    const requestSource = $derived.by<"tmdb" | "tvdb" | "anilist" | null>(() => {
        if (indexer === "tmdb" || indexer === "tvdb" || indexer === "anilist") return indexer;
        if (indexer === undefined) return "tmdb";
        return null;
    });
    const requestMediaType = $derived.by<"movie" | "tv" | null>(() => {
        if (normalizedType === "movie" || normalizedType === "tv") return normalizedType;
        return null;
    });
    const requestExternalId = $derived(data.id != null ? String(data.id) : null);

    // Riven's library is keyed by tmdb-movie/tvdb-show ids only. A TMDB
    // *show* id and every Anilist id aren't usable directly and resolve
    // through the store first; `resolved` is the identity every downstream
    // lookup/link/mutation actually uses. `undefined` here just means "not
    // applicable" (missing id, or a type this feature doesn't cover, e.g.
    // person/company) — distinct from the store's own `"pending"`.
    //
    // This has to be plain $state updated from $effect, not $derived: the
    // store's resolver writes to its own cache as a side effect (to record
    // "pending" / cache the eventual result), and Svelte 5 throws
    // (`state_unsafe_mutation`) on a $state write reachable from inside a
    // $derived's evaluation — derived values must be pure. $effect is the
    // rune meant for exactly this "reading triggers a side effect" shape.
    let resolved = $state<ReturnType<typeof getResolvedLibraryId> | undefined>(undefined);
    $effect(() => {
        if (!requestSource || !requestExternalId || !requestMediaType) {
            resolved = undefined;
            return;
        }
        resolved = getResolvedLibraryId(requestSource, requestExternalId, requestMediaType);
    });

    let mediaURL = $derived.by(() => {
        if (!data.id) return null;
        if (normalizedType === "person" || normalizedType === "company") {
            return `/details/entity/${data.id}/${normalizedType}`;
        }

        if (
            (indexer === "tmdb" || indexer === "tvdb" || indexer === undefined) &&
            (normalizedType === "movie" || normalizedType === "tv")
        ) {
            const params: string[] = [];
            if (indexer === "tvdb") params.push("indexer=tvdb");
            if (data.details_query) {
                for (const [key, value] of Object.entries(data.details_query)) {
                    params.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
                }
            }
            const queryParam = params.length > 0 ? `?${params.join("&")}` : "";
            // If indexer is undefined, assume tmdb behavior for now as default
            return `/details/media/${data.id}/${normalizedType}${queryParam}`;
        }

        if (indexer === "anilist" && (normalizedType === "movie" || normalizedType === "tv")) {
            // There is no /details/anilist/... route — Anilist ids aren't
            // ones the details page understands at all. Resolve to the
            // equivalent TMDB/TVDB id first (same resolution the button
            // below uses) rather than link to a guaranteed 404; while
            // that's in flight, no link is better than a broken one.
            if (!resolved || resolved === "pending") return null;
            const queryParam = resolved.indexer === "tvdb" ? "?indexer=tvdb" : "";
            return `/details/media/${resolved.id}/${normalizedType}${queryParam}`;
        }

        return `/details/${indexer}${normalizedType ? `/${normalizedType}` : ""}/${data.id}`;
    });
    let subtitle = $derived.by(() => {
        const parts = [];
        if (data.media_type === "tv" || normalizedType === "tv") parts.push("TV");
        else if (data.media_type === "movie" || normalizedType === "movie") parts.push("Movie");
        else if (data.media_type === "person" || normalizedType === "person") parts.push("Person");
        else if (data.media_type === "company" || normalizedType === "company")
            parts.push("Studio");

        if (data.year && data.year !== "N/A") parts.push(data.year);
        return parts.join(" • ") || null;
    });

    // The Request/status button only makes sense for a movie/show with a
    // resolvable identity in Riven's own library id-space — "riven"
    // (already-resolved library items, e.g. Recently Added) and
    // person/company results aren't requestable content in the first place,
    // and `resolved === null` means an Anilist/TMDB-show id that genuinely
    // couldn't be matched to anything.
    const requestEligible = $derived(!!resolved && resolved !== "pending");

    let statusEntry = $state<ReturnType<typeof getLibraryStatus> | null>(null);
    $effect(() => {
        if (!requestSource || !requestExternalId || !requestMediaType) {
            statusEntry = null;
            return;
        }
        statusEntry = getLibraryStatus(requestSource, requestExternalId, requestMediaType);
    });

    // The footer's Request button/status pill lives inside the card's own <a>
    // (so it stays visually anchored to the poster), so a click on it would
    // otherwise also navigate to the details page. Rather than attach a click
    // handler to a non-interactive wrapper around it (which needs its own
    // keyboard handling and ARIA role to be accessible), the check lives on
    // the anchor itself — already a properly interactive element — and just
    // looks at whether the click originated inside the marked footer region.
    function handleCardClick(e: MouseEvent) {
        if ((e.target as HTMLElement | null)?.closest("[data-card-footer-action]")) {
            e.preventDefault();
        }
    }

    // Default container classes (w-full allows grid to control width)
    // Merged with passed className
    const containerClasses = $derived(
        cn(
            "group relative block w-full outline-none ring-offset-2 focus-visible:ring-2 focus-visible:ring-primary rounded-xl",
            className
        )
    );

    function getMediaHref(mediaURL: string) {
        const [pathname, search = ""] = mediaURL.split("?");

        if (pathname.startsWith("/details/media/")) {
            const [, , , id, mediaType] = pathname.split("/");
            const basePath = resolve("/(protected)/details/media/[id]/[mediaType]", {
                id,
                mediaType
            });
            return search ? `${basePath}?${search}` : basePath;
        }

        if (pathname.startsWith("/details/entity/")) {
            const [, , , id, type] = pathname.split("/");
            return resolve("/(protected)/details/entity/[id]/[type]", { id, type });
        }

        return mediaURL;
    }
</script>

{#snippet cardContent()}
    <PortraitCard
        title={data.title}
        {subtitle}
        image={data.poster_path}
        {isSelectable}
        isSelected={isSelectable && !!data.riven_id && selectStore?.has(data.riven_id)}
        onSelectToggle={() => selectStore?.toggle(data.riven_id)}>
        {#snippet topRight()}
            {#if data.badge}
                <Badge
                    class={cn(
                        "border-white/10 px-2 py-0.5 text-[10px] shadow-sm backdrop-blur-md",
                        badgeVariantClasses[data.badge.variant] || badgeVariantClasses.default
                    )}>{data.badge.text}</Badge>
            {/if}
        {/snippet}
        {#snippet footer()}
            {#if requestEligible && resolved && resolved !== "pending" && requestMediaType}
                {#if statusEntry === null || statusEntry.status === "loading"}
                    <span
                        class="inline-flex h-6 w-20 animate-pulse rounded-full bg-white/10 backdrop-blur-md"
                        aria-hidden="true"></span>
                {:else if statusEntry.status === "not_found"}
                    <div data-card-footer-action>
                        <ItemRequest
                            size="sm"
                            variant="secondary"
                            class="border-primary/50 text-primary hover:bg-primary/10 hover:text-primary hover:border-primary h-6 rounded-full border bg-black/40 px-3 text-[10px] font-semibold shadow-sm backdrop-blur-md"
                            title={data.title}
                            ids={[]}
                            mediaType={requestMediaType}
                            externalId={resolved.id}
                            onSuccess={(itemId) => {
                                // Captured by value: by the time this async
                                // callback runs, the reactive `resolved`
                                // binding could have moved on to a different
                                // value, which is why TS won't narrow it
                                // in-place here the way it does above.
                                const requestedId = resolved;
                                if (itemId != null && requestedId && requestedId !== "pending") {
                                    markLibraryStatusRequested(
                                        requestedId.indexer,
                                        requestedId.id,
                                        itemId
                                    );
                                }
                            }}>
                            Request
                        </ItemRequest>
                    </div>
                {:else}
                    <StatusBadge state={statusEntry.state} size="sm" class="border-white/10 border" />
                {/if}
            {/if}
        {/snippet}
    </PortraitCard>
{/snippet}

{#if mediaURL}
    <a href={getMediaHref(mediaURL)} class={containerClasses} onclick={handleCardClick}>
        {@render cardContent()}
    </a>
{:else}
    <div class={containerClasses}>
        {@render cardContent()}
    </div>
{/if}
