<script lang="ts">
    import PortraitCard from "$lib/components/media/portrait-card.svelte";
    import ItemRequest from "$lib/components/media/riven/item-request.svelte";
    import StatusBadge from "$lib/components/media/status-badge.svelte";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { cn } from "$lib/utils";
    import { resolve } from "$app/paths";
    import { getLibraryStatus, markLibraryStatusRequested } from "$lib/stores/library-status.svelte";

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
        if ((indexer === "tvdb" || indexer === "tmdb") && t === "show") t = "tv";
        // Ensure type is set if only in data
        if (!t && data.media_type) t = data.media_type;
        return t;
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

    // The Request/status button only makes sense for a movie/show sourced
    // from an external indexer with a usable id — "riven" (already-resolved
    // library items) and "anilist" (no tmdb/tvdb id available at all) can't
    // be requested or status-checked this way, and person/company results
    // aren't requestable content in the first place.
    const requestMediaType = $derived.by<"movie" | "tv" | null>(() => {
        if (normalizedType === "movie" || normalizedType === "tv") return normalizedType;
        return null;
    });
    // Mirrors `mediaURL`'s own fallback below: some result sources (the raw
    // TMDB search/discover results behind the dedicated trending pages, via
    // SearchStore) never tag their items with an `indexer` at all, and an
    // absent indexer has always meant "assume tmdb" for this component's own
    // link-building — treating it any differently here just means the button
    // silently never renders for those pages instead of erroring loudly.
    const requestIndexer = $derived.by<"tmdb" | "tvdb" | null>(() => {
        if (indexer === "tmdb" || indexer === "tvdb") return indexer;
        if (indexer === undefined) return "tmdb";
        return null;
    });
    const requestExternalId = $derived(data.id != null ? String(data.id) : null);
    const requestEligible = $derived(
        !!requestMediaType && !!requestIndexer && !!requestExternalId
    );

    let statusEntry = $state<ReturnType<typeof getLibraryStatus> | null>(null);
    $effect(() => {
        if (!requestEligible || !requestIndexer || !requestExternalId || !requestMediaType) {
            statusEntry = null;
            return;
        }
        statusEntry = getLibraryStatus(requestIndexer, requestExternalId, requestMediaType);
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
            {#if requestEligible && requestIndexer && requestExternalId && requestMediaType}
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
                            externalId={requestExternalId}
                            onSuccess={(itemId) => {
                                if (itemId != null) {
                                    markLibraryStatusRequested(
                                        requestIndexer,
                                        requestExternalId,
                                        requestMediaType,
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
