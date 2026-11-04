<script lang="ts">
    import { toast } from "svelte-sonner";
    import { invalidateAll } from "$app/navigation";
    import Disc from "@lucide/svelte/icons/disc-3";
    import PageShell from "#lib/components/page-shell.svelte";
    import StatusBadge from "#lib/components/media/status-badge.svelte";
    import { Button } from "#lib/components/ui/button/index.js";
    import { Checkbox } from "#lib/components/ui/checkbox/index.js";
    import { gqlClient } from "#lib/graphql-client.js";
    import { can } from "#lib/permissions.js";
    import { page } from "$app/state";
    import {
        REQUEST_ARTIST_MUTATION,
        type RequestArtistResponse
    } from "#lib/services/music.js";
    import type { PageProps } from "./$types";

    let { data }: PageProps = $props();

    const details = $derived(data.details);
    const artist = $derived(details.artist);
    const canRequest = $derived(can(page.data.permissions, "REQUEST_ITEMS"));

    // Release groups by primary type, in MusicBrainz's order of importance.
    const TYPE_ORDER = ["Album", "EP", "Single"];
    const groups = $derived.by(() => {
        const byType = new Map<string, typeof details.albums>();
        for (const album of details.albums) {
            const type = album.primaryType ?? "Other";
            byType.set(type, [...(byType.get(type) ?? []), album]);
        }
        return [...byType.entries()].sort(
            ([a], [b]) =>
                (TYPE_ORDER.indexOf(a) + 1 || TYPE_ORDER.length + 1) -
                (TYPE_ORDER.indexOf(b) + 1 || TYPE_ORDER.length + 1)
        );
    });

    let selected = $state<Set<string>>(new Set());
    let requesting = $state(false);
    let brokenCovers = $state<Set<string>>(new Set());

    function toggle(id: string) {
        const next = new Set(selected);
        if (next.has(id)) next.delete(id);
        else next.add(id);
        selected = next;
    }

    /** `albums` omitted follows the whole discography. */
    async function request(albums?: string[]) {
        requesting = true;
        try {
            const result = await gqlClient<RequestArtistResponse>(REQUEST_ARTIST_MUTATION, {
                input: { name: artist.name, musicbrainzId: artist.id, albums }
            });
            if (result.requestArtist.success) {
                toast.success(result.requestArtist.message);
                selected = new Set();
                await invalidateAll();
            } else {
                toast.info(result.requestArtist.message);
            }
        } catch (error) {
            toast.error(error instanceof Error ? error.message : "Failed to request artist");
        } finally {
            requesting = false;
        }
    }

    // `NaiveDate` reaches the client as an ISO date string.
    function year(date: unknown) {
        return typeof date === "string" ? date.slice(0, 4) : null;
    }
</script>

<svelte:head>
    <title>{artist.name} - Riven</title>
</svelte:head>

<PageShell>
    <div class="mx-auto w-full max-w-6xl space-y-8">
        <header class="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
            <div class="min-w-0">
                <h1 class="truncate text-3xl font-semibold tracking-tight">{artist.name}</h1>
                <p class="text-muted-foreground mt-1 text-sm">
                    {[artist.kind, artist.country, artist.disambiguation]
                        .filter(Boolean)
                        .join(" • ")}
                </p>
            </div>
            {#if canRequest}
                <div class="flex shrink-0 flex-wrap gap-2">
                    <Button
                        variant="outline"
                        disabled={requesting || selected.size === 0}
                        onclick={() => request([...selected])}>
                        Request {selected.size || ""} selected
                    </Button>
                    <Button
                        disabled={requesting || details.followsDiscography}
                        onclick={() => request()}>
                        {details.followsDiscography ? "Following discography" : "Follow discography"}
                    </Button>
                </div>
            {/if}
        </header>

        {#each groups as [type, albums] (type)}
            <section class="space-y-3">
                <h2 class="text-lg font-semibold">{type === "EP" ? "EPs" : `${type}s`}</h2>
                <div class="grid grid-cols-2 gap-4 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-6">
                    {#each albums as album (album.id)}
                        {@const requested = album.state != null}
                        <div class="group space-y-2">
                            <div class="bg-muted relative aspect-square overflow-hidden rounded-lg">
                                {#if brokenCovers.has(album.id)}
                                    <div
                                        class="text-muted-foreground flex h-full items-center justify-center">
                                        <Disc class="size-10" />
                                    </div>
                                {:else}
                                    <img
                                        src={album.coverUrl}
                                        alt={album.title}
                                        loading="lazy"
                                        class="h-full w-full object-cover"
                                        onerror={() =>
                                            (brokenCovers = new Set(brokenCovers).add(album.id))} />
                                {/if}
                                {#if album.state}
                                    <StatusBadge state={album.state} class="absolute top-2 left-2" />
                                {:else if canRequest}
                                    <Checkbox
                                        class="absolute top-2 right-2 bg-black/60"
                                        checked={selected.has(album.id)}
                                        aria-label="Select {album.title}"
                                        onCheckedChange={() => toggle(album.id)} />
                                {/if}
                            </div>
                            <div class="min-w-0">
                                <p class="truncate text-sm font-medium" title={album.title}>
                                    {album.title}
                                </p>
                                <p class="text-muted-foreground text-xs">
                                    {year(album.releaseDate) ?? "Unknown year"}{requested
                                        ? " • In library"
                                        : ""}
                                </p>
                            </div>
                        </div>
                    {/each}
                </div>
            </section>
        {:else}
            <p class="text-muted-foreground text-sm">MusicBrainz lists no releases for this artist.</p>
        {/each}
    </div>
</PageShell>
