<script lang="ts">
    import PageShell from "$lib/components/page-shell.svelte";
    import { gqlClient } from "$lib/graphql-client";
    import { cn } from "$lib/utils";
    import type { PageData } from "./$types";
    import ActivityCard from "$lib/components/dashboard/activity-card.svelte";
    import LibraryChartsCard from "$lib/components/dashboard/library-charts-card.svelte";
    import ReleaseYearCard from "$lib/components/dashboard/release-year-card.svelte";
    import ServiceStatusCard from "$lib/components/dashboard/service-status-card.svelte";
    import DownloaderServicesGrid from "$lib/components/dashboard/downloader-services-grid.svelte";
    import WatchingNowCard from "$lib/components/dashboard/watching-now-card.svelte";
    import UsenetProvidersCard from "$lib/components/dashboard/usenet-providers-card.svelte";
    import UsenetActivityCard from "$lib/components/dashboard/usenet-activity-card.svelte";
    import UsenetHealthCard from "$lib/components/dashboard/usenet-health-card.svelte";
    import IndexerStatsCard from "$lib/components/dashboard/indexer-stats-card.svelte";
    import type {
        ActivePlaybackSession,
        DebridUserInfo,
        IndexerStats
    } from "$lib/gql/schema";
    import {
        ACTIVE_PLAYBACK_QUERY,
        EMPTY_TITLE_SUMMARY,
        INDEXER_STATS_QUERY,
        STATS_QUERY,
        USENET_HEALTH_QUERY,
        type ActivePlaybackResult,
        type DashboardStats,
        type IndexerStatsResult,
        type UsenetHealthResult
    } from "./queries";
    import { onMount } from "svelte";
    import { subscribeToRivenMediaEvents } from "$lib/services/riven-live-updates";

    let { data }: { data: PageData } = $props();

    let activePlaybackSessions = $state<ActivePlaybackSession[]>([]);
    let downloaderServices = $state<DebridUserInfo[]>([]);
    let statistics = $state<DashboardStats | undefined>(undefined);
    let usenet = $state<UsenetHealthResult>({
        nntpProviders: [],
        usenetStreamingHealth: null,
        usenetTitleHealth: [],
        usenetTitleHealthSummary: EMPTY_TITLE_SUMMARY,
        usenetTraffic: null
    });
    let indexerStats = $state<IndexerStats[]>([]);

    const serviceStatuses = $derived(
        (data as PageData & { services?: Record<string, boolean | null> }).services ?? null
    );
    const kpiCards = $derived.by(() => [
        {
            title: "Total Items",
            value: statistics?.stats.totalItems.toLocaleString()
        },
        {
            title: "Completed",
            value: statistics?.stats.completed.toLocaleString()
        },
        {
            title: "Incomplete",
            value: statistics?.stats.incompleteItems.toLocaleString(),
            tone: "warning" as const
        },
        {
            title: "Completion Rate",
            value: statistics ? `${statistics.stats.completionRate.toFixed(2)}%` : "0%"
        }
    ]);

    async function refreshDashboardStats() {
        statistics = await gqlClient<DashboardStats>(STATS_QUERY);
    }

    // Resolve streamed Promises from the load into local state.
    // data.statistics / activePlaybackSessions / downloaderServices are Promises —
    // returning them un-awaited from the load lets SvelteKit transition
    // immediately while data arrives in the background.
    $effect(() => {
        let cancelled = false;

        Promise.resolve(data.statistics).then((s) => {
            if (!cancelled && s != null) statistics = s;
        });
        Promise.resolve(data.activePlaybackSessions).then((sessions) => {
            if (!cancelled) activePlaybackSessions = sessions ?? [];
        });
        Promise.resolve(data.downloaderServices).then((services) => {
            if (!cancelled) downloaderServices = services ?? [];
        });
        Promise.resolve(data.indexerStats).then((rows) => {
            if (!cancelled) indexerStats = rows ?? [];
        });
        Promise.resolve(data.usenetHealth).then((health) => {
            if (!cancelled && health) usenet = health;
        });

        return () => {
            cancelled = true;
        };
    });

    $effect(() => {
        return subscribeToRivenMediaEvents(refreshDashboardStats);
    });

    onMount(() => {
        let cancelled = false;

        // Each poll keeps the last successful snapshot on transient failures.
        const refresh = async () => {
            try {
                const result = await gqlClient<ActivePlaybackResult>(ACTIVE_PLAYBACK_QUERY);
                if (!cancelled) activePlaybackSessions = result.activePlaybackSessions ?? [];
            } catch {
                // keep the last snapshot
            }
            try {
                const health = await gqlClient<UsenetHealthResult>(USENET_HEALTH_QUERY);
                if (!cancelled) {
                    usenet = {
                        nntpProviders: health.nntpProviders ?? [],
                        usenetStreamingHealth: health.usenetStreamingHealth ?? null,
                        usenetTitleHealth: health.usenetTitleHealth ?? [],
                        usenetTitleHealthSummary:
                            health.usenetTitleHealthSummary ?? EMPTY_TITLE_SUMMARY,
                        usenetTraffic: health.usenetTraffic ?? null
                    };
                }
            } catch {
                // keep the last snapshot
            }
            try {
                const result = await gqlClient<IndexerStatsResult>(INDEXER_STATS_QUERY);
                if (!cancelled) indexerStats = result.indexerStats ?? [];
            } catch {
                // keep the last snapshot
            }
        };

        const interval = window.setInterval(refresh, 15000);
        return () => {
            cancelled = true;
            window.clearInterval(interval);
        };
    });
</script>

<svelte:head>
    <title>Dashboard - Riven</title>
</svelte:head>

{#snippet KPICard({
    title,
    value,
    tone = "default"
}: {
    title: string;
    value: string | undefined;
    tone?: "default" | "warning";
})}
    <div class={cn("border-border/60 border-b py-5", tone === "warning" && "border-amber-600/30")}>
        <p class="text-sm font-medium text-neutral-300">{title}</p>
        <div
            class={cn(
                "mt-3 text-2xl font-semibold tracking-tight",
                tone === "warning" ? "text-amber-300" : "text-neutral-50"
            )}>
            {value}
        </div>
    </div>
{/snippet}

<PageShell class="mx-auto w-full max-w-7xl">
    <header class="border-border/60 border-b pb-6">
        <h1 class="text-3xl font-bold tracking-tight">Media Library Statistics</h1>
    </header>

    <section class="grid grid-cols-1 gap-x-10 gap-y-4 py-2 md:grid-cols-2 lg:grid-cols-4">
        {#each kpiCards as card (card.title)}
            {@render KPICard(card)}
        {/each}
    </section>

    <ActivityCard activity={statistics?.activity ?? {}} />
    <LibraryChartsCard stats={statistics?.stats} />
    <ReleaseYearCard data={statistics?.yearReleases ?? []} />
    <ServiceStatusCard statuses={serviceStatuses} />
    <DownloaderServicesGrid services={downloaderServices} />
    {#if indexerStats.length > 0}
        <IndexerStatsCard stats={indexerStats} />
    {/if}
    <WatchingNowCard sessions={activePlaybackSessions} />
    {#if usenet.nntpProviders.length > 0}
        <UsenetProvidersCard providers={usenet.nntpProviders} />
        <UsenetActivityCard health={usenet.usenetStreamingHealth} traffic={usenet.usenetTraffic} />
        <UsenetHealthCard titles={usenet.usenetTitleHealth} summary={usenet.usenetTitleHealthSummary} />
    {/if}
</PageShell>
