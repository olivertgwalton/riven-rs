<script lang="ts">
    import * as Chart from "$lib/components/ui/chart/index.js";
    import ResponsiveChartContainer from "$lib/components/media/riven/responsive-chart-container.svelte";
    import { BarChart } from "layerchart";
    import type { IndexerStats } from "./types";

    let { stats }: { stats: IndexerStats[] } = $props();

    const queryRows = $derived.by(() =>
        stats
            .map((s) => ({
                indexer: s.indexer,
                searchQueries: s.searchQueries,
                capsQueries: s.capsQueries,
                total: s.searchQueries + s.capsQueries
            }))
            .filter((row) => row.total > 0)
            .sort((a, b) => b.total - a.total)
    );

    const grabRows = $derived.by(() =>
        stats
            .map((s) => ({ indexer: s.indexer, successfulGrabs: s.successfulGrabs }))
            .filter((row) => row.successfulGrabs > 0)
            .sort((a, b) => b.successfulGrabs - a.successfulGrabs)
    );

    const queryConfig = {
        searchQueries: { label: "Search Queries", color: "var(--chart-1)" },
        capsQueries: { label: "Auth Queries", color: "var(--chart-2)" }
    } satisfies Chart.ChartConfig;

    const grabConfig = {
        successfulGrabs: { label: "Successful Grabs", color: "var(--chart-3)" }
    } satisfies Chart.ChartConfig;

    // Bars run horizontally so indexer names read straight off the axis:
    // "AnimeTosho (Usenet)" laid flat under a vertical bar either collides
    // with its neighbours or gets dropped.
    const padding = { top: 28, bottom: 28, left: 120, right: 16 };
    // Both charts share a height, keyed to whichever has more rows, so the two
    // columns end level even when an indexer has queries but no grabs yet.
    const chartHeight = $derived(
        `${Math.max(11, Math.max(queryRows.length, grabRows.length) * 2.75 + 4)}rem`
    );
</script>

<section class="border-border/60 grid gap-12 border-b py-8 lg:grid-cols-2">
    <div class="min-w-0">
        <h2 class="text-base font-semibold">Total Indexer Queries</h2>

        {#if queryRows.length === 0}
            <p class="mt-6 text-sm text-neutral-400">No indexer queries recorded yet.</p>
        {:else}
            <ResponsiveChartContainer
                config={queryConfig}
                class="mt-6 w-full"
                style="height: {chartHeight}">
                <BarChart
                    data={queryRows}
                    y="indexer"
                    orientation="horizontal"
                    seriesLayout="stack"
                    legend={{ placement: "top-left" }}
                    series={[
                        {
                            key: "searchQueries",
                            label: "Search Queries",
                            color: "var(--chart-1)"
                        },
                        { key: "capsQueries", label: "Auth Queries", color: "var(--chart-2)" }
                    ]}
                    {padding}>
                    {#snippet tooltip()}
                        <Chart.Tooltip />
                    {/snippet}
                </BarChart>
            </ResponsiveChartContainer>
        {/if}
    </div>

    <div class="min-w-0">
        <h2 class="text-base font-semibold">Total Indexer Successful Grabs</h2>

        {#if grabRows.length === 0}
            <p class="mt-6 text-sm text-neutral-400">No grabs recorded yet.</p>
        {:else}
            <ResponsiveChartContainer
                config={grabConfig}
                class="mt-6 w-full"
                style="height: {chartHeight}">
                <BarChart
                    data={grabRows}
                    y="indexer"
                    orientation="horizontal"
                    legend={{ placement: "top-left" }}
                    series={[
                        {
                            key: "successfulGrabs",
                            label: "Successful Grabs",
                            color: "var(--chart-3)"
                        }
                    ]}
                    {padding}>
                    {#snippet tooltip()}
                        <Chart.Tooltip />
                    {/snippet}
                </BarChart>
            </ResponsiveChartContainer>
        {/if}
    </div>
</section>
