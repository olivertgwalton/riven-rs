<script lang="ts">
    import { cn } from "$lib/utils";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { formatBytes, getServiceDisplayName } from "$lib/helpers";
    import { formatDate } from "$lib/utils/date";
    import type { DebridUserInfo } from "$lib/gql/schema";

    let { services }: { services: DebridUserInfo[] } = $props();

    function daysLeft(premiumUntil: string | null | undefined): number | null {
        const ms = premiumUntil ? new Date(premiumUntil).getTime() : NaN;
        return Number.isNaN(ms) ? null : Math.ceil((ms - Date.now()) / 86_400_000);
    }

    const premiumMeta = {
        premium: {
            variant: "default" as const,
            class: "rounded-xl bg-amber-600/30 text-amber-300 hover:bg-amber-600/40",
            label: "Premium"
        },
        trial: {
            variant: "secondary" as const,
            class: "rounded-xl bg-blue-600/20 text-blue-300",
            label: "Trial"
        },
        expired: { variant: "destructive" as const, class: "rounded-xl", label: "Expired" }
    };
</script>

{#snippet Field({
    label,
    value,
    valueClass = "mt-0.5 text-sm font-medium text-neutral-100"
}: {
    label: string;
    value: string | number;
    valueClass?: string;
})}
    <div>
        <p class="text-xs font-medium text-neutral-400">{label}</p>
        <p class={valueClass}>{value}</p>
    </div>
{/snippet}

<section class="border-border/60 border-b py-8">
    <div class="mb-6 flex items-end justify-between gap-4">
        <h2 class="text-base font-semibold">Downloaders</h2>
        {#if services.length > 0}
            <span class="text-muted-foreground text-sm">{services.length} configured</span>
        {/if}
    </div>

    <div class="grid gap-x-8 gap-y-6 md:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-4">
        {#each services as downloader (downloader.store)}
            {@const premiumDaysLeft = daysLeft(downloader.premiumUntil)}
            {@const premium =
                premiumMeta[
                    downloader.subscriptionStatus === "premium" ||
                    downloader.subscriptionStatus === "trial"
                        ? downloader.subscriptionStatus
                        : "expired"
                ]}

            <div class="border-border/60 border-b pb-5">
                <div class="flex items-center justify-between gap-3">
                    <h3 class="text-base font-semibold text-neutral-50">
                        {getServiceDisplayName(downloader.store)}
                    </h3>
                    <Badge variant={premium.variant} class={premium.class}>{premium.label}</Badge>
                </div>

                <div class="mt-3 space-y-3">
                    {#if downloader.username || downloader.email}
                        {@render Field({
                            label: "Account",
                            value: downloader.username || downloader.email || "Unknown"
                        })}
                    {/if}

                    {#if downloader.subscriptionStatus === "premium" && (downloader.premiumUntil || premiumDaysLeft !== null)}
                        <div class="grid grid-cols-2 gap-3">
                            {#if downloader.premiumUntil}
                                {@render Field({
                                    label: "Expires",
                                    value: formatDate(downloader.premiumUntil) ?? "Unknown"
                                })}
                            {/if}
                            {#if premiumDaysLeft !== null}
                                {@render Field({
                                    label: "Days Left",
                                    value: premiumDaysLeft,
                                    valueClass: cn(
                                        "mt-0.5 text-sm font-semibold",
                                        premiumDaysLeft < 7
                                            ? "text-red-400"
                                            : premiumDaysLeft < 30
                                              ? "text-amber-300"
                                              : "text-green-400"
                                    )
                                })}
                            {/if}
                        </div>
                    {/if}

                    <div class="grid grid-cols-2 gap-3">
                        {#if typeof downloader.points === "number"}
                            {@render Field({
                                label: "Points",
                                value: downloader.points.toLocaleString()
                            })}
                        {/if}
                        {#if downloader.totalDownloadedBytes != null}
                            {@render Field({
                                label: "Downloaded",
                                value: formatBytes(downloader.totalDownloadedBytes)
                            })}
                        {/if}
                    </div>

                    {#if downloader.cooldownUntil}
                        <div
                            class="rounded-md bg-amber-600/20 p-2 text-xs font-medium text-amber-300">
                            Cooldown until {formatDate(downloader.cooldownUntil)}
                        </div>
                    {/if}
                </div>
            </div>
        {/each}
    </div>
</section>
