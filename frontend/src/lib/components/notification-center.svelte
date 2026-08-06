<script lang="ts">
    import { notificationStore, type Notification } from "$lib/stores/notifications.svelte";
    import { onMount, onDestroy } from "svelte";
    import { goto } from "$app/navigation";
    import { resolve } from "$app/paths";
    import * as Popover from "$lib/components/ui/popover/index.js";
    import { Button } from "$lib/components/ui/button/index.js";
    import { Badge } from "$lib/components/ui/badge/index.js";
    import { Separator } from "$lib/components/ui/separator/index.js";
    import Bell from "@lucide/svelte/icons/bell";
    import BellRing from "@lucide/svelte/icons/bell-ring";
    import Check from "@lucide/svelte/icons/check";
    import CheckCheck from "@lucide/svelte/icons/check-check";
    import Trash2 from "@lucide/svelte/icons/trash-2";
    import X from "@lucide/svelte/icons/x";
    import Tooltip from "./tooltip.svelte";
    import { toast } from "svelte-sonner";
    import { cn } from "$lib/utils";

    let open = $state(false);

    // Deduplicated notifications carry a stable dedupeKey. Passing that as the
    // toast `id` makes svelte-sonner update the existing toast in-place rather
    // than mounting a new animated node.
    function handleBatchAdded(batch: readonly Notification[]) {
        for (const n of batch) {
            const description = n.message;
            const opts = {
                description,
                duration: 5000,
                ...(n.dedupeKey !== null ? { id: n.dedupeKey } : {})
            };
            if (n.severity === "error") toast.error(n.title, opts);
            else if (n.severity === "warning") toast.warning(n.title, opts);
            else toast.success(n.title, opts);
        }
    }

    onMount(() => {
        notificationStore.onBatchAdded = handleBatchAdded;
        notificationStore.connect();
    });

    onDestroy(() => {
        notificationStore.onBatchAdded = null;
        notificationStore.disconnect();
    });

    function formatTimestamp(timestamp: string): string {
        const date = new Date(timestamp);
        const now = new Date();
        const diffMs = now.getTime() - date.getTime();
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);

        if (diffMins < 1) return "Just now";
        if (diffMins < 60) return `${diffMins}m ago`;
        if (diffHours < 24) return `${diffHours}h ago`;
        if (diffDays < 7) return `${diffDays}d ago`;
        return date.toLocaleDateString();
    }

    function getTypeColor(type: string): string {
        switch (type) {
            case "movie":
                return "bg-blue-500/10 text-blue-500";
            case "show":
                return "bg-purple-500/10 text-purple-500";
            case "season":
                return "bg-green-500/10 text-green-500";
            case "episode":
                return "bg-orange-500/10 text-orange-500";
            default:
                return "bg-gray-500/10 text-gray-500";
        }
    }

    // TMDB has no separate "episode" root id — a season/episode notification's
    // tmdbId is already the id of the show it belongs to, so every type maps
    // to a `/details/media/{tmdbId}/{mediaType}` link, "show"/"season"/
    // "episode" all landing on the same show page "movie" would.
    function detailsHref(notification: Notification): string | null {
        const mediaType = notification.type === "movie" ? "movie" : "tv";
        if (notification.tmdbId) {
            return resolve(`/details/media/${notification.tmdbId}/${mediaType}`);
        }
        // Shows/seasons/episodes are frequently TVDB-only — no TMDB match at
        // all — so tmdbId alone would leave most TV notifications
        // unclickable. `?indexer=tvdb` tells the details route to resolve
        // the id as a TVDB id instead, same as the rest of the app does for
        // TVDB-sourced content (see list-item.svelte).
        if (notification.tvdbId) {
            return resolve(`/details/media/${notification.tvdbId}/${mediaType}?indexer=tvdb`);
        }
        return null;
    }

    function handleNotificationClick(notification: Notification) {
        const href = detailsHref(notification);
        if (!href) return;
        if (!notification.read) notificationStore.markAsRead(notification.id);
        open = false;
        goto(href);
    }

    function handleMarkAsRead(id: string) {
        notificationStore.markAsRead(id);
    }

    function handleMarkAllAsRead() {
        notificationStore.markAllAsRead();
        toast.success("All notifications marked as read");
    }

    function handleClearAll() {
        notificationStore.clear();
        toast.success("All notifications cleared");
    }

    function handleClose() {
        open = false;
    }

    function handleRemove(id: string) {
        notificationStore.remove(id);
    }

    let {
        class: className,
        variant = "outline",
        side = "bottom",
        align = "end",
        sideOffset = 4
    } = $props<{
        class?: string;
        variant?: "default" | "destructive" | "outline" | "secondary" | "ghost" | "link";
        side?: "top" | "right" | "bottom" | "left";
        align?: "start" | "center" | "end";
        sideOffset?: number;
    }>();
</script>

<Popover.Root bind:open>
    <Popover.Trigger>
        {#snippet child({ props })}
            <Button
                {variant}
                size="icon"
                class={cn("h-10 w-10 cursor-pointer", className)}
                {...props}>
                {#if notificationStore.unreadCount > 0}
                    <BellRing class="text-primary fill-primary/10 size-5" />
                {:else}
                    <Bell class="size-5" />
                {/if}
            </Button>
        {/snippet}
    </Popover.Trigger>
    <Popover.Content
        class="w-96 rounded-2xl border border-white/10 bg-zinc-950/95 p-0 shadow-2xl shadow-black/50 backdrop-blur-2xl"
        {side}
        {align}
        {sideOffset}>
        <div class="flex flex-col">
            <div class="flex items-center justify-between p-4 pb-3">
                <h3 class="text-sm font-semibold">Notifications</h3>
                <div class="flex items-center gap-1">
                    {#if notificationStore.unreadCount > 0}
                        <Tooltip>
                            {#snippet trigger()}
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    class="text-muted-foreground hover:text-foreground h-7 w-7"
                                    onclick={handleMarkAllAsRead}>
                                    <CheckCheck class="size-3.5" />
                                </Button>
                            {/snippet}
                            {#snippet content()}
                                <p>Mark all as read</p>
                            {/snippet}
                        </Tooltip>
                    {/if}
                    {#if notificationStore.notifications.length > 0}
                        <Tooltip>
                            {#snippet trigger()}
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    class="text-muted-foreground hover:text-destructive h-7 w-7"
                                    onclick={handleClearAll}>
                                    <Trash2 class="size-3.5" />
                                </Button>
                            {/snippet}
                            {#snippet content()}
                                <p>Clear all notifications</p>
                            {/snippet}
                        </Tooltip>
                    {/if}
                    <Tooltip>
                        {#snippet trigger()}
                            <Button
                                variant="ghost"
                                size="icon"
                                class="text-muted-foreground hover:text-foreground h-7 w-7"
                                onclick={handleClose}>
                                <X class="size-3.5" />
                            </Button>
                        {/snippet}
                        {#snippet content()}
                            <p>Close</p>
                        {/snippet}
                    </Tooltip>
                </div>
            </div>
            <Separator />

            <div class="notification-scroll max-h-100 overflow-y-auto">
                {#if notificationStore.notifications.length === 0}
                    <div class="flex flex-col items-center justify-center p-8 text-center">
                        <div
                            class="bg-muted/20 mb-4 flex items-center justify-center rounded-full p-4">
                            <Bell class="text-muted-foreground/50 size-8" />
                        </div>
                        <p class="text-foreground text-sm font-medium">No notifications yet</p>
                        <p class="text-muted-foreground/70 mt-1 text-xs">
                            You'll be notified when items complete
                        </p>
                    </div>
                {:else}
                    {#snippet notificationBody(notification: Notification)}
                        <div class="flex items-start justify-between gap-2">
                            <div class="flex-1 space-y-1">
                                <div class="flex items-center gap-2">
                                    <Badge
                                        variant="secondary"
                                        class="{getTypeColor(notification.type)} text-[10px]">
                                        {notification.type}
                                    </Badge>
                                    {#if notification.count > 1}
                                        <span
                                            class="bg-muted text-muted-foreground rounded-full px-1.5 py-0.5 text-[10px] font-medium tabular-nums">
                                            ×{notification.count}
                                        </span>
                                    {/if}
                                    {#if !notification.read}
                                        <div class="size-2 rounded-full bg-blue-500"></div>
                                    {/if}
                                </div>
                                <p class="text-sm leading-none font-medium">
                                    {notification.title}
                                </p>
                                <p class="text-muted-foreground text-xs">
                                    {notification.message}
                                </p>
                                <p class="text-muted-foreground/70 text-xs">
                                    {formatTimestamp(notification.timestamp)}
                                </p>
                            </div>
                            <div class="flex flex-col gap-1">
                                {#if !notification.read}
                                    <Tooltip>
                                        {#snippet trigger()}
                                            <Button
                                                variant="ghost"
                                                size="icon"
                                                class="text-muted-foreground hover:text-primary size-6"
                                                onclick={(event) => {
                                                    event.stopPropagation();
                                                    handleMarkAsRead(notification.id);
                                                }}>
                                                <Check class="size-3" />
                                            </Button>
                                        {/snippet}
                                        {#snippet content()}
                                            <p>Mark as read</p>
                                        {/snippet}
                                    </Tooltip>
                                {/if}
                                <Tooltip>
                                    {#snippet trigger()}
                                        <Button
                                            variant="ghost"
                                            size="icon"
                                            class="text-muted-foreground hover:text-destructive size-6"
                                            onclick={(event) => {
                                                event.stopPropagation();
                                                handleRemove(notification.id);
                                            }}>
                                            <Trash2 class="size-3" />
                                        </Button>
                                    {/snippet}
                                    {#snippet content()}
                                        <p>Remove</p>
                                    {/snippet}
                                </Tooltip>
                            </div>
                        </div>
                    {/snippet}

                    {#each notificationStore.notifications as notification (notification.id)}
                        {@const href = detailsHref(notification)}
                        {#if href}
                            <!-- biome-ignore lint/a11y/useSemanticElements: notificationBody renders its own <button>s (mark-as-read, remove) for the row's per-item actions — nesting a <button> inside a <button> is invalid HTML, so this can't be a real button and still contain those. -->
                            <div
                                class="border-border/50 hover:bg-muted/30 border-b p-3 transition-colors {!notification.read
                                    ? 'bg-muted/10'
                                    : ''} cursor-pointer"
                                role="button"
                                tabindex={0}
                                onclick={() => handleNotificationClick(notification)}
                                onkeydown={(event) => {
                                    if (event.key === "Enter" || event.key === " ") {
                                        event.preventDefault();
                                        handleNotificationClick(notification);
                                    }
                                }}>
                                {@render notificationBody(notification)}
                            </div>
                        {:else}
                            <div
                                class="border-border/50 hover:bg-muted/30 border-b p-3 transition-colors {!notification.read
                                    ? 'bg-muted/10'
                                    : ''}">
                                {@render notificationBody(notification)}
                            </div>
                        {/if}
                    {/each}
                {/if}
            </div>

            {#if notificationStore.connectionStatus === "error"}
                <div class="border-destructive/20 bg-destructive/10 border-t p-3">
                    <p class="text-destructive text-xs">
                        Connection error. Notifications may be delayed.
                    </p>
                    <Button
                        variant="outline"
                        size="sm"
                        class="mt-2 h-6 text-xs"
                        onclick={() => notificationStore.reconnect()}>
                        Reconnect
                    </Button>
                </div>
            {/if}
        </div>
    </Popover.Content>
</Popover.Root>

<style>
    /* app.css hides scrollbars globally (`::-webkit-scrollbar { display:
       none }` with no scoping selector) — fine for drag/wheel-scrolled
       carousels elsewhere, but this list needs a visible indicator that
       there's more to scroll to. Overridden here rather than in app.css so
       the global behavior elsewhere is untouched. */
    .notification-scroll {
        scrollbar-width: thin;
    }

    .notification-scroll::-webkit-scrollbar {
        display: block;
        width: 6px;
    }

    .notification-scroll::-webkit-scrollbar-track {
        background: transparent;
    }

    .notification-scroll::-webkit-scrollbar-thumb {
        background-color: rgb(255 255 255 / 15%);
        border-radius: 3px;
    }

    .notification-scroll::-webkit-scrollbar-thumb:hover {
        background-color: rgb(255 255 255 / 25%);
    }
</style>
