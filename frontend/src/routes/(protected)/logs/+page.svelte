<script lang="ts">
    import { onMount, onDestroy } from "svelte";
    import { Button } from "$lib/components/ui/button/index.js";
    import { toast } from "svelte-sonner";
    import { logStore, type LogEntry, type LiveLogLine } from "$lib/stores/logs.svelte";
    import { createScopedLogger } from "$lib/logger";
    import PageShell from "$lib/components/page-shell.svelte";

    const logger = createScopedLogger("logs-page");

    onMount(() => {
        logStore.connect();
    });

    onDestroy(() => {
        logStore.disconnect();
    });

    function getStatusColor() {
        switch (logStore.connectionStatus) {
            case "connected":
                return "bg-green-500";
            case "connecting":
                return "bg-yellow-500";
            case "disconnected":
                return "bg-gray-500";
            case "error":
                return "bg-red-500";
            default:
                return "bg-gray-500";
        }
    }

    function getStatusText() {
        switch (logStore.connectionStatus) {
            case "connected":
                return "Connected";
            case "connecting":
                return logStore.reconnectAttempts > 0
                    ? `Reconnecting... (${logStore.reconnectAttempts}/${logStore.maxReconnectAttempts})`
                    : "Connecting...";
            case "disconnected":
                return "Disconnected";
            case "error":
                return "Connection Error";
            default:
                return "Unknown";
        }
    }

    async function handleUploadLogs() {
        try {
            toast.info("Log upload is not supported in the new backend.");
        } catch (e) {
            logger.error("Failed to upload logs:", e);
        }
    }
</script>

<svelte:head>
    <title>Logs - Riven</title>
</svelte:head>

{#snippet liveLine(line: LiveLogLine)}
    <div class="border-border/50 hover:bg-muted/20 border-b transition-colors last:border-b-0">
        <div class="text-foreground/90 p-2 font-mono text-xs wrap-break-word whitespace-pre-wrap">
            {line}
        </div>
    </div>
{/snippet}

{#snippet logEntry(log: LogEntry)}
    {@const levelColors = {
        error: "text-red-400",
        warn: "text-yellow-400",
        info: "text-green-400",
        debug: "text-blue-400",
        trace: "text-muted-foreground"
    }}
    {@const level = (log.level ?? "info").toLowerCase()}
    <div class="border-border/50 hover:bg-muted/20 border-b transition-colors last:border-b-0">
        <div
            class="text-foreground/90 grid grid-cols-[auto_auto_auto_1fr] gap-x-3 p-2 font-mono text-xs">
            <span class="text-muted-foreground shrink-0">{log.timestamp ?? ""}</span>
            <span class="shrink-0 font-semibold uppercase {levelColors[level as keyof typeof levelColors] ?? 'text-foreground'}"
                >{level}</span>
            <span class="text-muted-foreground/70 shrink-0">{log.target ?? ""}</span>
            <span class="wrap-break-word whitespace-pre-wrap">{log.message ?? ""}</span>
        </div>
    </div>
{/snippet}

{#snippet loadingSpinner(message: string)}
    <div class="flex h-full flex-col items-center justify-center p-8">
        <div
            class="border-primary mx-auto mb-4 h-8 w-8 animate-spin rounded-full border-2 border-t-transparent">
        </div>
        <p class="text-muted-foreground text-sm">{message}</p>
    </div>
{/snippet}

{#snippet errorDisplay(
    errorMessage: string,
    retryAction: () => void,
    buttonText: string = "Try Again"
)}
    <div class="bg-destructive/10 border-destructive/20 rounded-lg border p-6">
        <h3 class="text-destructive mb-3 text-lg font-semibold">Error Loading Logs</h3>
        <pre
            class="text-destructive/80 bg-destructive/5 mb-4 overflow-x-auto rounded border p-3 font-mono text-sm">{errorMessage}</pre>
        <button type="button"
            class="bg-primary hover:bg-primary/90 text-primary-foreground rounded-lg px-4 py-2 font-medium transition-colors"
            onclick={retryAction}>
            {buttonText}
        </button>
    </div>
{/snippet}

{#snippet tabButton(name: string, isActive: boolean, onClickAction: () => void)}
    <button type="button"
        class="rounded px-3 py-1.5 text-sm font-medium transition-colors {isActive
            ? 'bg-primary/10 text-primary'
            : 'hover:bg-muted/50'}"
        onclick={onClickAction}>
        {name}
    </button>
{/snippet}

{#snippet statusIndicator()}
    <div class="flex items-center gap-2">
        <div
            class="{getStatusColor()} h-2 w-2 rounded-full {logStore.connectionStatus === 'connecting'
                ? 'animate-pulse'
                : ''}">
        </div>
        <span class="text-muted-foreground text-sm">{getStatusText()}</span>
    </div>
{/snippet}

{#snippet emptyState(message: string, actionText?: string, actionFn?: () => void)}
    <div class="flex h-full flex-col items-center justify-center p-8">
        <p class="text-muted-foreground text-sm">{message}</p>
        {#if actionText && actionFn}
            <button type="button"
                class="bg-primary/10 hover:bg-primary/20 text-primary mt-4 rounded-lg px-4 py-2 text-sm font-medium transition-colors"
                onclick={actionFn}>
                {actionText}
            </button>
        {/if}
    </div>
{/snippet}

<PageShell class="h-full">
    {#if logStore.error && logStore.connectionStatus === "error" && logStore.reconnectAttempts >= logStore.maxReconnectAttempts}
        <div class="bg-destructive/10 border-destructive/20 rounded-lg border p-6">
            <h3 class="text-destructive mb-3 text-lg font-semibold">Connection Failed</h3>
            <pre
                class="text-destructive/80 bg-destructive/5 mb-4 overflow-x-auto rounded border p-3 font-mono text-sm">{logStore.error}</pre>
            <button type="button"
                class="bg-primary hover:bg-primary/90 text-primary-foreground rounded-lg px-4 py-2 font-medium transition-colors"
                onclick={() => logStore.reconnect()}>
                Try Again
            </button>
        </div>
    {:else if logStore.logs.length > 0 || logStore.historicalLogs.length > 0 || logStore.connectionStatus !== "disconnected" || logStore.isLoadingHistorical}
        <div class="flex h-full min-h-0 flex-col">
            <div class="mb-6 flex flex-col items-start justify-between gap-4 md:flex-row">
                <div>
                    <h1 class="text-3xl font-bold tracking-tight">System Logs</h1>
                    <p class="text-muted-foreground mt-1">System monitoring and logs</p>
                </div>
                <div class="flex items-center gap-4">
                    <Button variant="secondary" onclick={handleUploadLogs}>Upload Logs</Button>
                    <div
                        class="bg-primary/10 text-primary border-primary/20 rounded-lg border px-4 py-2 font-medium">
                        {logStore.activeTab === "live" ? logStore.logs.length : logStore.historicalLogs.length} entries
                    </div>
                </div>
            </div>

            <div class="bg-card flex min-h-0 flex-1 flex-col rounded-lg border shadow-sm">
                <div
                    class="bg-muted/30 flex shrink-0 flex-col items-center justify-between gap-4 border-b px-6 py-3 md:flex-row">
                    <div class="flex items-center gap-2">
                        {@render tabButton("Live Logs", logStore.activeTab === "live", () =>
                            logStore.setActiveTab("live")
                        )}
                        {@render tabButton("Historical Logs", logStore.activeTab === "historical", () =>
                            logStore.setActiveTab("historical")
                        )}
                    </div>
                    <div class="flex items-center gap-4">
                        {#if logStore.activeTab === "live"}
                            {@render statusIndicator()}
                            {#if logStore.connectionStatus === "error" && logStore.reconnectAttempts < logStore.maxReconnectAttempts}
                                <button type="button"
                                    class="bg-primary/10 hover:bg-primary/20 text-primary border-primary/20 rounded border px-3 py-1 text-sm font-medium transition-colors"
                                    onclick={() => logStore.reconnect()}>
                                    Reconnect Now
                                </button>
                            {/if}
                        {:else}
                            <button type="button"
                                class="bg-primary/10 hover:bg-primary/20 text-primary border-primary/20 rounded border px-3 py-1 text-sm font-medium transition-colors"
                                onclick={() => logStore.fetchHistoricalLogs()}
                                disabled={logStore.isLoadingHistorical}>
                                {logStore.isLoadingHistorical ? "Loading..." : "Refresh"}
                            </button>
                        {/if}
                    </div>
                </div>

                <div class="min-h-0 flex-1 overflow-y-auto">
                    {#if logStore.activeTab === "live"}
                        {#if logStore.logs.length > 0}
                            {#each logStore.logs.slice().reverse() as line, i (i)}
                                {@render liveLine(line)}
                            {/each}
                        {:else if logStore.connectionStatus === "connecting"}
                            {@render loadingSpinner(getStatusText())}
                        {:else if logStore.connectionStatus === "connected" || logStore.hasConnected}
                            {@render emptyState("Connected. Waiting for live logs...")}
                        {:else if logStore.error}
                            <div class="p-8">
                                {@render errorDisplay(
                                    logStore.error,
                                    () => logStore.reconnect(),
                                    "Reconnect"
                                )}
                            </div>
                        {/if}
                    {:else if logStore.isLoadingHistorical}
                        {@render loadingSpinner("Loading historical logs...")}
                    {:else if logStore.historicalError}
                        <div class="p-8">
                            {@render errorDisplay(logStore.historicalError, () =>
                                logStore.fetchHistoricalLogs()
                            )}
                        </div>
                    {:else if logStore.historicalLogs.length > 0}
                        {#each logStore.historicalLogs.slice().reverse() as log, i (i)}
                            {@render logEntry(log)}
                        {/each}
                    {:else}
                        {@render emptyState("No historical logs found", "Refresh", () =>
                            logStore.fetchHistoricalLogs()
                        )}
                    {/if}
                </div>
            </div>
        </div>
    {:else}
        <div class="flex h-full flex-col items-center justify-center">
            <div class="bg-card max-w-md rounded-lg border p-8 text-center shadow-sm">
                <div
                    class="border-primary mx-auto mb-4 h-12 w-12 animate-spin rounded-full border-2 border-t-transparent">
                </div>
                <h3 class="mb-2 text-lg font-semibold">Connecting to Logs</h3>
                <p class="text-muted-foreground text-sm">
                    Establishing connection to log server...
                </p>
            </div>
        </div>
    {/if}
</PageShell>
