import { gqlSubscribeClient } from "$lib/graphql-client";
import { gqlClient } from "$lib/graphql-client";
import { createScopedLogger } from "$lib/logger";

const logger = createScopedLogger("logs");

export type LogEntry = {
	timestamp?: string | null;
	level?: string | null;
	message?: string | null;
	target?: string | null;
};

export type LiveLogLine = string;

const HISTORICAL_LOGS_QUERY = `
    query GetLogs($limit: Int, $level: String) {
        logs(limit: $limit, level: $level) {
            timestamp
            level
            message
            target
        }
    }
`;

const LOG_LINES_SUBSCRIPTION = `subscription {
    logLines
}`;

class LogStore {
	logs = $state<LiveLogLine[]>([]);
	historicalLogs = $state<LogEntry[]>([]);
	isLoadingHistorical = $state(false);
	error = $state<string | null>(null);
	historicalError = $state<string | null>(null);
	connectionStatus = $state<
		"connecting" | "connected" | "disconnected" | "error"
	>("disconnected");
	reconnectAttempts = $state(0);
	readonly maxReconnectAttempts = 5;
	hasConnected = $state(false);
	#activeTab = $state<"live" | "historical">("live");
	#unsubscribe: (() => void) | null = null;

	// Read-only: switching tabs goes through setActiveTab (lazy-loads history).
	get activeTab() {
		return this.#activeTab;
	}

	async fetchHistoricalLogs(limit = 500, level?: string) {
		try {
			this.isLoadingHistorical = true;
			this.historicalError = null;

			const data = await gqlClient<{ logs: LogEntry[] }>(
				HISTORICAL_LOGS_QUERY,
				{
					limit,
					level: level ?? null,
				},
			);

			this.historicalLogs = data.logs;
		} catch (e: unknown) {
			const message = e instanceof Error ? e.message : "Unknown error";
			logger.error("Failed to fetch historical logs:", e);
			this.historicalError = `Failed to fetch logs: ${message}`;
		} finally {
			this.isLoadingHistorical = false;
		}
	}

	connect() {
		if (this.#unsubscribe) {
			return;
		}

		this.connectionStatus = "connecting";
		this.error = null;
		this.hasConnected = false;
		this.reconnectAttempts = 0;

		this.#unsubscribe = gqlSubscribeClient<{ logLines: string }>(
			LOG_LINES_SUBSCRIPTION,
			undefined,
			{
				onData: (payload) => {
					this.connectionStatus = "connected";
					this.hasConnected = true;
					this.reconnectAttempts = 0;
					this.error = null;

					const raw = payload.logLines;
					if (!raw?.trim()) return;
					this.logs.push(raw);
				},
				onError: (error) => {
					const streamEnded = error.message === "Stream ended";

					if (!streamEnded) {
						logger.error("Log subscription error:", error);
						this.reconnectAttempts += 1;
					}

					if (
						!streamEnded &&
						this.reconnectAttempts >= this.maxReconnectAttempts
					) {
						this.connectionStatus = "error";
						this.error = "Log stream disconnected";
						return;
					}

					this.connectionStatus = "connecting";
					this.disconnect();
					setTimeout(() => this.connect(), streamEnded ? 500 : 1000);
				},
			},
		);
	}

	disconnect() {
		this.connectionStatus = "disconnected";
		this.hasConnected = false;
		if (this.#unsubscribe) {
			this.#unsubscribe();
			this.#unsubscribe = null;
		}
	}

	reconnect() {
		this.disconnect();
		this.connect();
	}

	setActiveTab(tab: "live" | "historical") {
		this.#activeTab = tab;
		if (tab === "historical" && this.historicalLogs.length === 0) {
			this.fetchHistoricalLogs();
		}
	}
}

export const logStore = new LogStore();
