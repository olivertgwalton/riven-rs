import type { PageLoad } from "./$types";
import { gqlClient } from "$lib/graphql-client";
import { createScopedLogger } from "$lib/logger";
import {
	ACTIVE_PLAYBACK_QUERY,
	DEBRID_ACCOUNT_INFO_QUERY,
	EMPTY_TITLE_SUMMARY,
	INDEXER_STATS_QUERY,
	STATS_QUERY,
	USENET_HEALTH_QUERY,
	type ActivePlaybackResult,
	type DashboardStats,
	type DebridAccountInfoResult,
	type IndexerStatsResult,
	type UsenetHealthResult,
} from "./queries";

const logger = createScopedLogger("dashboard");
const DASHBOARD_STATS_DEPENDENCY = "riven:dashboard-stats";

export const load = (async ({ depends }) => {
	depends(DASHBOARD_STATS_DEPENDENCY);

	// All queries start in parallel. Returning Promises (not awaited values)
	// makes SvelteKit stream the data — navigation is instant and content fills in.
	const statistics = gqlClient<DashboardStats>(STATS_QUERY, {}).catch(
		(err): DashboardStats | null => {
			logger.error("Failed to fetch stats:", err);
			return null;
		},
	);

	const activePlaybackSessions = gqlClient<ActivePlaybackResult>(
		ACTIVE_PLAYBACK_QUERY,
		{},
	)
		.then((data) => data.activePlaybackSessions ?? [])
		.catch(() => []);

	const downloaderServices = gqlClient<DebridAccountInfoResult>(
		DEBRID_ACCOUNT_INFO_QUERY,
		{},
	)
		.then((data) => data.debridAccountInfo ?? [])
		.catch(() => []);

	const indexerStats = gqlClient<IndexerStatsResult>(INDEXER_STATS_QUERY, {})
		.then((data) => data.indexerStats ?? [])
		.catch(() => []);

	const usenetHealth = gqlClient<UsenetHealthResult>(USENET_HEALTH_QUERY, {})
		.then(
			(data): UsenetHealthResult => ({
				nntpProviders: data.nntpProviders ?? [],
				usenetStreamingHealth: data.usenetStreamingHealth ?? null,
				usenetTitleHealth: data.usenetTitleHealth ?? [],
				usenetTitleHealthSummary:
					data.usenetTitleHealthSummary ?? EMPTY_TITLE_SUMMARY,
				usenetTraffic: data.usenetTraffic ?? null,
			}),
		)
		.catch(
			(): UsenetHealthResult => ({
				nntpProviders: [],
				usenetStreamingHealth: null,
				usenetTitleHealth: [],
				usenetTitleHealthSummary: EMPTY_TITLE_SUMMARY,
				usenetTraffic: null,
			}),
		);

	return {
		statistics,
		activePlaybackSessions,
		downloaderServices,
		indexerStats,
		usenetHealth,
	};
}) satisfies PageLoad;
