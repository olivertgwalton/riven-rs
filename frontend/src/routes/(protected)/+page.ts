import type { PageLoad } from "./$types";
import { createScopedLogger } from "$lib/logger";
import { gqlClient } from "$lib/graphql-client";
import {
	fetchTmdbTrending,
	mapGqlTmdbList,
} from "$lib/services/backend-metadata";
import {
	getRecentItemsVariables,
	mapRecentItemsPage,
	RECENT_ITEMS_QUERY,
	type RecentListItem,
	type RecentItemsResponse,
} from "$lib/services/recent-items";

const logger = createScopedLogger("home");

export const load: PageLoad = async () => {
	// Auth gating lives in the protected layout.

	try {
		const trendingResults = await fetchTmdbTrending({
			type: "all",
			timeWindow: "day",
			page: 1,
		});

		let recentlyAdded: RecentListItem[] = [];
		try {
			const recentData = await gqlClient<RecentItemsResponse>(
				RECENT_ITEMS_QUERY,
				getRecentItemsVariables(),
			);
			recentlyAdded = mapRecentItemsPage(recentData).items;
		} catch (err) {
			logger.error("Error fetching recently added data:", err);
		}

		// Filter to only movies and TV shows with backdrops
		const tmdbResults = mapGqlTmdbList(trendingResults);
		const filtered = tmdbResults.filter(
			(item) =>
				(item.media_type === "movie" || item.media_type === "tv") &&
				item.backdrop_path,
		);

		return { nowPlaying: filtered, recentlyAdded };
	} catch (err) {
		logger.error("Error fetching now playing data:", err);
		return { nowPlaying: [], recentlyAdded: [] };
	}
};
