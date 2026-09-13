import type { PageLoad } from "./$types";
import { parseSearchQuery } from "$lib/search-parser";
import type { TmdbListItem } from "$lib/gql/schema";
import { logger } from "$lib/logger";
import {
	fetchTmdbCategory,
	fetchTmdbTrending,
} from "$lib/services/backend-metadata";

export const load: PageLoad = async ({ url }) => {
	const parsed = parseSearchQuery(url.searchParams.get("query") ?? "");

	// Fetch trending content for search examples and hero
	let heroItems: TmdbListItem[] = [];
	let feelingLuckyItems: TmdbListItem[] = [];
	let searchExamples: string[] = [];

	try {
		// Generate 4 distinct random pages to ensure variety
		const randomPagePopMovie = Math.floor(Math.random() * 50) + 1;
		const randomPagePopTV = Math.floor(Math.random() * 50) + 1;
		const randomPageTopMovie = Math.floor(Math.random() * 50) + 1;
		const randomPageTopTV = Math.floor(Math.random() * 50) + 1;

		const [
			trendingMovies,
			trendingTV,
			popularMovies,
			popularTV,
			topRatedMovies,
			topRatedTV,
		] = await Promise.all([
			fetchTmdbTrending({ type: "movie", timeWindow: "week", page: 1 }),
			fetchTmdbTrending({ type: "tv", timeWindow: "week", page: 1 }),
			fetchTmdbCategory({
				type: "movie",
				category: "popular",
				page: randomPagePopMovie,
			}),
			fetchTmdbCategory({
				type: "tv",
				category: "popular",
				page: randomPagePopTV,
			}),
			fetchTmdbCategory({
				type: "movie",
				category: "top_rated",
				page: randomPageTopMovie,
			}),
			fetchTmdbCategory({
				type: "tv",
				category: "top_rated",
				page: randomPageTopTV,
			}),
		]);

		// Hero items: Top trending
		heroItems = shuffleArray([
			...trendingMovies.slice(0, 5),
			...trendingTV.slice(0, 5),
		]);

		// Feeling Lucky: Massive pool of random high-quality content
		feelingLuckyItems = shuffleArray([
			...heroItems,
			...popularMovies,
			...popularTV,
			...topRatedMovies,
			...topRatedTV,
		]);

		// Extract titles for search examples from hero items
		searchExamples = heroItems
			.slice(0, 6)
			.map((item) => item.title?.toLowerCase() || "");
	} catch (err) {
		logger.error("Failed to fetch trending content", err);
	}

	return {
		parsed,
		searchExamples,
		heroItems,
		feelingLuckyItems,
	};
};

function shuffleArray<T>(array: T[]): T[] {
	const newArray = [...array];
	for (let i = newArray.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[newArray[i], newArray[j]] = [newArray[j], newArray[i]];
	}
	return newArray;
}
