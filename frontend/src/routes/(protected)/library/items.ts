import { createScopedLogger } from "$lib/logger";
import { getYearFromISO } from "$lib/utils/date";

// Shared by the load (initial fetch) and the page (live refresh).

const logger = createScopedLogger("library");

export interface GqlMediaItem {
	id: number;
	itemType: string;
	title: string;
	tmdbId?: string | null;
	tvdbId?: string | null;
	parentId?: number | null;
	posterPath?: string | null;
	airedAt?: string | null;
	seasonNumber?: number | null;
	episodeNumber?: number | null;
	showId?: number | null;
	showTitle?: string | null;
	showTmdbId?: string | null;
	showTvdbId?: string | null;
	showPosterPath?: string | null;
}

export interface GqlItemsPage {
	items: GqlMediaItem[];
	page: number;
	limit: number;
	totalItems: number;
	totalPages: number;
}

export const ITEMS_QUERY = `
    query GetItems(
        $page: Int
        $limit: Int
        $sort: String
        $types: [MediaItemType!]
        $search: String
        $states: [MediaItemState!]
    ) {
        items(page: $page, limit: $limit, sort: $sort, types: $types, search: $search, states: $states) {
            items {
                id
                itemType
                title
                tmdbId
                tvdbId
                parentId
                posterPath
                airedAt
                seasonNumber
                episodeNumber
                showId
                showTitle
                showTmdbId
                showTvdbId
                showPosterPath
            }
            page
            limit
            totalItems
            totalPages
        }
    }
`;

const SORTS = ["title_asc", "title_desc", "date_asc", "date_desc"];

export type LibrarySearch = {
	page: number;
	limit: number;
	sort: string;
	type: string[];
	states: string[];
	search: string;
};

/** Library filters from the URL, with defaults for missing/invalid values. */
export function parseLibrarySearch(params: URLSearchParams): LibrarySearch {
	const int = (key: string, fallback: number, max = Infinity) => {
		const n = Number(params.get(key));
		return Number.isInteger(n) && n >= 1 && n <= max ? n : fallback;
	};
	const sort = params.get("sort") ?? "";
	const type = params.getAll("type").filter(Boolean);
	const states = params.getAll("states").filter(Boolean);

	return {
		page: int("page", 1),
		limit: int("limit", 24, 100),
		sort: SORTS.includes(sort) ? sort : "date_desc",
		type: type.length ? type : ["movie", "show"],
		states: states.length ? states : ["All"],
		search: params.get("search") ?? "",
	};
}

/** GraphQL variables for ITEMS_QUERY ("All" means no state filter). */
export function itemsQueryVariables(search: LibrarySearch) {
	const states = search.states.filter((s) => s !== "All");
	return {
		page: search.page,
		limit: search.limit,
		sort: search.sort,
		types: search.type.map((t) => t.toUpperCase()),
		search: search.search || undefined,
		states: states.length > 0 ? states : undefined,
	};
}

export function transformItems(items: GqlMediaItem[]) {
	return items.map((item) => {
		const rawType = item.itemType.toLowerCase();
		let id: string | number | null = null;
		let indexer: "tmdb" | "tvdb" = "tmdb";
		let mediaPageType = rawType === "show" ? "tv" : rawType;
		let posterPath = item.posterPath;
		const detailParams = new URLSearchParams();

		if (rawType === "movie") {
			id = item.tmdbId ?? null;
		} else if (rawType === "show") {
			id = item.tvdbId ?? null;
			indexer = "tvdb";
		} else if (rawType === "season" || rawType === "episode") {
			id = item.showTvdbId ?? item.showTmdbId ?? null;
			indexer = item.showTvdbId ? "tvdb" : "tmdb";
			mediaPageType = "tv";
			posterPath = item.showPosterPath ?? item.posterPath;
			if (item.seasonNumber != null) {
				detailParams.set("season", item.seasonNumber.toString());
			}
			if (item.episodeNumber != null) {
				detailParams.set("episode", item.episodeNumber.toString());
			}
		}

		if (!id) {
			logger.warn(
				`Rendering item "${item.title}" (id: ${item.id}, type: ${item.itemType}) without a details link: missing external ID`,
			);
		}

		return {
			id,
			title: item.title,
			posterPath,
			mediaType: mediaPageType,
			year: getYearFromISO(item.airedAt) ?? "N/A",
			indexer,
			type: mediaPageType,
			detailsQuery: detailParams.toString(),
			badge:
				rawType === "season"
					? { text: "Season", variant: "default" }
					: rawType === "episode"
						? { text: "Episode", variant: "default" }
						: undefined,
			rivenId: item.id,
		};
	});
}

export type LibraryItem = ReturnType<typeof transformItems>[number];
