import { gqlClient } from "$lib/graphql-client";
import type { TMDBTransformedListItem } from "$lib/metadata/parser";

export interface GqlTmdbListItem {
	id: number;
	title: string;
	posterPath: string | null;
	mediaType: "movie" | "tv" | "person" | "company";
	year: string;
	voteAverage: number | null;
	voteCount: number | null;
	popularity?: number | null;
	overview?: string | null;
	backdropPath?: string | null;
	genreIds?: number[] | null;
	releaseDate?: string | null;
	firstAirDate?: string | null;
	originalTitle?: string | null;
	originalLanguage?: string | null;
	indexer: "tmdb" | "tvdb";
}

const TMDB_LIST_FIELDS = `
    id
    title
    posterPath
    mediaType
    year
    voteAverage
    voteCount
    popularity
    overview
    backdropPath
    genreIds
    releaseDate
    firstAirDate
    originalTitle
    originalLanguage
    indexer
`;

const TMDB_DETAILS_QUERY = `query($type: String!, $id: Int!, $appendToResponse: String) {
    tmdbDetails(type: $type, id: $id, appendToResponse: $appendToResponse)
}`;

const RESOLVE_EXTERNAL_ID_QUERY = `query($from: String!, $to: String!, $id: String!, $mediaType: String) {
    resolveExternalId(from: $from, to: $to, id: $id, mediaType: $mediaType) {
        id
        resolved
    }
}`;

const TMDB_TRENDING_QUERY = `query($type: String!, $timeWindow: String!, $page: Int) {
    trendingTmdb(type: $type, timeWindow: $timeWindow, page: $page) {
        results { ${TMDB_LIST_FIELDS} }
    }
}`;

const TMDB_CATEGORY_QUERY = `query($type: String!, $category: String!, $page: Int) {
    tmdbCategory(type: $type, category: $category, page: $page) {
        results { ${TMDB_LIST_FIELDS} }
    }
}`;

const SEARCH_TMDB_QUERY = `query($type: String!, $params: JSON, $searchMode: String) {
    searchTmdb(type: $type, params: $params, searchMode: $searchMode) {
        results { ${TMDB_LIST_FIELDS} }
    }
}`;

/**
 * Paginated TMDB search query for client-side use (search modal + search store).
 * Mirrors `SEARCH_TMDB_QUERY` but also selects pagination metadata.
 */
export const SEARCH_TMDB_PAGE_QUERY = `query SearchTmdb($type: String!, $params: JSON, $searchMode: String) {
    searchTmdb(type: $type, params: $params, searchMode: $searchMode) {
        results { ${TMDB_LIST_FIELDS} }
        page totalPages totalResults
    }
}`;

const TVDB_SERIES_EXTENDED_QUERY = `query($id: Int!, $meta: String) {
    tvdbSeriesExtended(id: $id, meta: $meta)
}`;

const TVDB_PERSON_EXTENDED_QUERY = `query($id: Int!, $meta: String) {
    tvdbPersonExtended(id: $id, meta: $meta)
}`;

const TVDB_EPISODES_QUERY = `query($id: Int!, $seasonType: String!, $lang: String!, $page: Int) {
    tvdbEpisodes(id: $id, seasonType: $seasonType, lang: $lang, page: $page)
}`;

function mapGqlTmdbListItem(item: GqlTmdbListItem): TMDBTransformedListItem {
	return {
		id: item.id,
		title: item.title,
		poster_path: item.posterPath,
		media_type: item.mediaType,
		year: item.year,
		vote_average: item.voteAverage,
		vote_count: item.voteCount,
		popularity: item.popularity ?? undefined,
		overview: item.overview ?? undefined,
		backdrop_path: item.backdropPath ?? undefined,
		genre_ids: item.genreIds ?? undefined,
		release_date: item.releaseDate ?? undefined,
		first_air_date: item.firstAirDate ?? undefined,
		original_title: item.originalTitle ?? undefined,
		original_language: item.originalLanguage ?? undefined,
		indexer: item.indexer,
	};
}

export function mapGqlTmdbList(
	items: GqlTmdbListItem[],
): TMDBTransformedListItem[] {
	return items.map(mapGqlTmdbListItem);
}

export async function fetchTmdbDetails<T>(options: {
	type: "movie" | "tv" | "person" | "company";
	id: number;
	appendToResponse?: string;
}) {
	const data = await gqlClient<{ tmdbDetails: T }>(TMDB_DETAILS_QUERY, options);
	return data.tmdbDetails;
}

export async function resolveExternalId(options: {
	from: "tmdb" | "tvdb" | "imdb" | "anilist" | "riven";
	to: "tmdb" | "tvdb" | "imdb" | "anilist" | "riven";
	id: string;
	mediaType?: "movie" | "tv";
}) {
	const data = await gqlClient<{
		resolveExternalId: { id: string; resolved: boolean };
	}>(RESOLVE_EXTERNAL_ID_QUERY, options);
	return data.resolveExternalId;
}

export async function fetchTmdbTrending(options: {
	type: "movie" | "tv" | "all";
	timeWindow: "day" | "week";
	page?: number;
}) {
	const data = await gqlClient<{
		trendingTmdb: { results: GqlTmdbListItem[] };
	}>(TMDB_TRENDING_QUERY, options);
	return data.trendingTmdb.results;
}

export async function fetchTmdbCategory(options: {
	type: "movie" | "tv";
	category: "popular" | "top_rated";
	page?: number;
}) {
	const data = await gqlClient<{
		tmdbCategory: { results: GqlTmdbListItem[] };
	}>(TMDB_CATEGORY_QUERY, options);
	return data.tmdbCategory.results;
}

export async function searchTmdb(options: {
	type: "movie" | "tv" | "person" | "company";
	params?: Record<string, unknown>;
	searchMode?: "search" | "discover" | "hybrid";
}) {
	const data = await gqlClient<{ searchTmdb: { results: GqlTmdbListItem[] } }>(
		SEARCH_TMDB_QUERY,
		options,
	);
	return data.searchTmdb.results;
}

export async function fetchTvdbSeriesExtended<T>(id: number, meta?: string) {
	const data = await gqlClient<{ tvdbSeriesExtended: T }>(
		TVDB_SERIES_EXTENDED_QUERY,
		{
			id,
			meta,
		},
	);
	return data.tvdbSeriesExtended;
}

export async function fetchTvdbPersonExtended<T>(id: number, meta?: string) {
	const data = await gqlClient<{ tvdbPersonExtended: T }>(
		TVDB_PERSON_EXTENDED_QUERY,
		{
			id,
			meta,
		},
	);
	return data.tvdbPersonExtended;
}

export async function fetchTvdbEpisodes<T>(options: {
	id: number;
	seasonType: string;
	lang: string;
	page?: number;
}) {
	const data = await gqlClient<{ tvdbEpisodes: T }>(
		TVDB_EPISODES_QUERY,
		options,
	);
	return data.tvdbEpisodes;
}
