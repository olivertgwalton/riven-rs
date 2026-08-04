import { gqlClient } from "$lib/graphql-client";
import type { TmdbListItem } from "$lib/gql/schema";

export type { TmdbListItem };

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
		trendingTmdb: { results: TmdbListItem[] };
	}>(TMDB_TRENDING_QUERY, options);
	return data.trendingTmdb.results;
}

export async function fetchTmdbCategory(options: {
	type: "movie" | "tv";
	category: "popular" | "top_rated";
	page?: number;
}) {
	const data = await gqlClient<{
		tmdbCategory: { results: TmdbListItem[] };
	}>(TMDB_CATEGORY_QUERY, options);
	return data.tmdbCategory.results;
}

export async function searchTmdb(options: {
	type: "movie" | "tv" | "person" | "company";
	params?: Record<string, unknown>;
	searchMode?: "search" | "discover" | "hybrid";
}) {
	const data = await gqlClient<{ searchTmdb: { results: TmdbListItem[] } }>(
		SEARCH_TMDB_QUERY,
		options,
	);
	return data.searchTmdb.results;
}
