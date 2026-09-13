import { browser } from "$app/environment";
import type { ParsedSearchQuery } from "$lib/search-parser";

import { createScopedLogger } from "$lib/logger";
import { gqlClient } from "$lib/graphql-client";
import type { TmdbListItem } from "$lib/gql/schema";
import { SEARCH_TMDB_PAGE_QUERY } from "$lib/services/backend-metadata";

const logger = createScopedLogger("search");

// Redefine SearchResult here to avoid importing server code
export interface SearchResult {
	results: TmdbListItem[];
	page: number;
	total_pages: number;
	total_results: number;
}

interface GqlTmdbPage {
	results: TmdbListItem[];
	page: number;
	totalPages: number;
	totalResults: number;
}

function mapTmdbPage(gql: GqlTmdbPage): SearchResult {
	return {
		results: gql.results,
		page: gql.page,
		total_pages: gql.totalPages,
		total_results: gql.totalResults,
	};
}

type Kind = "movie" | "tv" | "person" | "company";
const KINDS: Kind[] = ["movie", "tv", "person", "company"];

interface KindState {
	results: TmdbListItem[];
	page: number;
	total: number;
	hasMore: boolean;
}

function emptyKinds(): Record<Kind, KindState> {
	const empty = (): KindState => ({
		results: [],
		page: 1,
		total: 0,
		hasMore: true,
	});
	return { movie: empty(), tv: empty(), person: empty(), company: empty() };
}

// Filter parameters that can be applied to TMDB searches
export interface FilterParams {
	// Date filters
	"primary_release_date.gte"?: string;
	"primary_release_date.lte"?: string;
	"first_air_date.gte"?: string;
	"first_air_date.lte"?: string;
	// Genre filter
	with_genres?: string;
	// Language filter
	with_original_language?: string;
	// Certification filters (movies only)
	certification_country?: string;
	certification?: string;
	// Runtime filters
	"with_runtime.gte"?: number;
	"with_runtime.lte"?: number;
	// Vote average filters
	"vote_average.gte"?: number;
	"vote_average.lte"?: number;
	// Vote count filters
	"vote_count.gte"?: number;
	"vote_count.lte"?: number;
	// Sort
	sort_by?: string;
}

export class SearchStore {
	searchQuery = $state<string>("");
	rawSearchString = $state<string>("");
	parsedSearch = $state<ParsedSearchQuery | null>(null);
	kinds = $state(emptyKinds());
	loading = $state<boolean>(false);
	error = $state<string | null>(null);
	mediaType = $state<Kind | "both">("both");
	warnings = $state<string[]>([]);

	// Additional filter parameters from the filter panel
	filterParams = $state<FilterParams>({});

	// If true, allows searching/fetching even without a query or filters
	// Useful for discovery pages that verify "popular" by default
	allowEmptySearch = $state<boolean>(false);

	// For request cancellation
	abortController: AbortController | null = null;

	/** The kinds covered by a media type ("both" means all of them). */
	#selected(type: Kind | "both" = this.mediaType): Kind[] {
		return type === "both" ? KINDS : [type];
	}

	// Results are exactly what the API returns.
	get results() {
		const results = this.#selected().flatMap((k) => this.kinds[k].results);
		if (this.mediaType === "both") {
			// Sort merged results by popularity
			results.sort((a, b) => (b.popularity ?? 0) - (a.popularity ?? 0));
		}
		return results;
	}

	get unfilteredResultsCount() {
		return this.#selected().reduce(
			(n, k) => n + this.kinds[k].results.length,
			0,
		);
	}

	get totalResults() {
		return this.#selected().reduce((n, k) => n + this.kinds[k].total, 0);
	}

	get hasMore() {
		return this.#selected().some((k) => this.kinds[k].hasMore);
	}

	async setMediaType(type: Kind | "both") {
		if (this.mediaType === type) return;
		this.mediaType = type;

		if (!this.parsedSearch) return;

		// If we switched to a type and have NO results for it, fetch.
		const missing = this.#selected(type).filter(
			(k) => this.kinds[k].results.length === 0,
		);
		if (missing.length > 0) {
			await this.fetchMissingMedia(missing);
		}
	}

	private async fetchMissingMedia(missing: Kind[]) {
		this.cancelPendingRequests();
		this.abortController = new AbortController();
		const signal = this.abortController.signal;

		try {
			this.loading = true;
			this.error = null;

			await Promise.all(missing.map((k) => this.fetchMedia(k, 1, signal)));
		} catch (error) {
			if (error instanceof Error && error.name === "AbortError") return;
			logger.error("Error fetching missing media:", error);
			this.error = error instanceof Error ? error.message : String(error);
		} finally {
			if (!signal.aborted) {
				this.loading = false;
			}
		}
	}

	/**
	 * Syncs the store with a new parsed search query.
	 * Handles diffing and triggering search/clear automatically.
	 */
	syncQuery(parsed: ParsedSearchQuery | null) {
		const newQuery = parsed?.query || "";

		if (!parsed || !newQuery) {
			this.clear();
			return;
		}

		// Avoid re-searching if the query hasn't changed
		if (this.searchQuery === newQuery) {
			return;
		}

		this.setSearch(newQuery, parsed);
		this.search();
	}

	setSearch(rawString: string, parsed: ParsedSearchQuery) {
		this.rawSearchString = rawString;
		this.searchQuery = parsed.query;
		this.parsedSearch = parsed;
		this.warnings = parsed.warnings;

		// Reset state for new search
		this.kinds = emptyKinds();
	}

	/**
	 * Cancel any pending search requests
	 */
	cancelPendingRequests(): void {
		if (this.abortController) {
			this.abortController.abort();
			this.abortController = null;
		}
		this.loading = false;
	}

	/**
	 * Immediate search
	 */
	async search(): Promise<void> {
		if (!browser) return;

		this.cancelPendingRequests();

		this.abortController = new AbortController();
		const signal = this.abortController.signal;

		try {
			this.loading = true;
			this.error = null;

			const selected = this.#selected();
			for (const k of selected) {
				Object.assign(this.kinds[k], { results: [], total: 0, page: 1 });
			}
			await Promise.all(selected.map((k) => this.fetchMedia(k, 1, signal)));
		} catch (error) {
			if (error instanceof Error && error.name === "AbortError") {
				return;
			}
			logger.error("Error searching:", error);
			this.error = error instanceof Error ? error.message : String(error);
		} finally {
			if (!signal.aborted) {
				this.loading = false;
			}
		}
	}

	private deduplicateItems(
		newItems: TmdbListItem[],
		existingItems: TmdbListItem[] = [],
	): TmdbListItem[] {
		const seenIds = new Set(existingItems.map((i) => i.id));
		const uniqueItems: TmdbListItem[] = [];

		for (const item of newItems) {
			if (
				item &&
				item.id !== undefined &&
				item.id !== null &&
				!seenIds.has(item.id)
			) {
				uniqueItems.push(item);
				seenIds.add(item.id);
			}
		}
		return uniqueItems;
	}

	/**
	 * Set filter parameters and optionally trigger a search
	 */
	setFilters(params: FilterParams, triggerSearch = true): void {
		this.filterParams = params;
		logger.debug("setFilters called", {
			params,
			triggerSearch,
			hasParsedSearch: !!this.parsedSearch,
		});
		if (
			triggerSearch &&
			(this.parsedSearch || Object.keys(params).length > 0)
		) {
			// Reset results when applying new filters
			this.kinds = emptyKinds();
			this.search();
		}
	}

	/**
	 * Clear filter parameters
	 */
	clearFilters(): void {
		this.filterParams = {};
	}

	private buildSearchParams(page: number) {
		const hasFilters = Object.keys(this.filterParams).length > 0;
		const searchMode = hasFilters
			? "discover"
			: this.parsedSearch?.searchMode || "discover";

		const params: Record<string, unknown> = {
			...(this.parsedSearch?.tmdbParams || {}),
			...this.filterParams,
			page,
		};

		if (searchMode === "discover") {
			delete params.query;
		}

		// Strip undefined/null/empty values
		const cleaned: Record<string, string> = {};
		for (const [key, value] of Object.entries(params)) {
			if (value !== undefined && value !== null && value !== "") {
				cleaned[key] = String(value);
			}
		}

		return { searchMode, params: cleaned };
	}

	private async fetchSearchResults(
		type: Kind,
		page: number,
		signal?: AbortSignal,
	): Promise<SearchResult> {
		const { searchMode, params } = this.buildSearchParams(page);
		const data = await gqlClient<{ searchTmdb: GqlTmdbPage }>(
			SEARCH_TMDB_PAGE_QUERY,
			{ type, params, searchMode },
			signal,
		);
		return mapTmdbPage(data.searchTmdb);
	}

	// Fetching needs a parsed search, filter params, or empty search allowed.
	get #canFetch(): boolean {
		return (
			!!this.parsedSearch ||
			Object.keys(this.filterParams).length > 0 ||
			this.allowEmptySearch
		);
	}

	private async fetchMedia(
		type: Kind,
		page: number,
		signal?: AbortSignal,
	): Promise<void> {
		if (!this.#canFetch) return;

		const result = await this.fetchSearchResults(type, page, signal);

		if (signal?.aborted) return;

		const items = (result.results || []) as TmdbListItem[];
		const kind = this.kinds[type];

		if (page === 1) {
			kind.results = this.deduplicateItems(items);
			if (this.mediaType === type || this.mediaType === "both") {
				kind.total = result.total_results || 0;
			}
		} else {
			kind.results = [
				...kind.results,
				...this.deduplicateItems(items, kind.results),
			];
		}

		kind.hasMore = result.page < result.total_pages;
	}

	private async loadMoreMedia(type: Kind, signal?: AbortSignal): Promise<void> {
		const kind = this.kinds[type];
		if (!this.#canFetch || !kind.hasMore) return;

		kind.page += 1;

		try {
			const result = await this.fetchSearchResults(type, kind.page, signal);

			if (signal?.aborted) return;

			const newItems = (result.results || []) as TmdbListItem[];
			if (newItems.length > 0) {
				kind.results = [
					...kind.results,
					...this.deduplicateItems(newItems, kind.results),
				];
			}

			kind.hasMore = result.page < result.total_pages;
		} catch (err) {
			kind.page -= 1;
			throw err;
		}
	}

	async loadMore(): Promise<void> {
		if (!browser || this.loading || !this.hasMore || !this.#canFetch) return;

		// Cancel any pending requests
		this.cancelPendingRequests();
		this.abortController = new AbortController();
		const signal = this.abortController.signal;

		try {
			this.loading = true;
			this.error = null;

			await Promise.all(
				this.#selected()
					.filter((k) => this.kinds[k].hasMore)
					.map((k) => this.loadMoreMedia(k, signal)),
			);
		} catch (error) {
			if (error instanceof Error && error.name === "AbortError") return;
			logger.error("Error loading more results:", error);
			this.error = error instanceof Error ? error.message : String(error);
		} finally {
			if (!signal.aborted) {
				this.loading = false;
			}
		}
	}

	clear() {
		this.cancelPendingRequests();

		this.searchQuery = "";
		this.rawSearchString = "";
		this.parsedSearch = null;
		this.kinds = emptyKinds();
		this.error = null;
		this.warnings = [];
		this.loading = false;
		this.filterParams = {};
	}

	/**
	 * Check if filters or search is active
	 */
	get hasActiveSearch(): boolean {
		return !!this.parsedSearch || Object.keys(this.filterParams).length > 0;
	}
}
