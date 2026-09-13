import { browser } from "$app/environment";
import { createScopedLogger } from "$lib/logger";
import { deduplicateById } from "$lib/utils";

const logger = createScopedLogger("lists-cache");

interface CachedData<T> {
	items: T[];
	timestamp: number;
}

type TimeWindow = "day" | "week";
type MediaListLoader<T> = (
	page: number,
	timeWindow: TimeWindow | null,
) => Promise<T[]>;

interface MediaListStoreOptions<T> {
	key: string;
	initialTimeWindow?: TimeWindow;
	noCache?: boolean;
	initialData?: T[];
	loader: MediaListLoader<T>;
}

/**
 * Base interface for list items from various APIs.
 * All items must have at least an `id` for tracking.
 */
export interface BaseListItem {
	id: number;
	title?: string;
	name?: string;
	posterPath?: string | null;
	mediaType?: string;
	[key: string]: unknown;
}

const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes cache validity

export class MediaListStore<T = unknown> {
	readonly #key: string;
	readonly #supportsTimeWindow: boolean;
	readonly #defaultTimeWindow: TimeWindow;
	readonly #noCache: boolean;
	readonly #loader: MediaListLoader<T>;

	// Time window preference, persisted in sessionStorage.
	#timeWindow = $state<TimeWindow>("day");

	// Runtime state (non-persisted)
	#items = $state<T[]>([]);
	#loading = $state(false);
	#error = $state<string | null>(null);
	#page = $state(1);
	#hasMore = $state(true);
	#initialized = $state(false);

	constructor(options: MediaListStoreOptions<T>) {
		this.#key = options.key;
		this.#supportsTimeWindow = options.initialTimeWindow !== undefined;
		this.#defaultTimeWindow = options.initialTimeWindow ?? "day";
		this.#noCache = options.noCache ?? false;
		this.#loader = options.loader;

		this.#timeWindow = this.#defaultTimeWindow;
		if (browser && this.#supportsTimeWindow) {
			try {
				const stored = sessionStorage.getItem(this.#preferencesKey);
				if (stored) this.#timeWindow = JSON.parse(stored).timeWindow;
			} catch {
				// ignore malformed preference
			}
		}

		// Initialize with initialData if provided, otherwise empty
		if (options.initialData?.length) {
			this.#items = options.initialData;
			this.#initialized = true;
			// Optionally cache it if we want persistence across soft reloads,
			// but usually server data is fresh enough.
			if (!this.#noCache) {
				this.#setCachedData(options.initialData);
			}
		} else if (browser) {
			// Only auto-load if no initial data
			this.load();
		}
	}

	get items(): T[] {
		return this.#items;
	}

	get timeWindow(): TimeWindow | null {
		if (!this.#supportsTimeWindow) return null;
		return this.#timeWindow;
	}

	get #preferencesKey(): string {
		return `${this.#key}_preferences`;
	}

	get loading(): boolean {
		return this.#loading;
	}

	get error(): string | null {
		return this.#error;
	}

	get hasMore(): boolean {
		return this.#hasMore;
	}

	get initialized(): boolean {
		return this.#initialized;
	}

	#getStorageKey(): string {
		const tw = this.timeWindow;
		return tw ? `${this.#key}_${tw}_cache` : `${this.#key}_cache`;
	}

	#getCachedData(): CachedData<T> | null {
		if (!browser || this.#noCache) return null;

		try {
			const stored = sessionStorage.getItem(this.#getStorageKey());
			if (!stored) return null;

			const cached = JSON.parse(stored) as CachedData<T>;
			const now = Date.now();

			// Check if cache is still valid
			if (now - cached.timestamp > CACHE_DURATION) {
				sessionStorage.removeItem(this.#getStorageKey());
				return null;
			}

			return cached;
		} catch {
			return null;
		}
	}

	#setCachedData(items: T[]): void {
		if (!browser || this.#noCache) return;

		try {
			const cached: CachedData<T> = {
				items,
				timestamp: Date.now(),
			};
			sessionStorage.setItem(this.#getStorageKey(), JSON.stringify(cached));
		} catch (e) {
			// Handle quota exceeded - clear old caches
			if (e instanceof DOMException && e.name === "QuotaExceededError") {
				this.#clearAllCaches();
			}
		}
	}

	#clearAllCaches(): void {
		if (!browser) return;

		const keysToRemove: string[] = [];
		for (let i = 0; i < sessionStorage.length; i++) {
			const key = sessionStorage.key(i);
			if (key?.includes("_cache")) {
				keysToRemove.push(key);
			}
		}
		for (const key of keysToRemove) {
			sessionStorage.removeItem(key);
		}
	}

	async changeTimeWindow(window: TimeWindow): Promise<void> {
		if (!this.#supportsTimeWindow || this.timeWindow === window) return;

		this.#timeWindow = window;
		try {
			sessionStorage.setItem(
				this.#preferencesKey,
				JSON.stringify({ timeWindow: window }),
			);
		} catch {
			// storage unavailable; preference lasts for this page only
		}

		// Reset pagination and reload
		this.#page = 1;
		this.#hasMore = true;
		this.#items = [];
		await this.load();
	}

	async load(): Promise<void> {
		if (!browser) return;

		// Try to use cached data first
		const cached = this.#getCachedData();
		if (cached && cached.items.length > 0) {
			this.#items = cached.items;
			this.#initialized = true;
			return;
		}

		await this.#fetchFromApi();
	}

	async #fetchFromApi(): Promise<void> {
		if (this.#loading) return;

		try {
			this.#loading = true;
			this.#error = null;

			const items = deduplicateById(
				(await this.#loader(this.#page, this.timeWindow)) as (T & {
					id?: unknown;
				})[],
			);

			this.#items = items;
			this.#initialized = true;
			this.#setCachedData(items);
		} catch (error) {
			logger.error(`[MediaListStore:${this.#key}] Fetch error:`, error);
			this.#error = error instanceof Error ? error.message : String(error);
		} finally {
			this.#loading = false;
		}
	}

	async loadMore(): Promise<void> {
		if (!browser || this.#loading || !this.#hasMore) return;

		try {
			this.#loading = true;
			this.#error = null;
			this.#page += 1;

			const newItems = await this.#loader(this.#page, this.timeWindow);

			if (newItems.length === 0) {
				this.#hasMore = false;
			} else {
				// Combine and deduplicate items
				const combined = [...this.#items, ...newItems] as (T & {
					id?: unknown;
				})[];
				this.#items = deduplicateById(combined) as T[];
				// Update cache with all items
				this.#setCachedData(this.#items);
			}
		} catch (error) {
			logger.error(`[MediaListStore:${this.#key}] Load more error:`, error);
			this.#error = error instanceof Error ? error.message : String(error);
			this.#page -= 1;
		} finally {
			this.#loading = false;
		}
	}

	/**
	 * Force refresh data from API, bypassing cache
	 */
	async refresh(): Promise<void> {
		this.clearCache();
		this.#page = 1;
		this.#hasMore = true;
		await this.#fetchFromApi();
	}

	clearCache(): void {
		if (browser) {
			sessionStorage.removeItem(this.#getStorageKey());
		}
	}
}
