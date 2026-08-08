/**
 * Reactive, auto-batching lookup of "is this suggested item already in my
 * Riven library, and what's its state" — used by the Request/status button
 * on suggested-content cards (homepage rows, explore grid, trending grids).
 *
 * A single carousel row can render 20+ cards at once; querying each one's
 * status individually would be N+1 network round trips. Instead, every call
 * to `getLibraryStatus` within the same tick is coalesced into one batched
 * `mediaItemStatusesByTmdbIds`/`ByTvdbIds` query via a microtask-scheduled
 * flush, so a whole row resolves in one (or two, tmdb+tvdb) request.
 *
 * Badges then stay live off the same Riven events the details page reacts to
 * (`itemScraped`, `itemDownloaded`, `itemFailed`, `itemsDeleted`, and the
 * request events). The details page can afford `mediaItemStateUpdatesByTmdb`,
 * a per-item subscription that reloads the item and walks its parentage on
 * every event — at one item per page. A grid can't: with N cards that is N
 * server-side streams doing N database round trips per event, to render one
 * word each. So cards share a single subscription and re-run the batch query
 * for whatever is currently on screen, which is the same push, fanned out
 * here instead of per card.
 */

import { gqlClient } from "$lib/graphql-client";
import { resolveExternalId } from "$lib/services/backend-metadata";
import { subscribeToRivenMediaEvents } from "$lib/services/riven-live-updates";

export type LibraryStatusEntry =
	| { status: "loading" }
	| { status: "not_found" }
	| { status: "found"; id: number; state: string };

type Indexer = "tmdb" | "tvdb";
type MediaKind = "movie" | "tv";
export type ResolvableSource = Indexer | "anilist";

const cache = $state<Record<string, LibraryStatusEntry>>({});

function keyFor(indexer: Indexer, externalId: string): string {
	return `${indexer}:${externalId}`;
}

type MediaItemStatusRow = {
	id: number;
	state: string;
	tmdbId: string | null;
	tvdbId: string | null;
};

type ResolvedFetch = {
	entry: LibraryStatusEntry;
	// False when the batch request itself failed (network error, GraphQL
	// error): the caller still needs an answer to stop spinning, but must
	// not treat it as a confirmed result to persist.
	cacheable: boolean;
};

type PendingBatch = {
	ids: Set<string>;
	resolvers: Map<string, ((result: ResolvedFetch) => void)[]>;
};

function emptyBatch(): PendingBatch {
	return { ids: new Set(), resolvers: new Map() };
}

const batches: Record<Indexer, PendingBatch> = {
	tmdb: emptyBatch(),
	tvdb: emptyBatch()
};

let flushScheduled = false;

function scheduleFlush() {
	if (flushScheduled) return;
	flushScheduled = true;
	queueMicrotask(runFlush);
}

async function runFlush() {
	flushScheduled = false;
	await Promise.all([flushBatch("tmdb"), flushBatch("tvdb")]);
}

const BATCH_QUERIES: Record<Indexer, string> = {
	tmdb: `query($ids: [String!]!) {
        mediaItemStatusesByTmdbIds(tmdbIds: $ids) { id state tmdbId tvdbId }
    }`,
	tvdb: `query($ids: [String!]!) {
        mediaItemStatusesByTvdbIds(tvdbIds: $ids) { id state tmdbId tvdbId }
    }`
};

/**
 * Server-side ceiling on one bulk status lookup (`MAX_BULK_STATUS_IDS`).
 * Exceeding it fails the whole query, which would leave every card in the
 * batch spinning — so oversized batches are split rather than sent.
 */
const MAX_IDS_PER_QUERY = 500;

async function flushBatch(indexer: Indexer) {
	const batch = batches[indexer];
	if (batch.ids.size === 0) return;

	const ids = Array.from(batch.ids);
	const resolvers = batch.resolvers;
	batches[indexer] = emptyBatch();

	let rows: MediaItemStatusRow[] = [];
	let failed = false;
	try {
		const field = indexer === "tmdb" ? "mediaItemStatusesByTmdbIds" : "mediaItemStatusesByTvdbIds";
		const chunks: string[][] = [];
		for (let start = 0; start < ids.length; start += MAX_IDS_PER_QUERY) {
			chunks.push(ids.slice(start, start + MAX_IDS_PER_QUERY));
		}
		const query = BATCH_QUERIES[indexer];
		const fetchChunk = (chunk: string[]) =>
			gqlClient<Record<string, MediaItemStatusRow[]>>(query, { ids: chunk });
		const results = await Promise.all(chunks.map(fetchChunk));
		rows = results.flatMap((result) => result[field] ?? []);
	} catch {
		// A failed batch shouldn't leave every card spinning forever, so
		// resolvers still get an answer — but that answer must not be
		// cached as "not_found": caching a transient network failure as
		// "confirmed absent from the library" would permanently show
		// "Request" for an item that's actually already there, for the
		// rest of the session, until a full reload clears the cache.
		failed = true;
	}

	const byExternalId = new Map<string, LibraryStatusEntry>();
	for (const row of rows) {
		const extId = indexer === "tmdb" ? row.tmdbId : row.tvdbId;
		if (extId) byExternalId.set(extId, { status: "found", id: row.id, state: row.state });
	}

	for (const id of ids) {
		const entry = byExternalId.get(id) ?? { status: "not_found" };
		const key = keyFor(indexer, id);
		if (failed) {
			delete cache[key];
		} else if (!sameEntry(cache[key], entry)) {
			// Only write when something actually moved. Most items in a
			// refetch are unchanged — one episode finishing doesn't alter the
			// other fifty cards on screen — and an unconditional assignment
			// would invalidate every card's reactive read and re-render the
			// whole grid on every event.
			cache[key] = entry;
		}
		for (const resolve of resolvers.get(id) ?? []) resolve({ entry, cacheable: !failed });
	}
}

function sameEntry(
	a: LibraryStatusEntry | undefined,
	b: LibraryStatusEntry,
): boolean {
	if (!a || a.status !== b.status) return false;
	if (a.status !== "found" || b.status !== "found") return true;
	return a.id === b.id && a.state === b.state;
}

function queueFetch(indexer: Indexer, id: string): Promise<ResolvedFetch> {
	const batch = batches[indexer];
	batch.ids.add(id);
	const promise = new Promise<ResolvedFetch>((resolve) => {
		const list = batch.resolvers.get(id) ?? [];
		list.push(resolve);
		batch.resolvers.set(id, list);
	});
	scheduleFlush();
	return promise;
}

export type ResolvedId = { indexer: Indexer; id: string };

/**
 * Resolve an id from a source id-space to the id-space Riven's own library
 * actually keys that content by (movies by TMDB id, shows by TVDB id — never
 * TMDB, see the details page's own `+page.ts` resolution for the same
 * reason). Two sources need this: Anilist ids aren't TMDB/TVDB ids at all, and
 * a TMDB-sourced *TV show* result still needs translating since shows are
 * keyed by TVDB id regardless of where the search result came from. A TMDB
 * *movie* or a TVDB result is already in the right space and resolves
 * instantly with no network call.
 *
 * Cached and deduped per (source, mediaType, id) — the same title showing up
 * in multiple rows only ever resolves once.
 */
type ResolvedFetchId = { result: ResolvedId | null; cacheable: boolean };

const idResolutionCache = new Map<string, Promise<ResolvedFetchId>>();

function resolutionCacheKey(source: ResolvableSource, externalId: string, mediaType: MediaKind): string {
	return `${source}:${mediaType}:${externalId}`;
}

function resolveToLibraryId(
	source: ResolvableSource,
	externalId: string,
	mediaType: MediaKind
): Promise<ResolvedFetchId> {
	const target: Indexer = mediaType === "tv" ? "tvdb" : "tmdb";
	if (source === target) {
		return Promise.resolve({ result: { indexer: target, id: externalId }, cacheable: true });
	}
	const cacheKey = resolutionCacheKey(source, externalId, mediaType);
	const cached = idResolutionCache.get(cacheKey);
	if (cached) return cached;

	const pending = resolveExternalId({ from: source, to: target, id: externalId, mediaType })
		.then((r) => ({
			result: r.resolved ? { indexer: target, id: r.id } : null,
			cacheable: true
		}))
		.catch(() => {
			// A request failure isn't a real "no match" answer — don't leave
			// it memoized, or a transient network error would permanently
			// look identical to "this id genuinely doesn't resolve" for the
			// rest of the session.
			idResolutionCache.delete(cacheKey);
			return { result: null, cacheable: false };
		});
	// Set eagerly (before the request settles) so concurrent callers for the
	// same id within the request window dedupe onto this one promise.
	idResolutionCache.set(cacheKey, pending);
	return pending;
}

const resolvedIdState = $state<Record<string, ResolvedId | null | "pending">>({});

/**
 * Reactive read of a source id's resolved (tmdb-movie or tvdb-show) identity
 * in Riven's own library id-space. Used both to build a working details-page
 * link for a source whose raw id Riven never uses directly (Anilist), and by
 * `getLibraryStatus` internally for the same sources.
 *
 * Returns `"pending"` while the resolution is in flight, `null` if the id
 * could not be resolved at all (e.g. an anime with no TMDB/TVDB match yet).
 */
export function getResolvedLibraryId(
	source: ResolvableSource,
	externalId: string,
	mediaType: MediaKind
): ResolvedId | null | "pending" {
	const key = resolutionCacheKey(source, externalId, mediaType);
	const existing = resolvedIdState[key];
	if (existing !== undefined) return existing;

	resolvedIdState[key] = "pending";
	resolveToLibraryId(source, externalId, mediaType).then(({ result, cacheable }) => {
		if (cacheable) {
			resolvedIdState[key] = result;
		} else {
			delete resolvedIdState[key];
		}
	});
	return resolvedIdState[key];
}

/**
 * Reactive read of a suggested item's library status. Sources whose ids
 * aren't already in Riven's own id-space (Anilist always; TMDB for a TV
 * show) resolve first via `getResolvedLibraryId`, then join the same batched
 * lookup as everything else.
 */
export function getLibraryStatus(
	source: ResolvableSource,
	externalId: string,
	mediaType: MediaKind
): LibraryStatusEntry {
	const resolved = getResolvedLibraryId(source, externalId, mediaType);
	if (resolved === "pending") return { status: "loading" };
	if (resolved === null) return { status: "not_found" };

	const key = keyFor(resolved.indexer, resolved.id);
	const existing = cache[key];
	if (existing) return existing;

	cache[key] = { status: "loading" };
	queueFetch(resolved.indexer, resolved.id).then(({ entry, cacheable }) => {
		if (cacheable) {
			cache[key] = entry;
		} else {
			delete cache[key];
		}
	});
	return cache[key];
}

/**
 * Cards currently on screen, by resolution key, refcounted because the same
 * title legitimately appears in several rows at once (trending and
 * recommendations, say) and the last one unmounting is the only one that
 * should stop the watch.
 */
const watchers = new Map<string, number>();

let unsubscribeFromEvents: (() => void) | undefined;

/**
 * Set when events arrived while the tab was in the background, so the catch-up
 * on return is one refetch rather than one per event missed.
 */
let missedEventsWhileHidden = false;

function documentHidden(): boolean {
	return typeof document !== "undefined" && document.hidden;
}

/**
 * Re-read every on-screen card's status. Cards whose id-space translation
 * hasn't landed yet are skipped — the resolution itself will run the lookup
 * when it settles.
 *
 * Deferred entirely while the tab is hidden: nobody can see the badges, and a
 * backgrounded tab left open through a download queue would otherwise keep
 * querying all night for pixels nobody is looking at.
 */
function refetchWatched() {
	if (documentHidden()) {
		missedEventsWhileHidden = true;
		return;
	}
	for (const key of watchers.keys()) {
		const resolved = resolvedIdState[key];
		if (!resolved || resolved === "pending") continue;
		void queueFetch(resolved.indexer, resolved.id);
	}
}

function onVisibilityChange() {
	if (documentHidden() || !missedEventsWhileHidden) return;
	missedEventsWhileHidden = false;
	refetchWatched();
}

/**
 * Register a card as on screen, so its badge follows the library instead of
 * freezing at whatever it read on first paint. Returns the matching
 * unregister — call it on unmount (returning it straight out of an `$effect`
 * does the right thing).
 *
 * The event subscription is opened by the first watcher and closed by the
 * last, so a page with no suggested-content cards holds no stream open.
 */
export function watchLibraryStatus(
	source: ResolvableSource,
	externalId: string,
	mediaType: MediaKind
): () => void {
	const key = resolutionCacheKey(source, externalId, mediaType);
	watchers.set(key, (watchers.get(key) ?? 0) + 1);

	if (!unsubscribeFromEvents) {
		// `subscribeToRivenMediaEvents` debounces and shares one set of streams
		// across the whole app, so a burst of events (a season finishing a
		// dozen episodes) collapses into one batched refetch.
		unsubscribeFromEvents = subscribeToRivenMediaEvents(refetchWatched);
		document.addEventListener("visibilitychange", onVisibilityChange);
	}

	return () => {
		const remaining = (watchers.get(key) ?? 1) - 1;
		if (remaining > 0) {
			watchers.set(key, remaining);
			return;
		}
		watchers.delete(key);
		if (watchers.size === 0) {
			unsubscribeFromEvents?.();
			unsubscribeFromEvents = undefined;
			document.removeEventListener("visibilitychange", onVisibilityChange);
			missedEventsWhileHidden = false;
		}
	};
}

/**
 * Optimistically flip a card from "Request" to a status pill right after a
 * successful request mutation, instead of waiting on a fresh network round
 * trip to notice the newly-created item. Callers pass the *resolved*
 * (tmdb-movie or tvdb-show) identity — the same one the request mutation
 * itself was actually sent with.
 */
export function markLibraryStatusRequested(indexer: Indexer, externalId: string, itemId: number) {
	cache[keyFor(indexer, externalId)] = { status: "found", id: itemId, state: "Indexed" };
}
