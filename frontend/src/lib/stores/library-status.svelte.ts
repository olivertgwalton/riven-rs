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
 */

import { gqlClient } from "$lib/graphql-client";
import { resolveExternalId } from "$lib/services/backend-metadata";

export type LibraryStatusEntry =
	| { status: "loading" }
	| { status: "not_found" }
	| { status: "found"; id: number; state: string };

type Indexer = "tmdb" | "tvdb";
type MediaKind = "movie" | "tv";

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

type PendingBatch = {
	ids: Set<string>;
	resolvers: Map<string, ((entry: LibraryStatusEntry) => void)[]>;
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

async function flushBatch(indexer: Indexer) {
	const batch = batches[indexer];
	if (batch.ids.size === 0) return;

	const ids = Array.from(batch.ids);
	const resolvers = batch.resolvers;
	batches[indexer] = emptyBatch();

	let rows: MediaItemStatusRow[] = [];
	try {
		const field = indexer === "tmdb" ? "mediaItemStatusesByTmdbIds" : "mediaItemStatusesByTvdbIds";
		const result = await gqlClient<Record<string, MediaItemStatusRow[]>>(BATCH_QUERIES[indexer], {
			ids
		});
		rows = result[field] ?? [];
	} catch {
		// A failed batch shouldn't leave every card spinning forever — treat
		// as "unknown" (rendered the same as "not in library") rather than
		// retrying in a loop.
		rows = [];
	}

	const byExternalId = new Map<string, LibraryStatusEntry>();
	for (const row of rows) {
		const extId = indexer === "tmdb" ? row.tmdbId : row.tvdbId;
		if (extId) byExternalId.set(extId, { status: "found", id: row.id, state: row.state });
	}

	for (const id of ids) {
		const entry = byExternalId.get(id) ?? { status: "not_found" };
		cache[keyFor(indexer, id)] = entry;
		for (const resolve of resolvers.get(id) ?? []) resolve(entry);
	}
}

function queueFetch(indexer: Indexer, id: string): Promise<LibraryStatusEntry> {
	const batch = batches[indexer];
	batch.ids.add(id);
	const promise = new Promise<LibraryStatusEntry>((resolve) => {
		const list = batch.resolvers.get(id) ?? [];
		list.push(resolve);
		batch.resolvers.set(id, list);
	});
	scheduleFlush();
	return promise;
}

// TMDB has no independent notion of a TV show's *Riven* identity — every show
// in this app is keyed by TVDB id (see the tmdb->tvdb resolution the details
// page's +page.ts does before it will even fetch show metadata). A TMDB-
// sourced TV card therefore has to resolve to a TVDB id first before its
// library status can be looked up at all. Deduped per id so the same show
// appearing in multiple rows doesn't re-resolve repeatedly.
const tvResolutionCache = new Map<string, Promise<string | null>>();
function resolveTmdbTvToTvdb(tmdbId: string): Promise<string | null> {
	let pending = tvResolutionCache.get(tmdbId);
	if (!pending) {
		pending = resolveExternalId({ from: "tmdb", to: "tvdb", id: tmdbId, mediaType: "tv" })
			.then((r) => (r.resolved ? r.id : null))
			.catch(() => null);
		tvResolutionCache.set(tmdbId, pending);
	}
	return pending;
}

/**
 * Reactive read of a suggested item's library status. First call for a given
 * (indexer, externalId) kicks off the batched fetch (or, for a TMDB-sourced
 * TV show, a resolve-to-TVDB step first) and returns `{status: "loading"}`;
 * once the result lands, the returned entry updates in place (the backing
 * store is `$state`, so callers reading this inside a component re-render).
 */
export function getLibraryStatus(
	indexer: Indexer,
	externalId: string,
	mediaType: MediaKind
): LibraryStatusEntry {
	if (indexer === "tmdb" && mediaType === "tv") {
		const resolveKey = `tmdb-tv:${externalId}`;
		const existing = cache[resolveKey];
		if (existing) return existing;

		cache[resolveKey] = { status: "loading" };
		resolveTmdbTvToTvdb(externalId)
			.then((tvdbId) => {
				if (!tvdbId) {
					cache[resolveKey] = { status: "not_found" };
					return undefined;
				}
				return queueFetch("tvdb", tvdbId).then((entry) => {
					cache[resolveKey] = entry;
					cache[keyFor("tvdb", tvdbId)] = entry;
				});
			})
			.catch(() => {
				cache[resolveKey] = { status: "not_found" };
			});
		return cache[resolveKey];
	}

	const effectiveIndexer: Indexer = indexer === "tvdb" ? "tvdb" : "tmdb";
	const key = keyFor(effectiveIndexer, externalId);
	const existing = cache[key];
	if (existing) return existing;

	cache[key] = { status: "loading" };
	queueFetch(effectiveIndexer, externalId).then((entry) => {
		cache[key] = entry;
	});
	return cache[key];
}

/**
 * Optimistically flip a card from "Request" to a status pill right after a
 * successful request mutation, instead of waiting on a fresh network round
 * trip to notice the newly-created item.
 */
export function markLibraryStatusRequested(
	indexer: Indexer,
	externalId: string,
	mediaType: MediaKind,
	itemId: number
) {
	const entry: LibraryStatusEntry = { status: "found", id: itemId, state: "Indexed" };
	if (indexer === "tmdb" && mediaType === "tv") {
		cache[`tmdb-tv:${externalId}`] = entry;
		return;
	}
	cache[keyFor(indexer === "tvdb" ? "tvdb" : "tmdb", externalId)] = entry;
}
