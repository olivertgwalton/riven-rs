import { gqlSubscribeClient } from "$lib/graphql-client";
import {
	MOVIE_REQUESTED_SUBSCRIPTION,
	SHOW_INDEXED_SUBSCRIPTION,
	SHOW_REQUESTED_SUBSCRIPTION,
	SHOW_REQUEST_UPDATED_SUBSCRIPTION,
} from "$lib/services/riven-media";

const MEDIA_EVENT_SUBSCRIPTIONS = [
	MOVIE_REQUESTED_SUBSCRIPTION,
	SHOW_REQUESTED_SUBSCRIPTION,
	SHOW_REQUEST_UPDATED_SUBSCRIPTION,
	SHOW_INDEXED_SUBSCRIPTION,
	`subscription RivenItemScraped {
        itemScraped
    }`,
	`subscription RivenItemDownloaded {
        itemDownloaded
    }`,
	`subscription RivenItemFailed {
        itemFailed
    }`,
	`subscription RivenItemsDeleted {
        itemsDeleted
    }`,
];

/**
 * The event streams themselves, opened once for the whole app rather than
 * once per caller. Several parts of a page want the same events at the same
 * time — the homepage's Recently Added row, the library grid, the dashboard
 * counters, every suggested-content card's status badge — and each one
 * opening its own set meant this list of operations multiplied by the number
 * of callers on screen. They carry no arguments and no per-caller filtering,
 * so there is nothing to gain from separate streams.
 */
const listeners = new Set<() => void>();
let unsubscribeTransport: (() => void) | undefined;
let teardownTimer: ReturnType<typeof setTimeout> | undefined;

/**
 * How long the streams stay open after the last listener leaves. A client-side
 * navigation unmounts the old page's listeners before the new page's mount,
 * which without this would tear down and immediately re-open every stream on
 * every navigation.
 */
const TEARDOWN_GRACE_MS = 2_000;

function openTransport() {
	clearTimeout(teardownTimer);
	teardownTimer = undefined;
	if (unsubscribeTransport) return;

	const unsubscribers = MEDIA_EVENT_SUBSCRIPTIONS.map((subscription) =>
		gqlSubscribeClient<Record<string, unknown>>(subscription, undefined, {
			onData: () => {
				for (const listener of listeners) listener();
			},
			onError: () => {
				// Callers keep their last successful data snapshot. The shared GraphQL
				// subscription client owns transport-level retry behaviour where needed.
			},
		}),
	);

	unsubscribeTransport = () => {
		for (const unsubscribe of unsubscribers) unsubscribe();
	};
}

function closeTransportWhenIdle() {
	if (listeners.size > 0 || teardownTimer) return;
	teardownTimer = setTimeout(() => {
		teardownTimer = undefined;
		if (listeners.size > 0) return;
		unsubscribeTransport?.();
		unsubscribeTransport = undefined;
	}, TEARDOWN_GRACE_MS);
}

export function subscribeToRivenMediaEvents(
	refresh: () => void | Promise<void>,
	debounceMs = 250,
): () => void {
	let active = true;
	let refreshTimer: ReturnType<typeof setTimeout> | undefined;

	function refreshSoon() {
		clearTimeout(refreshTimer);
		refreshTimer = setTimeout(() => {
			if (!active) return;
			void refresh();
		}, debounceMs);
	}

	listeners.add(refreshSoon);
	openTransport();

	return () => {
		active = false;
		clearTimeout(refreshTimer);
		listeners.delete(refreshSoon);
		closeTransportWhenIdle();
	};
}
