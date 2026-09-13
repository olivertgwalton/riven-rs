/**
 * Centralised library mutation operations (reset / retry / remove / pause).
 *
 * Single source of truth for these mutations, which are otherwise duplicated as
 * inline strings across the media item-action and item-request components.
 */

import { gqlClient } from "$lib/graphql-client";

type ItemsMutation =
	| "resetItems"
	| "retryItems"
	| "removeItems"
	| "pauseItems"
	| "unpauseItems";

/** `mutation X($ids) { name(ids: $ids) }` returning the affected count. */
function mutate(name: ItemsMutation) {
	const op = name[0].toUpperCase() + name.slice(1);
	const query = `mutation ${op}($ids: [Int!]!) { ${name}(ids: $ids) }`;
	return async (ids: number[]): Promise<number> =>
		(await gqlClient<Record<ItemsMutation, number>>(query, { ids }))[name];
}

/** Coerce mixed/nullable id inputs into a clean numeric id array. */
export function toNumericIds(
	ids: (string | number | null | undefined)[],
): number[] {
	return ids
		.filter((id): id is string | number => id !== null && id !== undefined)
		.map(Number)
		.filter((n) => !Number.isNaN(n));
}

export const resetItems = mutate("resetItems");
export const retryItems = mutate("retryItems");
export const removeItems = mutate("removeItems");
const pauseItems = mutate("pauseItems");
const unpauseItems = mutate("unpauseItems");

/** Pause or unpause the given items depending on `paused`. Returns the affected count. */
export async function setItemsPaused(
	ids: number[],
	paused: boolean,
): Promise<number> {
	return paused ? pauseItems(ids) : unpauseItems(ids);
}
