/**
 * Centralised library mutation operations (reset / retry / remove / pause).
 *
 * Single source of truth for these mutations, which are otherwise duplicated as
 * inline strings across the media item-action and item-request components.
 */

import { gqlClient } from "$lib/graphql-client";

const RESET_ITEMS_MUTATION = `mutation ResetItems($ids: [Int!]!) { resetItems(ids: $ids) }`;
const RETRY_ITEMS_MUTATION = `mutation RetryItems($ids: [Int!]!) { retryItems(ids: $ids) }`;
const REMOVE_ITEMS_MUTATION = `mutation RemoveItems($ids: [Int!]!) { removeItems(ids: $ids) }`;
const PAUSE_ITEMS_MUTATION = `mutation PauseItems($ids: [Int!]!) { pauseItems(ids: $ids) }`;
const UNPAUSE_ITEMS_MUTATION = `mutation UnpauseItems($ids: [Int!]!) { unpauseItems(ids: $ids) }`;

/** Coerce mixed/nullable id inputs into a clean numeric id array. */
export function toNumericIds(
	ids: (string | number | null | undefined)[],
): number[] {
	return ids
		.filter((id): id is string | number => id !== null && id !== undefined)
		.map(Number)
		.filter((n) => !Number.isNaN(n));
}

/** Reset the given items on the backend. Returns the affected count. */
export async function resetItems(ids: number[]): Promise<number> {
	const result = await gqlClient<{ resetItems: number }>(RESET_ITEMS_MUTATION, {
		ids,
	});
	return result.resetItems;
}

/** Retry the given items on the backend. Returns the affected count. */
export async function retryItems(ids: number[]): Promise<number> {
	const result = await gqlClient<{ retryItems: number }>(RETRY_ITEMS_MUTATION, {
		ids,
	});
	return result.retryItems;
}

/** Remove the given items on the backend. Returns the affected count. */
export async function removeItems(ids: number[]): Promise<number> {
	const result = await gqlClient<{ removeItems: number }>(
		REMOVE_ITEMS_MUTATION,
		{ ids },
	);
	return result.removeItems;
}

async function pauseItems(ids: number[]): Promise<number> {
	const result = await gqlClient<{ pauseItems: number }>(PAUSE_ITEMS_MUTATION, {
		ids,
	});
	return result.pauseItems;
}

async function unpauseItems(ids: number[]): Promise<number> {
	const result = await gqlClient<{ unpauseItems: number }>(
		UNPAUSE_ITEMS_MUTATION,
		{ ids },
	);
	return result.unpauseItems;
}

/** Pause or unpause the given items depending on `paused`. Returns the affected count. */
export async function setItemsPaused(
	ids: number[],
	paused: boolean,
): Promise<number> {
	return paused ? pauseItems(ids) : unpauseItems(ids);
}
