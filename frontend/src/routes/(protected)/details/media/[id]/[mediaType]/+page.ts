import type { PageLoad } from "./$types";
import type { MediaDetails } from "$lib/gql/schema";
import { error } from "@sveltejs/kit";
import { createScopedLogger } from "$lib/logger";
import { gqlClient } from "$lib/graphql-client";
import {
	fetchMovieDetails,
	fetchShowDetails,
} from "$lib/services/media-details";
import { resolveExternalId } from "$lib/services/backend-metadata";
import {
	MEDIA_ITEM_STATE_BY_TMDB_QUERY,
	MEDIA_ITEM_STATE_BY_TVDB_QUERY,
	mapMediaItemStateTree,
	type GqlMediaItemStateTree,
} from "$lib/services/riven-media";

const logger = createScopedLogger("media-details");

export type { MediaDetails };

/** Riven's own view of the title, which the page renders alongside the metadata. */
async function rivenState(query: string, variables: Record<string, string>) {
	return gqlClient<{
		mediaItemStateByTmdb?: GqlMediaItemStateTree | null;
		mediaItemStateByTvdb?: GqlMediaItemStateTree | null;
	}>(query, variables)
		.then(
			(data) =>
				mapMediaItemStateTree(
					data.mediaItemStateByTmdb ?? data.mediaItemStateByTvdb ?? null,
				) ?? undefined,
		)
		.catch(() => undefined);
}

export const load = (async ({ params, url }) => {
	const { id, mediaType } = params;

	if (mediaType !== "movie" && mediaType !== "tv") {
		error(400, "Invalid media type");
	}
	if (!id || Number.isNaN(Number(id))) {
		error(400, "Invalid ID");
	}

	try {
		if (mediaType === "movie") {
			// The route carries a TVDB id when it came from a TVDB-sourced credit
			// (e.g. a person's filmography); movies are always fetched by TMDB id.
			let tmdbId = id;
			if (url.searchParams.get("indexer") === "tvdb") {
				const resolved = await resolveExternalId({
					from: "tvdb",
					to: "tmdb",
					id,
					mediaType: "movie",
				}).catch(() => null);

				if (!resolved?.resolved) {
					logger.error(`Failed to resolve TVDB ID ${id} to TMDB ID`);
					error(502, "Unable to resolve movie ID. Please try again later.");
				}
				tmdbId = resolved.id;
			}

			const [details, riven] = await Promise.all([
				fetchMovieDetails(Number(tmdbId)),
				rivenState(MEDIA_ITEM_STATE_BY_TMDB_QUERY, { tmdbId }),
			]);

			return {
				riven,
				rivenPending: false,
				resolvedTvdbId: null,
				mediaDetails: { type: "movie" as const, details },
			};
		}

		// The route carries a TVDB id when it came from the library, a TMDB one
		// otherwise; shows are fetched by TVDB id either way.
		const isTvdbId = url.searchParams.get("indexer") === "tvdb";
		let tvdbId = Number(id);

		if (!isTvdbId) {
			const resolved = await resolveExternalId({
				from: "tmdb",
				to: "tvdb",
				id,
				mediaType: "tv",
			}).catch(() => null);

			if (!resolved?.resolved) {
				logger.error(`Failed to resolve TMDB ID ${id} to TVDB ID`);
				error(502, "Unable to resolve TV show ID. Please try again later.");
			}
			tvdbId = Number(resolved.id);
		}

		const [details, riven] = await Promise.all([
			fetchShowDetails(tvdbId, isTvdbId ? undefined : id),
			rivenState(MEDIA_ITEM_STATE_BY_TVDB_QUERY, { tvdbId: String(tvdbId) }),
		]);

		return {
			riven,
			rivenPending: false,
			resolvedTvdbId: tvdbId,
			mediaDetails: { type: "tv" as const, details },
		};
	} catch (err) {
		// Re-throw SvelteKit errors (like 400, 502) so they render the error page.
		if (err && typeof err === "object" && "status" in err && "body" in err) {
			throw err;
		}
		logger.error("Unexpected error loading media details:", err);
		throw error(500, "Internal Server Error loading media details");
	}
}) satisfies PageLoad;
