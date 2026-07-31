import type { PageLoad } from "./$types";
import { gqlClient } from "$lib/graphql-client";
import { error } from "@sveltejs/kit";

const CALENDAR_QUERY = `
    query Calendar($limit: Int) {
        calendar(limit: $limit) {
            itemId
            showTitle
            itemType
            airedAt
            season
            episode
            tmdbId
            tvdbId
            lastState
        }
    }
`;

export const load = (async () => {
	try {
		const data = await gqlClient<{
			calendar: {
				itemId: number;
				showTitle: string;
				itemType: string;
				airedAt?: string | null;
				season?: number | null;
				episode?: number | null;
				tmdbId?: string | null;
				tvdbId?: string | null;
				lastState: string;
			}[];
		}>(CALENDAR_QUERY, { limit: 200 });

		const items = data.calendar.map((i) => ({
			item_id: i.itemId,
			show_title: i.showTitle,
			item_type: i.itemType,
			aired_at: i.airedAt ?? undefined,
			season: i.season ?? undefined,
			episode: i.episode ?? undefined,
			tmdb_id: i.tmdbId ?? undefined,
			tvdb_id: i.tvdbId ?? undefined,
			last_state: i.lastState,
		}));

		return { calendar: { data: items } };
	} catch {
		error(500, "Unable to fetch calendar data");
	}
}) satisfies PageLoad;
