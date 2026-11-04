import type { PageLoad } from "./$types";
import { gqlClient } from "#lib/graphql-client.js";
import {
	MUSIC_ARTIST_QUERY,
	type MusicArtistDetails,
} from "#lib/services/music.js";

export const load: PageLoad = async ({ params }) => {
	const data = await gqlClient<{ musicArtist: MusicArtistDetails }>(
		MUSIC_ARTIST_QUERY,
		{ id: params.id },
	);
	return { details: data.musicArtist };
};
