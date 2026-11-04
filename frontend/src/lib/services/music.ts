import type {
	MusicArtist,
	MusicArtistDetails,
	RequestItemMutationResponse,
} from "#lib/gql/schema.js";

export type { MusicArtist, MusicArtistDetails };

export const SEARCH_MUSIC_ARTISTS_QUERY = `query SearchMusicArtists($query: String!) {
    searchMusicArtists(query: $query) { id name country disambiguation kind }
}`;

export const MUSIC_ARTIST_QUERY = `query MusicArtist($id: String!) {
    musicArtist(id: $id) {
        artist { id name country disambiguation kind }
        albums { id title releaseDate primaryType coverUrl state }
        libraryId
        followsDiscography
    }
}`;

export const REQUEST_ARTIST_MUTATION = `mutation RequestArtist($input: ArtistRequestInput!) {
    requestArtist(input: $input) { success message statusText }
}`;

export type RequestArtistResponse = {
	requestArtist: Pick<
		RequestItemMutationResponse,
		"success" | "message" | "statusText"
	>;
};
