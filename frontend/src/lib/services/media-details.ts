import { gqlClient } from "$lib/graphql-client";
import type { MediaDetails, PersonDetails } from "$lib/gql/schema";

/**
 * Everything the detail pages render. Movies and shows come back through one
 * `MediaDetails` shape, so the selection is shared — the backend fills the
 * fields the other source has no answer for.
 */
const MEDIA_DETAILS_FIELDS = `
    id type title overview status year
    releaseDate formattedRuntime homepage certification
    posterPath backdropPath logo
    originalLanguage originCountry
    budget revenue
    imdbId tmdbId
    externalIds { source id }
    genres { id name slug }
    cast { id name character profilePath externalSource }
    crew { id name character profilePath externalSource }
    spokenLanguages { englishName iso6391 name }
    productionCompanies { id name logoPath originCountry }
    trailer { id name site key url }
    collection { id name posterPath backdropPath }
    seasons { id number name image overview }
    episodes { id name overview aired runtime image number absoluteNumber seasonNumber }
    episodeCount
    recommendations { id title posterPath mediaType year voteAverage voteCount indexer }
    similar { id title posterPath mediaType year voteAverage voteCount indexer }
    traktRecommendations { id title posterPath mediaType year voteAverage voteCount indexer }
`;

const PERSON_DETAILS_FIELDS = `
    id indexer name biography birthday deathday placeOfBirth
    profilePath knownForDepartment gender homepage imdbId tvdbUrl
    externalIds { source id }
    alsoKnownAs
    castCredits {
        id title originalTitle character job department
        posterPath backdropPath releaseDate year mediaType
        voteAverage popularity indexer
    }
    crewCredits {
        id title originalTitle character job department
        posterPath backdropPath releaseDate year mediaType
        voteAverage popularity indexer
    }
`;

const MOVIE_DETAILS_QUERY = `query($id: Int!) {
    movieDetails(id: $id) { ${MEDIA_DETAILS_FIELDS} }
}`;

const SHOW_DETAILS_QUERY = `query($id: Int!, $tmdbId: String) {
    showDetails(id: $id, tmdbId: $tmdbId) { ${MEDIA_DETAILS_FIELDS} }
}`;

const PERSON_DETAILS_QUERY = `query($id: Int!, $indexer: String) {
    personDetails(id: $id, indexer: $indexer) { ${PERSON_DETAILS_FIELDS} }
}`;

const COMPANY_DETAILS_QUERY = `query($id: Int!) {
    companyDetails(id: $id) { ${PERSON_DETAILS_FIELDS} }
}`;

export async function fetchMovieDetails(id: number) {
	const data = await gqlClient<{ movieDetails: MediaDetails }>(
		MOVIE_DETAILS_QUERY,
		{ id },
	);
	return data.movieDetails;
}

export async function fetchShowDetails(id: number, tmdbId?: string) {
	const data = await gqlClient<{ showDetails: MediaDetails }>(
		SHOW_DETAILS_QUERY,
		{ id, tmdbId },
	);
	return data.showDetails;
}

export async function fetchPersonDetails(id: number, indexer?: string) {
	const data = await gqlClient<{ personDetails: PersonDetails }>(
		PERSON_DETAILS_QUERY,
		{ id, indexer },
	);
	return data.personDetails;
}

export async function fetchCompanyDetails(id: number) {
	const data = await gqlClient<{ companyDetails: PersonDetails }>(
		COMPANY_DETAILS_QUERY,
		{ id },
	);
	return data.companyDetails;
}
