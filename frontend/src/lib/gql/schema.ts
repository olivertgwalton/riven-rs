export type Maybe<T> = T | null;
export type InputMaybe<T> = Maybe<T>;
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: { input: string; output: string; }
  String: { input: string; output: string; }
  Boolean: { input: boolean; output: boolean; }
  Int: { input: number; output: number; }
  Float: { input: number; output: number; }
  /**
   * Implement the DateTime<Utc> scalar
   *
   * The input/output is a string in RFC3339 format.
   */
  DateTime: { input: unknown; output: unknown; }
  /** A scalar that can represent any JSON value. */
  JSON: { input: unknown; output: unknown; }
  /**
   * ISO 8601 calendar date without timezone.
   * Format: %Y-%m-%d
   *
   * # Examples
   *
   * * `1994-11-13`
   * * `2000-02-24`
   */
  NaiveDate: { input: unknown; output: unknown; }
};

export type ActivePlaybackSession = {
  clientName?: Maybe<Scalars['String']['output']>;
  deviceName?: Maybe<Scalars['String']['output']>;
  durationSeconds?: Maybe<Scalars['Int']['output']>;
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  /**
   * Artwork for the item, as a path on this riven instance — fetch it from
   * here, not from the media server.
   */
  imageUrl?: Maybe<Scalars['String']['output']>;
  itemTitle: Scalars['String']['output'];
  itemType?: Maybe<Scalars['String']['output']>;
  parentTitle?: Maybe<Scalars['String']['output']>;
  playbackMethod: PlaybackMethod;
  playbackState: PlaybackState;
  positionSeconds?: Maybe<Scalars['Int']['output']>;
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  server: Scalars['String']['output'];
  userName?: Maybe<Scalars['String']['output']>;
};

export type AnilistListItem = {
  id: Scalars['Int']['output'];
  mediaType: Scalars['String']['output'];
  posterPath?: Maybe<Scalars['String']['output']>;
  title: Scalars['String']['output'];
  year: Scalars['String']['output'];
};

export type AnilistMappings = {
  anilistId: Scalars['Int']['output'];
  tmdbId?: Maybe<Scalars['Int']['output']>;
  tvdbId?: Maybe<Scalars['Int']['output']>;
};

export type AnilistPage = {
  page: Scalars['Int']['output'];
  results: Array<AnilistListItem>;
};

export type AnilistRating = {
  id: Scalars['Int']['output'];
  score?: Maybe<Scalars['Float']['output']>;
};

export type AudioTrack = {
  channels?: Maybe<Scalars['Int']['output']>;
  codec?: Maybe<Scalars['String']['output']>;
  language?: Maybe<Scalars['String']['output']>;
};

/** One cache's live figures. */
export type CacheHealth = {
  bytesMax: Scalars['Int']['output'];
  bytesUsed: Scalars['Int']['output'];
  entries: Scalars['Int']['output'];
  /** Over all lookups since start, 0.0–1.0. */
  hitRate: Scalars['Float']['output'];
  hits: Scalars['Int']['output'];
  misses: Scalars['Int']['output'];
  /** `read-ahead`, `nzb-meta`, `segment` or `segment-sizes`. */
  name: Scalars['String']['output'];
};

export type CalendarEntry = {
  airedAt?: Maybe<Scalars['String']['output']>;
  episode?: Maybe<Scalars['Int']['output']>;
  itemId: Scalars['Int']['output'];
  itemType: Scalars['String']['output'];
  lastState: Scalars['String']['output'];
  season?: Maybe<Scalars['Int']['output']>;
  showTitle: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
};

/**
 * Something a caller may be allowed to do.
 *
 * One variant per *action*, following riven-ts's access-control statements
 * (`item: ["request", "delete", "reset", "pause", "retry", "scrape"]`) rather
 * than a single "manage the library" lump. The distinction earns its keep the
 * moment you want a role that can retry a failed grab but not delete anything.
 *
 * riven-ts declared these in the frontend and enforced none of them — its
 * GraphQL server had no auth at all. Here they gate the resolvers.
 *
 * [`Capability::minimum_role`] is the *only* place a threshold is written down.
 * Both halves of authorisation read it: the guards that reject a mutation, and
 * the `viewer` query the UI renders from. They cannot disagree, because there
 * is nothing to disagree with.
 */
export type Capability =
  /**
   * Put an item straight into the library, bypassing the request queue.
   * No riven-ts counterpart — `addItem`/`discoverItem` are riven-rs's.
   */
  | 'ADD_ITEMS'
  | 'DELETE_ITEMS'
  /** Settings, profiles, indexing and setup — riven-ts's `adminAc` statements. */
  | 'MANAGE_SETTINGS'
  | 'PAUSE_ITEMS'
  /** Ask for something to be added. The only action an ordinary user has. */
  | 'REQUEST_ITEMS'
  | 'RESET_ITEMS'
  | 'RETRY_ITEMS'
  /**
   * Find and choose a release: manual scrape, stream discovery, and the
   * download that commits the chosen one.
   */
  | 'SCRAPE_ITEMS';

export type CastMember = {
  character?: Maybe<Scalars['String']['output']>;
  /** `tmdb` or `tvdb` — which indexer this person can be looked up in. */
  externalSource: Scalars['String']['output'];
  id: Scalars['Int']['output'];
  name: Scalars['String']['output'];
  profilePath?: Maybe<Scalars['String']['output']>;
};

export type ContentRating =
  | 'G'
  | 'NC_17'
  | 'PG'
  | 'PG_13'
  | 'R'
  | 'TV_14'
  | 'TV_G'
  | 'TV_MA'
  | 'TV_PG'
  | 'TV_Y'
  | 'TV_Y7';

export type DebridUserInfo = {
  cooldownUntil?: Maybe<Scalars['String']['output']>;
  email?: Maybe<Scalars['String']['output']>;
  points?: Maybe<Scalars['Int']['output']>;
  premiumUntil?: Maybe<Scalars['String']['output']>;
  store: Scalars['String']['output'];
  subscriptionStatus?: Maybe<Scalars['String']['output']>;
  totalDownloadedBytes?: Maybe<Scalars['Int']['output']>;
  username?: Maybe<Scalars['String']['output']>;
};

export type DiscoveredStream = {
  fileSizeBytes?: Maybe<Scalars['Int']['output']>;
  infoHash: Scalars['String']['output'];
  isCached: Scalars['Boolean']['output'];
  itemType: MediaItemType;
  key: Scalars['String']['output'];
  magnet: Scalars['String']['output'];
  parsedData?: Maybe<Scalars['JSON']['output']>;
  rank?: Maybe<Scalars['Int']['output']>;
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  title: Scalars['String']['output'];
};

export type DownloadMediaItemMutationInput = {
  id: Scalars['Int']['input'];
  processedBy: Scalars['String']['input'];
  torrent: Scalars['JSON']['input'];
};

export type DownloadMediaItemMutationResponse = {
  item?: Maybe<MediaItemUnion>;
  message: Scalars['String']['output'];
  statusText: MutationStatusText;
  success: Scalars['Boolean']['output'];
};

export type Episode = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  /** Always 1 — an episode has exactly one expected media file. */
  expectedFileCount: Scalars['Int']['output'];
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  /** Lookup keys: `["abs:{absoluteNumber}", "{seasonNumber}:{episodeNumber}"]`. */
  lookupKeys: Array<Scalars['String']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  /** The parent season for this episode. */
  season: Season;
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  streams: Array<Stream>;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};


export type EpisodeStreamsArgs = {
  infoHashes?: InputMaybe<Array<Scalars['String']['input']>>;
};

/** Episode with its primary filesystem entry (media file only). */
export type EpisodeFull = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  failedAttempts: Scalars['Int']['output'];
  filesystemEntries: Array<FileSystemEntry>;
  filesystemEntry?: Maybe<FileSystemEntry>;
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

/** Lightweight episode state used for live state subscriptions. */
export type EpisodeState = {
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  id: Scalars['Int']['output'];
  state: MediaItemState;
};

export type EpisodeSummary = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  aired?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  image?: Maybe<Scalars['String']['output']>;
  name?: Maybe<Scalars['String']['output']>;
  number?: Maybe<Scalars['Int']['output']>;
  overview?: Maybe<Scalars['String']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  seasonNumber?: Maybe<Scalars['Int']['output']>;
};

export type ExternalId = {
  id: Scalars['String']['output'];
  source: Scalars['String']['output'];
};

export type FileSystemEntry = {
  createdAt: Scalars['DateTime']['output'];
  downloadUrl?: Maybe<Scalars['String']['output']>;
  entryType: FileSystemEntryType;
  fileHash?: Maybe<Scalars['String']['output']>;
  fileSize: Scalars['Int']['output'];
  id: Scalars['Int']['output'];
  language?: Maybe<Scalars['String']['output']>;
  libraryProfiles?: Maybe<Scalars['JSON']['output']>;
  mediaItemId: Scalars['Int']['output'];
  /**
   * The stored document, typed. A row written by an older build that is
   * missing fields still reads — every field defaults.
   */
  mediaMetadata?: Maybe<MediaMetadata>;
  opensubtitlesId?: Maybe<Scalars['String']['output']>;
  originalFilename?: Maybe<Scalars['String']['output']>;
  parentOriginalFilename?: Maybe<Scalars['String']['output']>;
  path: Scalars['String']['output'];
  plugin?: Maybe<Scalars['String']['output']>;
  provider?: Maybe<Scalars['String']['output']>;
  providerDownloadId?: Maybe<Scalars['String']['output']>;
  rankingProfileName?: Maybe<Scalars['String']['output']>;
  resolution?: Maybe<Scalars['String']['output']>;
  sourceId?: Maybe<Scalars['String']['output']>;
  sourceProvider?: Maybe<Scalars['String']['output']>;
  streamId?: Maybe<Scalars['Int']['output']>;
  streamUrl?: Maybe<Scalars['String']['output']>;
  subtitleContent?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  usenetFileIndex?: Maybe<Scalars['Int']['output']>;
  usenetInfoHash?: Maybe<Scalars['String']['output']>;
  videoFileSize?: Maybe<Scalars['Int']['output']>;
};

export type FileSystemEntryType =
  | 'MEDIA'
  | 'SUBTITLE';

export type Genre = {
  id: Scalars['Int']['output'];
  name: Scalars['String']['output'];
  /** TVDB only. */
  slug?: Maybe<Scalars['String']['output']>;
};

export type IdResolution = {
  id: Scalars['String']['output'];
  resolved: Scalars['Boolean']['output'];
};

export type IndexEpisodeInput = {
  absoluteNumber?: InputMaybe<Scalars['Int']['input']>;
  /** ISO date string (YYYY-MM-DD). */
  airedAt?: InputMaybe<Scalars['String']['input']>;
  contentRating?: InputMaybe<ContentRating>;
  number: Scalars['Int']['input'];
  posterPath?: InputMaybe<Scalars['String']['input']>;
  runtime?: InputMaybe<Scalars['Int']['input']>;
  title: Scalars['String']['input'];
};

/** Input for the `indexMovie` mutation. */
export type IndexMovieInput = {
  /** Locale → title aliases, e.g. `{"de": ["Titel"]}`. */
  aliases?: InputMaybe<Scalars['JSON']['input']>;
  contentRating?: InputMaybe<ContentRating>;
  country?: InputMaybe<Scalars['String']['input']>;
  genres: Array<Scalars['String']['input']>;
  /** ID of the `ItemRequest` being indexed. */
  id: Scalars['Int']['input'];
  imdbId?: InputMaybe<Scalars['String']['input']>;
  language?: InputMaybe<Scalars['String']['input']>;
  posterUrl?: InputMaybe<Scalars['String']['input']>;
  rating?: InputMaybe<Scalars['Float']['input']>;
  /** ISO date string (YYYY-MM-DD) for the theatrical release. */
  releaseDate?: InputMaybe<Scalars['String']['input']>;
  runtime?: InputMaybe<Scalars['Int']['input']>;
  title: Scalars['String']['input'];
};

/** Structured response returned by `indexMovie`. */
export type IndexMovieMutationResponse = {
  message: Scalars['String']['output'];
  movie?: Maybe<Movie>;
  statusText: MutationStatusText;
  success: Scalars['Boolean']['output'];
};

export type IndexSeasonInput = {
  episodes: Array<IndexEpisodeInput>;
  number: Scalars['Int']['input'];
  title?: InputMaybe<Scalars['String']['input']>;
};

/** Input for the `indexShow` mutation. */
export type IndexShowInput = {
  /** Locale → title aliases, e.g. `{"de": ["Titel"]}`. */
  aliases?: InputMaybe<Scalars['JSON']['input']>;
  contentRating?: InputMaybe<ContentRating>;
  country?: InputMaybe<Scalars['String']['input']>;
  genres: Array<Scalars['String']['input']>;
  /** ID of the `ItemRequest` being indexed. */
  id: Scalars['Int']['input'];
  imdbId?: InputMaybe<Scalars['String']['input']>;
  language?: InputMaybe<Scalars['String']['input']>;
  network?: InputMaybe<Scalars['String']['input']>;
  posterUrl?: InputMaybe<Scalars['String']['input']>;
  rating?: InputMaybe<Scalars['Float']['input']>;
  seasons: Array<IndexSeasonInput>;
  status: ShowStatus;
  title: Scalars['String']['input'];
};

/** Structured response returned by `indexShow`. */
export type IndexShowMutationResponse = {
  message: Scalars['String']['output'];
  show?: Maybe<Show>;
  statusText: MutationStatusText;
  success: Scalars['Boolean']['output'];
};

export type InstanceStatus = {
  /** Human-readable reasons setup can't be completed yet (empty when ready). */
  blockers: Array<Scalars['String']['output']>;
  enabledProfileCount: Scalars['Int']['output'];
  enabledValidPluginCount: Scalars['Int']['output'];
  /** Whether the minimum viable configuration exists to finish setup. */
  readyToComplete: Scalars['Boolean']['output'];
  setupCompleted: Scalars['Boolean']['output'];
};

export type ItemRequest = {
  completedAt?: Maybe<Scalars['DateTime']['output']>;
  createdAt: Scalars['DateTime']['output'];
  externalRequestId?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  isPartialRequest: Scalars['Boolean']['output'];
  requestType: ItemRequestType;
  requestedBy?: Maybe<Scalars['String']['output']>;
  seasons?: Maybe<Scalars['JSON']['output']>;
  state: ItemRequestState;
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
};

export type ItemRequestState =
  | 'COMPLETED'
  | 'FAILED'
  | 'ONGOING'
  | 'REQUESTED'
  /**
   * An existing request had additional seasons appended after it was
   * already completed/ongoing. Signals to the indexer that it should
   * re-process this show; the recompute pipeline will then transition it
   * back to its derived state (completed/ongoing/unreleased).
   */
  | 'REQUESTED_ADDITIONAL_SEASONS'
  | 'UNRELEASED';

export type ItemRequestType =
  | 'MOVIE'
  | 'SHOW';

export type ItemsPage = {
  items: Array<MediaItemListRow>;
  limit: Scalars['Int']['output'];
  page: Scalars['Int']['output'];
  totalItems: Scalars['Int']['output'];
  totalPages: Scalars['Int']['output'];
};

export type LibraryStats = {
  completed: Scalars['Int']['output'];
  completionRate: Scalars['Float']['output'];
  failed: Scalars['Int']['output'];
  incompleteItems: Scalars['Int']['output'];
  indexed: Scalars['Int']['output'];
  ongoing: Scalars['Int']['output'];
  partiallyCompleted: Scalars['Int']['output'];
  paused: Scalars['Int']['output'];
  scraped: Scalars['Int']['output'];
  totalEpisodes: Scalars['Int']['output'];
  totalItems: Scalars['Int']['output'];
  totalMovies: Scalars['Int']['output'];
  totalSeasons: Scalars['Int']['output'];
  totalShows: Scalars['Int']['output'];
  unreleased: Scalars['Int']['output'];
};

export type LogEntry = {
  /**
   * The line's structured fields (media item id, title, info hash, error,
   * …) as a JSON object, with `message` removed since it is exposed above.
   * Without these a log line reads as a sentence with no subject — "no
   * scraper returned any stream for this item", but which item?
   */
  fields?: Maybe<Scalars['String']['output']>;
  level?: Maybe<Scalars['String']['output']>;
  message?: Maybe<Scalars['String']['output']>;
  target?: Maybe<Scalars['String']['output']>;
  timestamp?: Maybe<Scalars['String']['output']>;
};

/**
 * A TMDB `/3/movie/{id}` payload (appended with `external_ids,images,
 * recommendations,similar,videos,credits,release_dates`) or a TVDB
 * `/series/{id}/extended` payload unwrapped from its `data` envelope.
 */
export type MediaDetails = {
  backdropPath?: Maybe<Scalars['String']['output']>;
  budget?: Maybe<Scalars['Int']['output']>;
  cast: Array<CastMember>;
  certification: Scalars['String']['output'];
  collection?: Maybe<MovieCollection>;
  crew: Array<CastMember>;
  episodeCount: Scalars['Int']['output'];
  episodes: Array<EpisodeSummary>;
  externalIds: Array<ExternalId>;
  formattedRuntime?: Maybe<Scalars['String']['output']>;
  genres: Array<Genre>;
  homepage?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  logo?: Maybe<Scalars['String']['output']>;
  originCountry: Array<Scalars['String']['output']>;
  originalLanguage?: Maybe<Scalars['String']['output']>;
  overview?: Maybe<Scalars['String']['output']>;
  posterPath?: Maybe<Scalars['String']['output']>;
  productionCompanies: Array<ProductionCompany>;
  /**
   * TVDB has no recommendation feed; the fields exist so the UI reads movies
   * and shows through one shape.
   */
  recommendations: Array<TmdbListItem>;
  releaseDate?: Maybe<Scalars['String']['output']>;
  revenue?: Maybe<Scalars['Int']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  seasons: Array<SeasonSummary>;
  similar: Array<TmdbListItem>;
  spokenLanguages: Array<SpokenLanguage>;
  status?: Maybe<Scalars['String']['output']>;
  /**
   * TMDB calls it `title`, TVDB `name`; the resolver has already replaced
   * TVDB's with the English translation, so neither needs choosing here.
   */
  title?: Maybe<Scalars['String']['output']>;
  /** A show carries TMDB's id among its remote ids; a movie is one. */
  tmdbId?: Maybe<Scalars['Int']['output']>;
  trailer?: Maybe<Trailer>;
  traktRecommendations: Array<TmdbListItem>;
  type: Scalars['String']['output'];
  /** TMDB scores out of 10; TVDB's `score` is its own scale. */
  voteAverage?: Maybe<Scalars['Float']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

export type MediaItem = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

/** Media item (movie or show) with filesystem entry and, for shows, full season/episode tree. */
export type MediaItemFull = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  failedAttempts: Scalars['Int']['output'];
  filesystemEntries: Array<FileSystemEntry>;
  filesystemEntry?: Maybe<FileSystemEntry>;
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  seasons: Array<SeasonFull>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

export type MediaItemListRow = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  showId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL for the parent show, resolved the same way as the
   * row's own poster.
   */
  showPosterPath?: Maybe<Scalars['String']['output']>;
  showStatus?: Maybe<ShowStatus>;
  showTitle?: Maybe<Scalars['String']['output']>;
  showTmdbId?: Maybe<Scalars['String']['output']>;
  showTvdbId?: Maybe<Scalars['String']['output']>;
  state: MediaItemState;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

export type MediaItemState =
  | 'Completed'
  | 'Failed'
  | 'Indexed'
  | 'Ongoing'
  | 'PartiallyCompleted'
  | 'Paused'
  | 'Scraped'
  | 'Unreleased';

/** Lightweight media state tree used for live state subscriptions. */
export type MediaItemStateTree = {
  expectedFileCount: Scalars['Int']['output'];
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  seasons: Array<SeasonState>;
  state: MediaItemState;
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
};

export type MediaItemType =
  | 'EPISODE'
  | 'MOVIE'
  | 'SEASON'
  | 'SHOW';

/** Discriminated union of all concrete media item types. */
export type MediaItemUnion = Episode | Movie | Season | Show;

/**
 * The `media_metadata` document, derived from a release filename by
 * `riven_rank::derive_media_metadata` and stored as JSON on the entry.
 *
 * It is typed here rather than handed over as a `JSON` scalar because both
 * clients used to re-parse the blob — the Swift app in `FileDetailsView` and
 * the web frontend in `lib/types/riven.ts` — each maintaining its own copy of
 * a shape only this repo defines.
 */
export type MediaMetadata = {
  audioTracks: Array<AudioTrack>;
  bitrate?: Maybe<Scalars['Int']['output']>;
  containerFormat: Array<Scalars['String']['output']>;
  /** `parsed` when derived from the filename; set by whoever wrote the row. */
  dataSource?: Maybe<Scalars['String']['output']>;
  duration?: Maybe<Scalars['Float']['output']>;
  filename?: Maybe<Scalars['String']['output']>;
  isProper: Scalars['Boolean']['output'];
  isRemux: Scalars['Boolean']['output'];
  isRepack: Scalars['Boolean']['output'];
  originalFilename?: Maybe<Scalars['String']['output']>;
  parsedTitle?: Maybe<Scalars['String']['output']>;
  qualitySource?: Maybe<Scalars['String']['output']>;
  subtitleTracks: Array<SubtitleTrack>;
  video?: Maybe<VideoMetadata>;
  year?: Maybe<Scalars['Int']['output']>;
};

export type Movie = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  /** Always 1 — a movie has exactly one expected media file. */
  expectedFileCount: Scalars['Int']['output'];
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  streams: Array<Stream>;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};


export type MovieStreamsArgs = {
  infoHashes?: InputMaybe<Array<Scalars['String']['input']>>;
};

/** The franchise a movie belongs to, when TMDB says it belongs to one. */
export type MovieCollection = {
  backdropPath?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  name: Scalars['String']['output'];
  posterPath?: Maybe<Scalars['String']['output']>;
};

/** Input for requesting a movie to be tracked. */
export type MovieRequestInput = {
  /** External request ID for correlation with the originating content service. */
  externalRequestId?: InputMaybe<Scalars['String']['input']>;
  imdbId?: InputMaybe<Scalars['String']['input']>;
  /** Identifier of the external system (e.g. Seerr) that originated this request. */
  requestedBy?: InputMaybe<Scalars['String']['input']>;
  /** Title used as a placeholder until indexing fills in the canonical name. */
  title: Scalars['String']['input'];
  tmdbId?: InputMaybe<Scalars['String']['input']>;
};

export type MutationRoot = {
  /**
   * Add a new media item to track and immediately queue it for indexing.
   * For shows, `seasons` is an optional list of season numbers to request.
   * If omitted, all non-special seasons are requested.
   */
  addItem: MediaItem;
  /** Mark the instance-wide first-run setup flow as completed. */
  completeInitialSetup: Scalars['Boolean']['output'];
  /** Delete a custom ranking profile by ID. Built-in profiles cannot be deleted. */
  deleteCustomProfile: Scalars['Boolean']['output'];
  /**
   * Delete a specific filesystem entry (a single downloaded version) by its ID.
   * Returns true if the entry was found and deleted. The DB trigger on
   * `filesystem_entries` recomputes the owning item's state automatically.
   */
  deleteFilesystemEntry: Scalars['Boolean']['output'];
  /** Create or reuse a non-requested media item, then index/scrape it so streams can be inspected. */
  discoverItem: MediaItem;
  /** Discover stream candidates without creating or mutating media items. */
  discoverStreams: Array<DiscoveredStream>;
  /**
   * Create or prepare the real target item only after the user picks a specific stream.
   *
   * For TV, the stream is matched against its parsed seasons (or the
   * caller-supplied `seasons` / `season_number`). A single-season pack links
   * to that season; a multi-season pack links to the **show** so the download
   * flow can fill every season it contains.
   */
  downloadDiscoveredStream: Scalars['String']['output'];
  downloadMediaItem: DownloadMediaItemMutationResponse;
  /**
   * Persist indexer data for a movie and advance it to the scraping stage.
   *
   * Called by the indexer plugin after it has resolved metadata (title,
   * content rating, release date, etc.) for a movie item request.
   */
  indexMovie: IndexMovieMutationResponse;
  /**
   * Persist indexer data for a show (including seasons and episodes) and
   * advance it to the scraping stage.
   *
   * Called by the indexer plugin after it has resolved metadata for a show
   * item request.
   */
  indexShow: IndexShowMutationResponse;
  /** Pause items. */
  pauseItems: Scalars['Int']['output'];
  /**
   * Re-acquire a usenet title whose release is broken (missing data) or was
   * never ingested. The item is "completed" only because it still has a
   * media filesystem entry, so reset alone bounces back to completed; we
   * delete that entry to genuinely un-complete it, then re-process. The
   * re-scrape's ingest availability probe skips any incomplete release, so a
   * complete one is picked.
   */
  regrabUsenetTitle: Scalars['String']['output'];
  /** Recompute stored library-profile matches for every existing media entry. */
  rematchFilesystemLibraryProfiles: Scalars['Int']['output'];
  /** Remove items by ID. */
  removeItems: Scalars['Int']['output'];
  /**
   * Bulk-request movies and shows in a single call.
   *
   * Items are deduplicated by their primary external ID (TMDB for movies,
   * TVDB for shows, with IMDB as fallback) so duplicate entries from a
   * single content-service payload are collapsed before processing.
   *
   * Returns the count of unique items processed and separate lists of newly
   * created vs updated item requests. Conflicts (already-requested items
   * with no change) are silently skipped.
   */
  requestItems: RequestItemsResult;
  /**
   * Request a movie to be tracked and indexed.
   *
   * Returns a structured response. If an identical request already exists
   * the mutation succeeds without error but the `statusText` is `CONFLICT`
   * and `item` is `null`.
   */
  requestMovie: RequestItemMutationResponse;
  /**
   * Request a show (and optionally specific seasons) to be tracked and indexed.
   *
   * If the show was already requested but new seasons are included the request
   * is updated and `statusText` is `OK`. If nothing has changed `statusText`
   * is `CONFLICT` and `item` is `null`.
   */
  requestShow: RequestItemMutationResponse;
  /**
   * Re-run the availability scan for one usenet file now and persist the
   * result. Returns the new status (`healthy` / `unhealthy` / `unknown`).
   */
  rescanUsenetHealth: Scalars['String']['output'];
  /** Reset items to Indexed state and clear failed_attempts. */
  resetItems: Scalars['Int']['output'];
  resetLibrary: Scalars['Int']['output'];
  /** Clear failed_attempts for items so they will be retried. */
  retryItems: Scalars['Int']['output'];
  /**
   * Save a custom profile. If `id` is provided the existing profile is
   * updated; otherwise a new one is created. Built-in profiles cannot be
   * modified through this mutation — use `setProfileEnabled` instead.
   */
  saveCustomProfile: Scalars['JSON']['output'];
  /**
   * Save a stream URL on a filesystem entry (media entry).
   *
   * Used by players and integrations to store the resolved playback URL
   * alongside the downloaded file record.
   */
  saveStreamUrl: SaveStreamUrlMutationResponse;
  /**
   * Trigger a scrape for an existing item by entering its
   * per-item state machine. For shows, optionally provide season_numbers
   * to mark additional seasons requested before processing.
   */
  scrapeItem: Scalars['String']['output'];
  scrapeMediaItem: ScrapeMediaItemMutationResponse;
  /**
   * Accepts Seerr's webhook payload (wrapped in a GraphQL envelope by the
   * JSON payload template `validate_webhook_settings` configures on Seerr)
   * and either acknowledges a `TEST_NOTIFICATION` ping or upserts the
   * requested movie/show directly into the library, mirroring what the
   * periodic content-service flow would have produced for the same
   * request — so users don't have to wait for the next poll cycle.
   */
  seerrHandleWebhook: Scalars['Boolean']['output'];
  /**
   * Enable or disable a ranking profile (built-in or custom) by name.
   * Enabled profiles are used for multi-version scraping and downloading.
   */
  setProfileEnabled: Scalars['Boolean']['output'];
  /** Unpause items (derives next state from current facts). */
  unpauseItems: Scalars['Int']['output'];
  /** Update all settings. Accepts a JSON object of key/value pairs. */
  updateAllSettings: Scalars['JSON']['output'];
  /**
   * Update settings for any profile (built-in or custom) by name.
   * For built-in profiles these are stored as overrides that get merged on
   * top of the Rust defaults at load time.
   */
  updateProfileSettings: Scalars['Boolean']['output'];
  /**
   * Update rank settings. Deserialises into [`RankSettings`] (applying
   * serde defaults for any missing fields), then re-serialises the
   * canonical form — ensuring the Rust schema is the source of truth.
   */
  updateRankSettings: Scalars['JSON']['output'];
  /**
   * The single write entry point for settings: the "general" section or any
   * plugin (by name). Persists the section's values, reconciles its side
   * effects (general → logging/downloader/VFS; plugin → revalidate), and
   * returns the updated section so the UI gets fresh enabled/valid state.
   */
  updateSettings: SettingsSection;
};


export type MutationRootAddItemArgs = {
  imdbId?: InputMaybe<Scalars['String']['input']>;
  itemType: MediaItemType;
  seasons?: InputMaybe<Array<Scalars['Int']['input']>>;
  title: Scalars['String']['input'];
  tmdbId?: InputMaybe<Scalars['String']['input']>;
  tvdbId?: InputMaybe<Scalars['String']['input']>;
};


export type MutationRootDeleteCustomProfileArgs = {
  id: Scalars['Int']['input'];
};


export type MutationRootDeleteFilesystemEntryArgs = {
  id: Scalars['Int']['input'];
};


export type MutationRootDiscoverItemArgs = {
  imdbId?: InputMaybe<Scalars['String']['input']>;
  itemType: MediaItemType;
  seasons?: InputMaybe<Array<Scalars['Int']['input']>>;
  title: Scalars['String']['input'];
  tmdbId?: InputMaybe<Scalars['String']['input']>;
  tvdbId?: InputMaybe<Scalars['String']['input']>;
};


export type MutationRootDiscoverStreamsArgs = {
  cachedOnly?: InputMaybe<Scalars['Boolean']['input']>;
  imdbId?: InputMaybe<Scalars['String']['input']>;
  itemType: MediaItemType;
  seasons?: InputMaybe<Array<Scalars['Int']['input']>>;
  title: Scalars['String']['input'];
  tmdbId?: InputMaybe<Scalars['String']['input']>;
  tvdbId?: InputMaybe<Scalars['String']['input']>;
};


export type MutationRootDownloadDiscoveredStreamArgs = {
  imdbId?: InputMaybe<Scalars['String']['input']>;
  infoHash: Scalars['String']['input'];
  itemType: MediaItemType;
  magnet: Scalars['String']['input'];
  parsedData?: InputMaybe<Scalars['JSON']['input']>;
  rank?: InputMaybe<Scalars['Int']['input']>;
  seasonNumber?: InputMaybe<Scalars['Int']['input']>;
  seasons?: InputMaybe<Array<Scalars['Int']['input']>>;
  title: Scalars['String']['input'];
  tmdbId?: InputMaybe<Scalars['String']['input']>;
  tvdbId?: InputMaybe<Scalars['String']['input']>;
};


export type MutationRootDownloadMediaItemArgs = {
  input: DownloadMediaItemMutationInput;
};


export type MutationRootIndexMovieArgs = {
  input: IndexMovieInput;
};


export type MutationRootIndexShowArgs = {
  input: IndexShowInput;
};


export type MutationRootPauseItemsArgs = {
  ids: Array<Scalars['Int']['input']>;
};


export type MutationRootRegrabUsenetTitleArgs = {
  mediaItemId: Scalars['Int']['input'];
};


export type MutationRootRemoveItemsArgs = {
  ids: Array<Scalars['Int']['input']>;
};


export type MutationRootRequestItemsArgs = {
  movies: Array<MovieRequestInput>;
  shows: Array<ShowRequestInput>;
};


export type MutationRootRequestMovieArgs = {
  input: MovieRequestInput;
};


export type MutationRootRequestShowArgs = {
  input: ShowRequestInput;
};


export type MutationRootRescanUsenetHealthArgs = {
  fileIndex: Scalars['Int']['input'];
  infoHash: Scalars['String']['input'];
};


export type MutationRootResetItemsArgs = {
  ids: Array<Scalars['Int']['input']>;
};


export type MutationRootRetryItemsArgs = {
  ids: Array<Scalars['Int']['input']>;
};


export type MutationRootSaveCustomProfileArgs = {
  enabled?: InputMaybe<Scalars['Boolean']['input']>;
  id?: InputMaybe<Scalars['Int']['input']>;
  name: Scalars['String']['input'];
  settings: Scalars['JSON']['input'];
};


export type MutationRootSaveStreamUrlArgs = {
  id: Scalars['Int']['input'];
  url: Scalars['String']['input'];
};


export type MutationRootScrapeItemArgs = {
  id: Scalars['Int']['input'];
  seasonNumbers?: InputMaybe<Array<Scalars['Int']['input']>>;
};


export type MutationRootScrapeMediaItemArgs = {
  input: ScrapeMediaItemMutationInput;
};


export type MutationRootSeerrHandleWebhookArgs = {
  payload: Scalars['JSON']['input'];
};


export type MutationRootSetProfileEnabledArgs = {
  enabled: Scalars['Boolean']['input'];
  name: Scalars['String']['input'];
};


export type MutationRootUnpauseItemsArgs = {
  ids: Array<Scalars['Int']['input']>;
};


export type MutationRootUpdateAllSettingsArgs = {
  settings: Scalars['JSON']['input'];
};


export type MutationRootUpdateProfileSettingsArgs = {
  name: Scalars['String']['input'];
  settings: Scalars['JSON']['input'];
};


export type MutationRootUpdateRankSettingsArgs = {
  settings: Scalars['JSON']['input'];
};


export type MutationRootUpdateSettingsArgs = {
  section: Scalars['String']['input'];
  values: Scalars['JSON']['input'];
};

/** Shared status enum returned by all structured mutation responses. */
export type MutationStatusText =
  | 'BAD_REQUEST'
  | 'CONFLICT'
  | 'CREATED'
  | 'INTERNAL_SERVER_ERROR'
  | 'NOT_FOUND'
  | 'OK';

/** Live health of one configured NNTP provider. */
export type NntpProviderHealth = {
  /** Open sockets currently servicing a fetch. */
  activeConnections: Scalars['Int']['output'];
  /** Consecutive "no such article" answers since the last success. */
  consecutiveNotFound: Scalars['Int']['output'];
  /**
   * Provider is being tried after healthier ones because it keeps
   * answering 430 for articles others can serve.
   */
  demoted: Scalars['Boolean']['output'];
  host: Scalars['String']['output'];
  /** Open sockets sitting idle in the pool. */
  idleConnections: Scalars['Int']['output'];
  isBackup: Scalars['Boolean']['output'];
  /** Connection ceiling (`max_connections`). */
  maxConnections: Scalars['Int']['output'];
  /** Open sockets right now (idle + in-flight). */
  openConnections: Scalars['Int']['output'];
  port: Scalars['Int']['output'];
  /** Lower = preferred. Primaries are tried before backups. */
  priority: Scalars['Int']['output'];
};

export type PersonCredit = {
  backdropPath?: Maybe<Scalars['String']['output']>;
  character?: Maybe<Scalars['String']['output']>;
  department?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  indexer: Scalars['String']['output'];
  job?: Maybe<Scalars['String']['output']>;
  mediaType: Scalars['String']['output'];
  originalTitle: Scalars['String']['output'];
  popularity?: Maybe<Scalars['Float']['output']>;
  posterPath?: Maybe<Scalars['String']['output']>;
  releaseDate?: Maybe<Scalars['String']['output']>;
  title: Scalars['String']['output'];
  voteAverage?: Maybe<Scalars['Float']['output']>;
  voteCount?: Maybe<Scalars['Int']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

export type PersonDetails = {
  alsoKnownAs: Array<Scalars['String']['output']>;
  biography?: Maybe<Scalars['String']['output']>;
  birthday?: Maybe<Scalars['String']['output']>;
  castCredits: Array<PersonCredit>;
  crewCredits: Array<PersonCredit>;
  deathday?: Maybe<Scalars['String']['output']>;
  externalIds: Array<ExternalId>;
  gender?: Maybe<Scalars['String']['output']>;
  homepage?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexer: Scalars['String']['output'];
  knownForDepartment?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  placeOfBirth?: Maybe<Scalars['String']['output']>;
  profilePath?: Maybe<Scalars['String']['output']>;
  /**
   * Surfaced beside `externalIds` so the UI can link to TMDB without
   * searching the list for it.
   */
  tmdbId?: Maybe<Scalars['Int']['output']>;
  tvdbUrl?: Maybe<Scalars['String']['output']>;
};

export type PlaybackMethod =
  | 'DIRECT_PLAY'
  | 'DIRECT_STREAM'
  | 'TRANSCODE'
  | 'UNKNOWN';

export type PlaybackState =
  | 'BUFFERING'
  | 'IDLE'
  | 'PAUSED'
  | 'PLAYING'
  | 'UNKNOWN';

export type ProductionCompany = {
  id: Scalars['Int']['output'];
  logoPath?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  originCountry?: Maybe<Scalars['String']['output']>;
};

export type QueryRoot = {
  /** Get active playback sessions from configured media-server plugins. */
  activePlaybackSessions: Array<ActivePlaybackSession>;
  /**
   * Get completed-item activity counts grouped by date (past year).
   * Returns a JSON object mapping ISO date strings (YYYY-MM-DD) to counts.
   */
  activity: Scalars['JSON']['output'];
  /** Get all stored settings as a JSON object. */
  allSettings: Scalars['JSON']['output'];
  anilistMappings: AnilistMappings;
  anilistRating: AnilistRating;
  /** Get upcoming unreleased items (calendar feed), with show title resolved in a single query. */
  calendar: Array<CalendarEntry>;
  companyDetails: PersonDetails;
  /** Return all ranking profiles (built-in + custom) with their enabled status. */
  customProfiles: Scalars['JSON']['output'];
  /** Get debrid account information for all configured stores. */
  debridAccountInfo: Array<DebridUserInfo>;
  defaultRankProfile: Scalars['JSON']['output'];
  episodeByTvdb?: Maybe<MediaItem>;
  episodes: Array<MediaItem>;
  /**
   * Return the number of media files expected for a media item:
   * - Movie / Episode → 1
   * - Season → total episode count
   * - Show → total processable episode count (continuing shows exclude the last season)
   */
  expectedFileCount: Scalars['Int']['output'];
  filesystemEntries: Array<FileSystemEntry>;
  /**
   * Return instance-level status flags used by frontend bootstrap flows.
   * Owns the setup-readiness rule so the UI never has to recompute it.
   */
  instanceStatus: InstanceStatus;
  items: ItemsPage;
  itemsByState: Array<MediaItem>;
  logs: Array<LogEntry>;
  /**
   * Return lookup key strings for an episode:
   * `["abs:{absolute_number}", "{season_number}:{episode_number}"]`.
   */
  lookupKeys: Array<Scalars['String']['output']>;
  mediaItemById?: Maybe<MediaItemUnion>;
  mediaItemByImdb?: Maybe<MediaItem>;
  mediaItemByTmdb?: Maybe<MediaItem>;
  mediaItemByTvdb?: Maybe<MediaItem>;
  mediaItemFull?: Maybe<MediaItemFull>;
  mediaItemFullByTmdb?: Maybe<MediaItemFull>;
  mediaItemFullByTvdb?: Maybe<MediaItemFull>;
  mediaItemStateByTmdb?: Maybe<MediaItemStateTree>;
  mediaItemStateByTvdb?: Maybe<MediaItemStateTree>;
  mediaItems: Array<MediaItemUnion>;
  /**
   * Everything the movie detail page renders, in one shape shared with
   * `showDetails`.
   */
  movieDetails: MediaDetails;
  movies: Array<MediaItem>;
  /**
   * Per-provider NNTP health (connections + demotion state). Empty when
   * usenet isn't configured.
   */
  nntpProviders: Array<NntpProviderHealth>;
  /** A cast/crew member or a company, both rendered through one shape. */
  personDetails: PersonDetails;
  /**
   * Return all quality profiles as an ordered array of
   * `{ id, label, description, settings }` objects.
   * The `settings` field reflects the *effective* settings (base preset merged
   * with any user overrides stored in the database), so the UI always shows
   * the values that will actually be used at runtime.
   */
  qualityProfiles: Scalars['JSON']['output'];
  rankSettingsSchema: Scalars['JSON']['output'];
  ratings: RatingsResponse;
  resolveExternalId: IdResolution;
  resolveTmdbToTvdb?: Maybe<Scalars['Int']['output']>;
  searchTmdb: TmdbPage;
  seasons: Array<MediaItem>;
  /**
   * Every configurable settings surface — the instance-wide "general"
   * section plus one section per plugin — each with the schema to render it
   * and its typed values. This is the single read the settings/setup UIs use.
   */
  settingsSections: Array<SettingsSection>;
  /**
   * Ordered setup sections that plugins are grouped under (by `PluginInfo.category`).
   * This is the single source of truth for setup-step grouping, labels, and order.
   */
  setupGroups: Array<SetupGroup>;
  /**
   * Everything the show detail page renders, in one shape shared with
   * `movieDetails`. `id` is a TVDB series id; `tmdbId` is only used to ask
   * Trakt for related titles when the page was reached from a TMDB id.
   */
  showDetails: MediaDetails;
  shows: Array<MediaItem>;
  stats: LibraryStats;
  tmdbCategory: TmdbPage;
  tmdbCollectionDetails: TmdbCollectionDetails;
  tmdbLogoAndCert: TmdbLogoAndCert;
  traktRecommendations: Array<TmdbListItem>;
  trendingAnilist: AnilistPage;
  trendingTmdb: TmdbPage;
  /** Cache + fetch metrics for the in-process usenet streaming engine. */
  usenetStreamingHealth: UsenetStreamingHealth;
  /**
   * Per-title usenet health from the background availability scanner.
   * Ordered worst-first (unhealthy, then most missing segments).
   */
  usenetTitleHealth: Array<UsenetTitleHealth>;
  /**
   * Title-health counts grouped by status. The `WHERE EXISTS` filter must
   * mirror `usenet_title_health` so the summary matches the listed rows.
   */
  usenetTitleHealthSummary: UsenetTitleHealthSummary;
  /**
   * Per-provider download traffic — lifetime totals + a daily series for
   * the usage-trend chart.
   */
  usenetTraffic: UsenetTraffic;
  /** List child entry names (file or directory names) directly under a VFS path. */
  vfsDirectoryEntryPaths: Array<Scalars['String']['output']>;
  /** Get the filesystem entry (media file record) for a VFS file path. */
  vfsEntry?: Maybe<FileSystemEntry>;
  /** Get filesystem stat info for a VFS path (file or directory). */
  vfsEntryStat: VfsEntryStat;
  viewer: Viewer;
  /** Count of movies and shows per release year. */
  yearReleases: Array<YearRelease>;
};


export type QueryRootAnilistMappingsArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootAnilistRatingArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootCalendarArgs = {
  limit?: InputMaybe<Scalars['Int']['input']>;
};


export type QueryRootCompanyDetailsArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootEpisodeByTvdbArgs = {
  episodeNumber: Scalars['Int']['input'];
  seasonNumber?: InputMaybe<Scalars['Int']['input']>;
  tvdbId: Scalars['String']['input'];
};


export type QueryRootEpisodesArgs = {
  seasonId: Scalars['Int']['input'];
};


export type QueryRootExpectedFileCountArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootFilesystemEntriesArgs = {
  mediaItemId: Scalars['Int']['input'];
};


export type QueryRootItemsArgs = {
  limit?: InputMaybe<Scalars['Int']['input']>;
  page?: InputMaybe<Scalars['Int']['input']>;
  search?: InputMaybe<Scalars['String']['input']>;
  sort?: InputMaybe<Scalars['String']['input']>;
  states?: InputMaybe<Array<MediaItemState>>;
  types?: InputMaybe<Array<MediaItemType>>;
};


export type QueryRootItemsByStateArgs = {
  itemType: MediaItemType;
  state: MediaItemState;
};


export type QueryRootLogsArgs = {
  level?: InputMaybe<Scalars['String']['input']>;
  limit?: InputMaybe<Scalars['Int']['input']>;
};


export type QueryRootLookupKeysArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootMediaItemByIdArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootMediaItemByImdbArgs = {
  imdbId: Scalars['String']['input'];
};


export type QueryRootMediaItemByTmdbArgs = {
  tmdbId: Scalars['String']['input'];
};


export type QueryRootMediaItemByTvdbArgs = {
  tvdbId: Scalars['String']['input'];
};


export type QueryRootMediaItemFullArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootMediaItemFullByTmdbArgs = {
  tmdbId: Scalars['String']['input'];
};


export type QueryRootMediaItemFullByTvdbArgs = {
  tvdbId: Scalars['String']['input'];
};


export type QueryRootMediaItemStateByTmdbArgs = {
  tmdbId: Scalars['String']['input'];
};


export type QueryRootMediaItemStateByTvdbArgs = {
  tvdbId: Scalars['String']['input'];
};


export type QueryRootMovieDetailsArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootPersonDetailsArgs = {
  id: Scalars['Int']['input'];
  indexer?: InputMaybe<Scalars['String']['input']>;
};


export type QueryRootRatingsArgs = {
  id: Scalars['String']['input'];
  indexer: Scalars['String']['input'];
  mediaType?: InputMaybe<Scalars['String']['input']>;
};


export type QueryRootResolveExternalIdArgs = {
  from: Scalars['String']['input'];
  id: Scalars['String']['input'];
  mediaType?: InputMaybe<Scalars['String']['input']>;
  to: Scalars['String']['input'];
};


export type QueryRootResolveTmdbToTvdbArgs = {
  tmdbId: Scalars['String']['input'];
};


export type QueryRootSearchTmdbArgs = {
  params?: InputMaybe<Scalars['JSON']['input']>;
  searchMode?: InputMaybe<Scalars['String']['input']>;
  type: Scalars['String']['input'];
};


export type QueryRootSeasonsArgs = {
  includeSpecials?: InputMaybe<Scalars['Boolean']['input']>;
  showId: Scalars['Int']['input'];
};


export type QueryRootShowDetailsArgs = {
  id: Scalars['Int']['input'];
  tmdbId?: InputMaybe<Scalars['String']['input']>;
};


export type QueryRootTmdbCategoryArgs = {
  category: Scalars['String']['input'];
  page?: InputMaybe<Scalars['Int']['input']>;
  type: Scalars['String']['input'];
};


export type QueryRootTmdbCollectionDetailsArgs = {
  id: Scalars['Int']['input'];
};


export type QueryRootTmdbLogoAndCertArgs = {
  id: Scalars['Int']['input'];
  type: Scalars['String']['input'];
};


export type QueryRootTraktRecommendationsArgs = {
  id: Scalars['String']['input'];
  idType: Scalars['String']['input'];
  mediaType: Scalars['String']['input'];
};


export type QueryRootTrendingAnilistArgs = {
  page?: InputMaybe<Scalars['Int']['input']>;
  perPage?: InputMaybe<Scalars['Int']['input']>;
};


export type QueryRootTrendingTmdbArgs = {
  page?: InputMaybe<Scalars['Int']['input']>;
  timeWindow: Scalars['String']['input'];
  type: Scalars['String']['input'];
};


export type QueryRootVfsDirectoryEntryPathsArgs = {
  path: Scalars['String']['input'];
};


export type QueryRootVfsEntryArgs = {
  path: Scalars['String']['input'];
};


export type QueryRootVfsEntryStatArgs = {
  path: Scalars['String']['input'];
};

export type RatingScore = {
  image?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  score: Scalars['String']['output'];
  url?: Maybe<Scalars['String']['output']>;
};

export type RatingsResponse = {
  anilistId?: Maybe<Scalars['Int']['output']>;
  imdbId?: Maybe<Scalars['String']['output']>;
  mediaType?: Maybe<Scalars['String']['output']>;
  scores: Array<RatingScore>;
  tmdbId?: Maybe<Scalars['Int']['output']>;
};

/** Structured response returned by `requestMovie` and `requestShow`. */
export type RequestItemMutationResponse = {
  errorCode?: Maybe<RequestItemMutationResponseErrorCode>;
  /** The item request that was created or updated; `null` on conflict. */
  item?: Maybe<ItemRequest>;
  message: Scalars['String']['output'];
  statusText: MutationStatusText;
  success: Scalars['Boolean']['output'];
};

export type RequestItemMutationResponseErrorCode =
  | 'CONFLICT'
  | 'UNEXPECTED_ERROR';

/** Returned by `requestItems` — a summary of a bulk upsert operation. */
export type RequestItemsResult = {
  /** Total number of unique items processed after deduplication. */
  count: Scalars['Int']['output'];
  /** Newly created item requests. */
  newItems: Array<ItemRequest>;
  /** Item requests that were updated (e.g. new seasons added to an existing show request). */
  updatedItems: Array<ItemRequest>;
};

export type RivenNotification = {
  count?: Maybe<Scalars['Int']['output']>;
  durationSeconds?: Maybe<Scalars['Float']['output']>;
  error?: Maybe<Scalars['String']['output']>;
  eventType: Scalars['String']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  id?: Maybe<Scalars['Int']['output']>;
  imdbId?: Maybe<Scalars['String']['output']>;
  itemType?: Maybe<Scalars['String']['output']>;
  newItems?: Maybe<Scalars['Int']['output']>;
  streamCount?: Maybe<Scalars['Int']['output']>;
  title?: Maybe<Scalars['String']['output']>;
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

/** Structured response returned by `saveStreamUrl`. */
export type SaveStreamUrlMutationResponse = {
  item?: Maybe<FileSystemEntry>;
  message: Scalars['String']['output'];
  statusText: MutationStatusText;
  success: Scalars['Boolean']['output'];
};

export type ScrapeMediaItemMutationErrorCode =
  | 'INCORRECT_STATE'
  | 'NO_NEW_STREAMS'
  | 'SCRAPE_ERROR';

export type ScrapeMediaItemMutationInput = {
  id: Scalars['Int']['input'];
  results: Scalars['JSON']['input'];
};

export type ScrapeMediaItemMutationResponse = {
  errorCode?: Maybe<ScrapeMediaItemMutationErrorCode>;
  item?: Maybe<MediaItemUnion>;
  message: Scalars['String']['output'];
  newStreamsCount?: Maybe<Scalars['Int']['output']>;
  statusText: MutationStatusText;
  success: Scalars['Boolean']['output'];
};

export type Season = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  /** All episodes in this season. */
  episodes: Array<Episode>;
  /** Expected number of episode files to download (equals total episodes). */
  expectedFileCount: Scalars['Int']['output'];
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  /** The parent show for this season. */
  show: Show;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  streams: Array<Stream>;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  /** Total number of episodes in this season. */
  totalEpisodes: Scalars['Int']['output'];
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};


export type SeasonStreamsArgs = {
  infoHashes?: InputMaybe<Array<Scalars['String']['input']>>;
};

/** Season with its episodes and their file info. */
export type SeasonFull = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  episodes: Array<EpisodeFull>;
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};

/** Lightweight season state used for live state subscriptions. */
export type SeasonState = {
  episodes: Array<EpisodeState>;
  expectedFileCount: Scalars['Int']['output'];
  id: Scalars['Int']['output'];
  isRequested: Scalars['Boolean']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  state: MediaItemState;
};

export type SeasonSummary = {
  airDate?: Maybe<Scalars['String']['output']>;
  /** Derived from the episode list rather than carried by TVDB. */
  episodeCount: Scalars['Int']['output'];
  id: Scalars['Int']['output'];
  image?: Maybe<Scalars['String']['output']>;
  name?: Maybe<Scalars['String']['output']>;
  number?: Maybe<Scalars['Int']['output']>;
  overview?: Maybe<Scalars['String']['output']>;
};

/**
 * One configurable settings surface — either the instance-wide "general"
 * settings or a single plugin. The frontend renders `schema` + `values`
 * generically; the plugin-only fields are null for the general section.
 */
export type SettingsSection = {
  /** Setup grouping key (plugins only; see `setupGroups`). */
  category?: Maybe<Scalars['String']['output']>;
  configured?: Maybe<Scalars['Boolean']['output']>;
  enabled?: Maybe<Scalars['Boolean']['output']>;
  id: Scalars['String']['output'];
  /** "general" | "plugin". */
  kind: Scalars['String']['output'];
  missingRequiredFields: Array<Scalars['String']['output']>;
  /** JSON array of SettingField descriptors for rendering the form. */
  schema: Scalars['JSON']['output'];
  title: Scalars['String']['output'];
  valid?: Maybe<Scalars['Boolean']['output']>;
  /** Typed values object keyed by field key. */
  values: Scalars['JSON']['output'];
  version?: Maybe<Scalars['String']['output']>;
};

/** An ordered setup section that plugins are grouped under (by `PluginInfo.category`). */
export type SetupGroup = {
  description: Scalars['String']['output'];
  id: Scalars['String']['output'];
  title: Scalars['String']['output'];
};

export type Show = {
  absoluteNumber?: Maybe<Scalars['Int']['output']>;
  activeStreamId?: Maybe<Scalars['Int']['output']>;
  airedAt?: Maybe<Scalars['NaiveDate']['output']>;
  airedAtUtc?: Maybe<Scalars['DateTime']['output']>;
  aliases?: Maybe<Scalars['JSON']['output']>;
  contentRating?: Maybe<ContentRating>;
  country?: Maybe<Scalars['String']['output']>;
  createdAt: Scalars['DateTime']['output'];
  episodeNumber?: Maybe<Scalars['Int']['output']>;
  /**
   * Total expected downloadable episode files.
   * For continuing shows the currently-airing season is excluded.
   */
  expectedFileCount: Scalars['Int']['output'];
  failedAttempts: Scalars['Int']['output'];
  fullTitle?: Maybe<Scalars['String']['output']>;
  genres?: Maybe<Scalars['JSON']['output']>;
  id: Scalars['Int']['output'];
  imdbId?: Maybe<Scalars['String']['output']>;
  indexedAt?: Maybe<Scalars['DateTime']['output']>;
  isAnime: Scalars['Boolean']['output'];
  isRequested: Scalars['Boolean']['output'];
  isSpecial?: Maybe<Scalars['Boolean']['output']>;
  itemRequestId?: Maybe<Scalars['Int']['output']>;
  itemType: MediaItemType;
  language?: Maybe<Scalars['String']['output']>;
  lastScrapeAttemptAt?: Maybe<Scalars['DateTime']['output']>;
  network?: Maybe<Scalars['String']['output']>;
  networkTimezone?: Maybe<Scalars['String']['output']>;
  parentId?: Maybe<Scalars['Int']['output']>;
  /**
   * Absolute artwork URL. The column holds a bare path for anything indexed
   * before the plugins normalised what they wrote, so it is resolved here
   * rather than in each client.
   */
  posterPath?: Maybe<Scalars['String']['output']>;
  rating?: Maybe<Scalars['Float']['output']>;
  runtime?: Maybe<Scalars['Int']['output']>;
  scrapedAt?: Maybe<Scalars['DateTime']['output']>;
  scrapedTimes: Scalars['Int']['output'];
  seasonNumber?: Maybe<Scalars['Int']['output']>;
  /** Seasons for this show. Excludes season 0 (specials) by default. */
  seasons: Array<Season>;
  showStatus?: Maybe<ShowStatus>;
  state: MediaItemState;
  streams: Array<Stream>;
  title: Scalars['String']['output'];
  tmdbId?: Maybe<Scalars['String']['output']>;
  tvdbId?: Maybe<Scalars['String']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
  year?: Maybe<Scalars['Int']['output']>;
};


export type ShowSeasonsArgs = {
  includeSpecials?: Scalars['Boolean']['input'];
};


export type ShowStreamsArgs = {
  infoHashes?: InputMaybe<Array<Scalars['String']['input']>>;
};

/** Input for requesting a show (and optionally specific seasons) to be tracked. */
export type ShowRequestInput = {
  /** External request ID for correlation with the originating content service. */
  externalRequestId?: InputMaybe<Scalars['String']['input']>;
  imdbId?: InputMaybe<Scalars['String']['input']>;
  /** Identifier of the external system (e.g. Seerr) that originated this request. */
  requestedBy?: InputMaybe<Scalars['String']['input']>;
  /** Season numbers to request. When omitted all non-special seasons are requested. */
  seasons?: InputMaybe<Array<Scalars['Int']['input']>>;
  /** Title used as a placeholder until indexing fills in the canonical name. */
  title: Scalars['String']['input'];
  tvdbId?: InputMaybe<Scalars['String']['input']>;
};

export type ShowStatus =
  | 'CONTINUING'
  | 'ENDED';

export type SpokenLanguage = {
  englishName?: Maybe<Scalars['String']['output']>;
  iso6391?: Maybe<Scalars['String']['output']>;
  name?: Maybe<Scalars['String']['output']>;
};

export type Stream = {
  createdAt: Scalars['DateTime']['output'];
  fileSizeBytes?: Maybe<Scalars['Int']['output']>;
  id: Scalars['Int']['output'];
  infoHash: Scalars['String']['output'];
  magnet: Scalars['String']['output'];
  parsedData?: Maybe<Scalars['JSON']['output']>;
  rank?: Maybe<Scalars['Int']['output']>;
  updatedAt?: Maybe<Scalars['DateTime']['output']>;
};

export type SubscriptionRoot = {
  /** Fires when a media item transitions to the completed state. */
  itemDownloaded: Scalars['Int']['output'];
  /** Fires when a media item transitions to the failed state (scrape or download error). */
  itemFailed: Scalars['Int']['output'];
  /** Fires when a media item transitions to the scraped state. */
  itemScraped: Scalars['Int']['output'];
  /** Fires when one or more media items are deleted. */
  itemsDeleted: Array<Scalars['Int']['output']>;
  /**
   * Stream of live log lines. Replaces the `/logs/stream` SSE endpoint.
   * Each item is a JSON string matching `{ timestamp, level, message, target }`.
   */
  logLines: Scalars['String']['output'];
  mediaItemStateUpdatesByTmdb?: Maybe<MediaItemStateTree>;
  mediaItemStateUpdatesByTvdb?: Maybe<MediaItemStateTree>;
  /** Fires when a new movie item request is created. */
  movieRequested: ItemRequest;
  /** Stream of all UI-notable Riven events. Replaces the `/notifications/stream` SSE endpoint. */
  notifications: RivenNotification;
  /** Fires when a show has been indexed (metadata and episode structure persisted). */
  showIndexed: Show;
  /** Fires when an existing show item request is updated (e.g. new seasons added). */
  showRequestUpdated: ItemRequest;
  /** Fires when a new show item request is created. */
  showRequested: ItemRequest;
};


export type SubscriptionRootMediaItemStateUpdatesByTmdbArgs = {
  tmdbId: Scalars['String']['input'];
};


export type SubscriptionRootMediaItemStateUpdatesByTvdbArgs = {
  tvdbId: Scalars['String']['input'];
};

export type SubtitleTrack = {
  codec?: Maybe<Scalars['String']['output']>;
  language?: Maybe<Scalars['String']['output']>;
};

export type TmdbCollectionDetails = {
  backdropPath?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  name: Scalars['String']['output'];
  overview?: Maybe<Scalars['String']['output']>;
  parts: Array<TmdbCollectionPart>;
  posterPath?: Maybe<Scalars['String']['output']>;
};

export type TmdbCollectionPart = {
  backdropPath?: Maybe<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  mediaType: Scalars['String']['output'];
  overview?: Maybe<Scalars['String']['output']>;
  posterPath?: Maybe<Scalars['String']['output']>;
  releaseDate?: Maybe<Scalars['String']['output']>;
  title: Scalars['String']['output'];
  year: Scalars['String']['output'];
};

export type TmdbListItem = {
  backdropPath?: Maybe<Scalars['String']['output']>;
  firstAirDate?: Maybe<Scalars['String']['output']>;
  genreIds: Array<Scalars['Int']['output']>;
  /**
   * Names for `genre_ids`, resolved against TMDB's own genre lists. Empty
   * when the lists could not be fetched — the ids are still there.
   */
  genres: Array<Scalars['String']['output']>;
  id: Scalars['Int']['output'];
  indexer: Scalars['String']['output'];
  mediaType: Scalars['String']['output'];
  originalLanguage?: Maybe<Scalars['String']['output']>;
  originalTitle?: Maybe<Scalars['String']['output']>;
  overview?: Maybe<Scalars['String']['output']>;
  popularity?: Maybe<Scalars['Float']['output']>;
  posterPath?: Maybe<Scalars['String']['output']>;
  releaseDate?: Maybe<Scalars['String']['output']>;
  title: Scalars['String']['output'];
  voteAverage?: Maybe<Scalars['Float']['output']>;
  voteCount?: Maybe<Scalars['Int']['output']>;
  year: Scalars['String']['output'];
};

export type TmdbLogoAndCert = {
  certification?: Maybe<Scalars['String']['output']>;
  logo?: Maybe<Scalars['String']['output']>;
};

export type TmdbPage = {
  page: Scalars['Int']['output'];
  results: Array<TmdbListItem>;
  totalPages: Scalars['Int']['output'];
  totalResults: Scalars['Int']['output'];
};

export type Trailer = {
  id?: Maybe<Scalars['String']['output']>;
  key?: Maybe<Scalars['String']['output']>;
  name: Scalars['String']['output'];
  site?: Maybe<Scalars['String']['output']>;
  url?: Maybe<Scalars['String']['output']>;
};

/** One provider's traffic on one day (for the usage-trend chart). */
export type UsenetDailyTraffic = {
  articlesDownloaded: Scalars['Int']['output'];
  bytesDownloaded: Scalars['Int']['output'];
  /** `YYYY-MM-DD`. */
  day: Scalars['String']['output'];
  host: Scalars['String']['output'];
};

/** Lifetime download total for one provider. */
export type UsenetProviderTraffic = {
  articlesDownloaded: Scalars['Int']['output'];
  bytesDownloaded: Scalars['Int']['output'];
  host: Scalars['String']['output'];
};

/** In-process streaming engine health (caches + NNTP fetch counters). */
export type UsenetStreamingHealth = {
  /** Usenet file handles the VFS is currently serving. */
  activeStreams: Scalars['Int']['output'];
  /** Total decoded bytes served from the wire (poll deltas for throughput). */
  bytesDecoded: Scalars['Int']['output'];
  /** Read-ahead hit rate, flattened: the headline figure for a dashboard. */
  cacheHitRate: Scalars['Float']['output'];
  /**
   * Largest budget first, so `read-ahead` — the cache that decides whether a
   * read reaches the network — comes before the ones behind it.
   */
  caches: Array<CacheHealth>;
  /** Segments known permanently missing on every provider. */
  deadSegments: Scalars['Int']['output'];
  /** Fetch success rate over all wire fetches, 0.0–1.0. */
  fetchSuccessRate: Scalars['Float']['output'];
  /** Fetches that exhausted retries or hit a missing article. */
  fetchesFailed: Scalars['Int']['output'];
  /** Successful wire fetches (cache misses that decoded cleanly). */
  fetchesOk: Scalars['Int']['output'];
  /** Segments being fetched + decoded right now. */
  inFlight: Scalars['Int']['output'];
};

/** Health of one usenet-backed title, enriched for display. */
export type UsenetTitleHealth = {
  /** Unix seconds of the last check (null if never checked). */
  checkedAt?: Maybe<Scalars['Int']['output']>;
  errorSegments: Scalars['Int']['output'];
  fileIndex: Scalars['Int']['output'];
  infoHash: Scalars['String']['output'];
  mediaItemId?: Maybe<Scalars['Int']['output']>;
  mediaType?: Maybe<Scalars['String']['output']>;
  /** Missing segments as a percentage of those sampled. */
  missingPct: Scalars['Float']['output'];
  missingSegments: Scalars['Int']['output'];
  /** Unix seconds of the next scheduled auto-repair (null if none pending). */
  nextRepairAt?: Maybe<Scalars['Int']['output']>;
  posterPath?: Maybe<Scalars['String']['output']>;
  /** Auto-repair attempts made so far (0 if none / not applicable). */
  repairAttempts: Scalars['Int']['output'];
  sampledSegments: Scalars['Int']['output'];
  /** `healthy` | `unhealthy` | `unknown` | `checking`. */
  status: Scalars['String']['output'];
  /** `S05E03 · Sand Job` for episodes, year for movies. */
  subtitle?: Maybe<Scalars['String']['output']>;
  /** Show/movie title for display. */
  title?: Maybe<Scalars['String']['output']>;
  totalSegments: Scalars['Int']['output'];
};

/** Title-health counts grouped by status, for the dashboard summary line. */
export type UsenetTitleHealthSummary = {
  healthy: Scalars['Int']['output'];
  notIngested: Scalars['Int']['output'];
  total: Scalars['Int']['output'];
  unhealthy: Scalars['Int']['output'];
  /** Catch-all for any other status (e.g. `checking`/`unknown`). */
  unknown: Scalars['Int']['output'];
};

/** Download-traffic accounting across all usenet providers. */
export type UsenetTraffic = {
  /** Per-provider per-day series over the last two weeks (oldest first). */
  daily: Array<UsenetDailyTraffic>;
  /** Lifetime totals per provider, busiest first. */
  providers: Array<UsenetProviderTraffic>;
  totalArticlesDownloaded: Scalars['Int']['output'];
  totalBytesDownloaded: Scalars['Int']['output'];
};

/**
 * Riven's privilege ladder. Ordered: every check is `role >= minimum`.
 *
 * Exposed to GraphQL as an enum rather than a string so the set of roles is
 * part of the schema — a client cannot invent a fourth one, and adding one here
 * is a visible schema change rather than a new magic string.
 */
export type UserRole =
  | 'ADMIN'
  | 'MANAGER'
  | 'USER';

/** Filesystem stat metadata for a VFS path. */
export type VfsEntryStat = {
  atime: Scalars['DateTime']['output'];
  ctime: Scalars['DateTime']['output'];
  gid: Scalars['Int']['output'];
  /** Unix file mode (e.g. 0o040755 for directory, 0o100644 for regular file). */
  mode: Scalars['Int']['output'];
  mtime: Scalars['DateTime']['output'];
  /** Number of hard links. */
  nlink: Scalars['Int']['output'];
  /** File size in bytes (0 for directories). */
  size: Scalars['Int']['output'];
  uid: Scalars['Int']['output'];
};

export type VideoMetadata = {
  bitDepth?: Maybe<Scalars['Int']['output']>;
  codec?: Maybe<Scalars['String']['output']>;
  frameRate?: Maybe<Scalars['Float']['output']>;
  hdrType?: Maybe<Scalars['String']['output']>;
  resolutionHeight?: Maybe<Scalars['Int']['output']>;
  resolutionWidth?: Maybe<Scalars['Int']['output']>;
};

/**
 * What the caller may do, as the backend understands it.
 *
 * A list rather than a field per capability: adding one becomes a single edit
 * here, and clients receive it without a change on their side. The frontend
 * tests membership; it never derives anything from `role`.
 */
export type Viewer = {
  capabilities: Array<Capability>;
  role: UserRole;
};

export type YearRelease = {
  count: Scalars['Int']['output'];
  year: Scalars['Int']['output'];
};
