# 🎬 Riven-rs API Documentation

> Complete, production-ready API reference for Riven-rs — a powerful Rust-based media management and streaming orchestration platform.

**Version:** 1.0.0 | **Last Updated:** June 2026 | **Status:** Stable

---

## 📋 Table of Contents

- [Quick Start](#-quick-start)
- [Authentication & Security](#-authentication--security)
- [API Fundamentals](#-api-fundamentals)
- [GraphQL Queries](#-graphql-queries)
- [GraphQL Mutations](#-graphql-mutations)
- [GraphQL Subscriptions](#-graphql-subscriptions)
- [REST Endpoints](#-rest-endpoints)
- [Webhooks](#-webhooks)
- [Data Types & Objects](#-data-types--objects)
- [Error Handling](#-error-handling)
- [Best Practices](#-best-practices)
- [Code Examples](#-code-examples)

---

## 🚀 Quick Start

### Setup
```bash
# Clone and build
cd riven-rs
cp .env.example .env

# Configure required environment variables
export RIVEN_SETTING__DATABASE_URL="postgresql://user:pass@localhost/riven"
export RIVEN_SETTING__REDIS_URL="redis://localhost:6379"
export RIVEN_SETTING__GQL_PORT="8080"
export RIVEN_SETTING__API_KEY="your-secret-api-key"

# Start with Docker Compose
docker compose up --build
```

### First API Call
```bash
# Test GraphQL endpoint
curl -X POST http://localhost:8080/graphql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-secret-api-key" \
  -d '{
    "query": "{ instanceStatus { status uptime } }"
  }'
```

---

## 🔐 Authentication & Security

### API Key Authentication

**Required for:** Most mutations and protected queries

**Configuration:**
```bash
RIVEN_SETTING__API_KEY="your-secret-key-here"
```

**Usage:**
```bash
curl -H "Authorization: Bearer your-secret-key-here" \
  http://localhost:8080/graphql
```

### Frontend Auth Headers (RBAC)

For role-based access control, use the signing secret:

```bash
RIVEN_SETTING__FRONTEND_AUTH_SIGNING_SECRET="your-secret"
```

**Header Format:**
```
X-Riven-Auth: <signed-token>
```

### CORS Configuration

```bash
RIVEN_SETTING__CORS_ALLOWED_ORIGINS="https://example.com,https://app.example.com"
```

### Security Best Practices

✅ **DO:**
- Store API keys securely (environment variables, secrets management)
- Use HTTPS in production
- Implement rate limiting on client side
- Validate all input data
- Use specific scopes when possible

❌ **DON'T:**
- Commit API keys to version control
- Expose credentials in client-side code
- Share API keys across services
- Use weak/default API keys

---

## 🔌 API Fundamentals

### Base URL
```
http://localhost:8080
```

### GraphQL Endpoint
- **POST** `/graphql` — Query, mutations
- **GET** `/graphql` — Introspection queries
- **WebSocket** `/graphql` — Subscriptions

### Response Format

**Successful Query:**
```json
{
  "data": {
    "mediaItems": [
      {
        "id": "123",
        "title": "Breaking Bad",
        "state": "AVAILABLE",
        "itemType": "SHOW"
      }
    ]
  }
}
```

**Query with Errors:**
```json
{
  "data": null,
  "errors": [
    {
      "message": "Authentication required",
      "extensions": {
        "code": "UNAUTHENTICATED"
      }
    }
  ]
}
```

### Rate Limiting

Currently not enforced, but implement client-side backoff for robustness:

```javascript
const backoffMultiplier = 1.5;
let delay = 100; // ms

async function apiCallWithBackoff(query) {
  while (true) {
    try {
      const response = await fetch('/graphql', {
        method: 'POST',
        body: JSON.stringify({ query })
      });
      if (response.ok) return response.json();
      if (response.status === 429) {
        await new Promise(r => setTimeout(r, delay));
        delay *= backoffMultiplier;
        continue;
      }
      throw new Error(`HTTP ${response.status}`);
    } catch (error) {
      console.error('Request failed:', error);
      throw error;
    }
  }
}
```

---

## 📊 GraphQL Queries

### Media Queries

#### `mediaItemById(id: i64!): MediaItemUnion`

Retrieve a single media item by internal database ID.

**Description:**
- Returns the most detailed version available (Movie, Show, Season, or Episode)
- Includes all metadata and computed fields
- Filters out unindexed items

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | i64 | ✓ | Internal Riven database ID |

**Example Request:**
```graphql
query GetMovie {
  mediaItemById(id: 42) {
    ... on Movie {
      id
      title
      releaseDate
      imdbId
      tmdbId
      posterPath
      overview
      runtime
    }
  }
}
```

**Example Response:**
```json
{
  "data": {
    "mediaItemById": {
      "id": "42",
      "title": "Oppenheimer",
      "releaseDate": "2023-07-21",
      "imdbId": "tt15398776",
      "tmdbId": "912649",
      "posterPath": "/imagePath.jpg",
      "overview": "The story of American scientist J. Robert Oppenheimer...",
      "runtime": 180
    }
  }
}
```

**Errors:**
- `404 Not Found`: Item ID doesn't exist
- `400 Bad Request`: Invalid ID format

**Performance Notes:**
- O(1) lookup, optimal for ID-based retrieval
- Cache friendly: ID never changes
- Use this for item detail pages

---

#### `mediaItems: [MediaItemUnion]!`

Get the 25 most recently created media items.

**Description:**
- Returns items ordered by creation date (newest first)
- Useful for "Recent Activity" feeds
- Does not include deleted items
- Limited to 25 items (configurable in future)

**Example Request:**
```graphql
query RecentItems {
  mediaItems {
    id
    title
    itemType
    createdAt
    state
  }
}
```

**Example Response:**
```json
{
  "data": {
    "mediaItems": [
      {
        "id": "100",
        "title": "Oppenheimer",
        "itemType": "MOVIE",
        "createdAt": "2024-06-01T10:30:00Z",
        "state": "AVAILABLE"
      },
      {
        "id": "99",
        "title": "Breaking Bad",
        "itemType": "SHOW",
        "createdAt": "2024-05-31T15:20:00Z",
        "state": "DOWNLOADING"
      }
    ]
  }
}
```

**Use Cases:**
- Dashboard recent activity widget
- Feed of newly added content
- Quick preview of library growth

---

#### `mediaItemByImdb(imdbId: String!): MediaItem`

Lookup media item by IMDb identifier.

**Description:**
- Performs indexed lookup on IMDb ID
- IMDb IDs are globally unique (e.g., "tt0944947")
- Essential for cross-service integration

**Parameters:**

| Parameter | Type | Required | Format | Example |
|-----------|------|----------|--------|---------|
| `imdbId` | String | ✓ | tt[0-9]+ | tt0944947 |

**Example Request:**
```graphql
query GetByImdb {
  mediaItemByImdb(imdbId: "tt0944947") {
    id
    title
    itemType
    tmdbId
    tvdbId
  }
}
```

**Example Response:**
```json
{
  "data": {
    "mediaItemByImdb": {
      "id": "1",
      "title": "Game of Thrones",
      "itemType": "SHOW",
      "tmdbId": "1399",
      "tvdbId": "121361"
    }
  }
}
```

**Integration Examples:**

*Seerr Integration:*
```javascript
// When user requests via Seerr, lookup by IMDb ID
async function handleSeerrRequest(imdbId) {
  const query = `
    query { mediaItemByImdb(imdbId: "${imdbId}") { id } }
  `;
  const response = await fetch('/graphql', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${API_KEY}` },
    body: JSON.stringify({ query })
  });
  return response.json();
}
```

---

#### `mediaItemByTmdb(tmdbId: String!): MediaItem`

Lookup media item by The Movie Database (TMDB) ID.

**Description:**
- Lookup against TMDB (themoviedb.org) identifiers
- TMDB IDs are numeric but supplied as strings
- Alternative to IMDb for countries with limited IMDb coverage
- Direct TMDB API integration for metadata

**Parameters:**

| Parameter | Type | Required | Format | Example |
|-----------|------|----------|--------|---------|
| `tmdbId` | String | ✓ | [0-9]+ | 1399 |

**Example Request:**
```graphql
query GetByTmdb {
  mediaItemByTmdb(tmdbId: "1399") {
    id
    title
    imdbId
    tvdbId
    seasons { seasonNumber }
  }
}
```

**Use Cases:**
- Trakt integration (Trakt uses TMDB IDs)
- AniList mappings
- Dashboard recommendations

---

#### `mediaItemByTvdb(tvdbId: String!): MediaItem`

Lookup media item by TheTVDB (TVDB) identifier.

**Description:**
- TVDB specializes in TV show metadata
- Most accurate for series/episode information
- Preferred for season/episode lookups
- High-quality artwork and episode data

**Parameters:**

| Parameter | Type | Required | Format | Example |
|-----------|------|----------|--------|---------|
| `tvdbId` | String | ✓ | [0-9]+ | 81189 |

**Example Request:**
```graphql
query GetByTvdb {
  mediaItemByTvdb(tvdbId: "81189") {
    id
    title
    itemType
    episodes { episodeNumber title airDate }
  }
}
```

**Advantages:**
- Most detailed episode information
- Accurate air dates globally
- Comprehensive season structure

---

#### `mediaItemFull(id: i64!): MediaItemFull`

Get comprehensive item details including all metadata, filesystem entries, and season/episode tree.

**Description:**
- Most complete data fetch for a single item
- Includes all nested relationships
- Use for detail pages and full exports
- May be slower for large shows (many seasons/episodes)

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `id` | i64 | ✓ | Internal item ID |

**Example Request:**
```graphql
query GetFullShow {
  mediaItemFull(id: 1) {
    item {
      id
      title
      itemType
      state
      overview
    }
    filesystemEntry {
      id
      filePath
      fileSize
    }
    filesystemEntries {
      id
      filePath
      fileName
      fileSize
      createdAt
    }
    seasons {
      item {
        id
        seasonNumber
        title
      }
      episodes {
        item {
          id
          episodeNumber
          title
          airDate
        }
        filesystemEntry {
          filePath
          fileSize
        }
      }
    }
  }
}
```

**Example Response:**
```json
{
  "data": {
    "mediaItemFull": {
      "item": {
        "id": "2",
        "title": "Breaking Bad",
        "itemType": "SHOW",
        "state": "AVAILABLE",
        "overview": "A high school chemistry teacher..."
      },
      "filesystemEntry": {
        "id": "500",
        "filePath": "/movies/BreakingBad/show.info",
        "fileSize": 1024
      },
      "filesystemEntries": [
        {
          "id": "501",
          "filePath": "/movies/BreakingBad/S01E01.mkv",
          "fileName": "S01E01.mkv",
          "fileSize": 2147483648,
          "createdAt": "2024-01-15T08:30:00Z"
        }
      ],
      "seasons": [
        {
          "item": {
            "id": "10",
            "seasonNumber": 1,
            "title": "Season 1"
          },
          "episodes": [
            {
              "item": {
                "id": "11",
                "episodeNumber": 1,
                "title": "Pilot",
                "airDate": "2008-01-20"
              },
              "filesystemEntry": {
                "filePath": "/movies/BreakingBad/S01E01.mkv",
                "fileSize": 2147483648
              }
            }
          ]
        }
      ]
    }
  }
}
```

**Performance Characteristics:**
- Time: O(seasons + episodes) — Linear in show complexity
- For 5-season show: ~10ms
- For 10-season show: ~20ms
- Recommend pagination for 15+ season shows

**Use Cases:**
- Item detail pages (full view)
- Export data for external systems
- Admin dashboard comprehensive view

---

#### `mediaItemStateByTmdb(tmdbId: String!): MediaItemStateTree`

Get item state information (download/availability status) including all seasons and episodes by TMDB ID.

**Description:**
- Optimized query for state information only
- Lighter weight than `mediaItemFullByTmdb`
- Returns hierarchical state tree
- Ideal for status dashboards and progress bars

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `tmdbId` | String | ✓ | TMDB identifier |

**Example Request:**
```graphql
query ShowProgress {
  mediaItemStateByTmdb(tmdbId: "1399") {
    id
    state
    expectedFileCount
    seasons {
      id
      seasonNumber
      state
      isRequested
      expectedFileCount
      episodes {
        id
        episodeNumber
        state
      }
    }
  }
}
```

**State Values:**
| State | Meaning | Color |
|-------|---------|-------|
| UNRELEASED | Not yet aired | 🔵 Blue |
| RELEASED | Aired but not requested | ⚪ Gray |
| DOWNLOADING | Download in progress | 🟡 Yellow |
| AVAILABLE | Downloaded and ready | 🟢 Green |
| UNKNOWN | State cannot be determined | ⚫ Black |
| ONGOING | Still airing | 🟠 Orange |

**Example Response:**
```json
{
  "data": {
    "mediaItemStateByTmdb": {
      "id": "2",
      "state": "AVAILABLE",
      "expectedFileCount": 62,
      "seasons": [
        {
          "id": "10",
          "seasonNumber": 1,
          "state": "AVAILABLE",
          "isRequested": true,
          "expectedFileCount": 7,
          "episodes": [
            {
              "id": "11",
              "episodeNumber": 1,
              "state": "AVAILABLE"
            },
            {
              "id": "12",
              "episodeNumber": 2,
              "state": "AVAILABLE"
            }
          ]
        }
      ]
    }
  }
}
```

**Use Cases:**
- Progress bars (completed / total files)
- Status dashboard
- Episode checklist UI
- Monitoring download jobs

---

#### `movies: [MediaItem]!`

Get all movies in the library.

**Description:**
- Returns all indexed movies
- Excludes TV shows, seasons, and episodes
- Results ordered by creation date
- No pagination (load all)

**Example Request:**
```graphql
query AllMovies {
  movies {
    id
    title
    releaseDate
    state
    imdbId
    tmdbId
  }
}
```

**Use Cases:**
- Movie library view
- Statistics and analytics
- Export all movies

---

#### `shows: [MediaItem]!`

Get all TV shows in the library.

**Description:**
- Returns only show-level items
- Excludes seasons and episodes
- Ordered by creation date (newest first)
- No pagination (load all)

**Example Request:**
```graphql
query AllShows {
  shows {
    id
    title
    state
    networkName
    status
  }
}
```

---

#### `seasons(showId: i64!, includeSpecials: Boolean): [MediaItem]!`

Get all seasons for a show.

**Description:**
- Retrieves seasons for a specific show
- Can exclude special episodes (specials, bonus content)
- Ordered by season number
- Includes state information per season

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `showId` | i64 | ✓ | — | Parent show ID |
| `includeSpecials` | Boolean | ✗ | true | Include special seasons (S00) |

**Example Request — All Seasons:**
```graphql
query GetSeasons {
  seasons(showId: 2) {
    id
    seasonNumber
    title
    state
    episodeCount
  }
}
```

**Example Request — Exclude Specials:**
```graphql
query MainSeasons {
  seasons(showId: 2, includeSpecials: false) {
    seasonNumber
    title
  }
}
```

**Example Response:**
```json
{
  "data": {
    "seasons": [
      {
        "id": "10",
        "seasonNumber": 1,
        "title": "Season 1",
        "state": "AVAILABLE",
        "episodeCount": 7
      },
      {
        "id": "20",
        "seasonNumber": 2,
        "title": "Season 2",
        "state": "AVAILABLE",
        "episodeCount": 13
      }
    ]
  }
}
```

**Special Season Note:**
- Season 0 = Specials, behind-the-scenes, bonus content
- Many shows use S00 for specials
- Usually want to exclude from main library view

---

#### `episodes(seasonId: i64!): [MediaItem]!`

Get all episodes for a season.

**Description:**
- Retrieves episodes for a specific season
- Ordered by episode number
- Includes air date and state for each episode
- Links to parent season

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `seasonId` | i64 | ✓ | Parent season ID |

**Example Request:**
```graphql
query GetEpisodes {
  episodes(seasonId: 10) {
    id
    episodeNumber
    title
    airDate
    state
    overview
  }
}
```

**Example Response:**
```json
{
  "data": {
    "episodes": [
      {
        "id": "11",
        "episodeNumber": 1,
        "title": "Pilot",
        "airDate": "2008-01-20",
        "state": "AVAILABLE",
        "overview": "A high school chemistry teacher..."
      },
      {
        "id": "12",
        "episodeNumber": 2,
        "title": "Cat's in the Bag...",
        "airDate": "2008-01-27",
        "state": "AVAILABLE",
        "overview": "Walter and Jesse must dispose of evidence..."
      }
    ]
  }
}
```

---

#### `items(page: i64, limit: i64, sort: String, types: [MediaItemType], search: String, states: [MediaItemState]): ItemsPage!`

Advanced paginated search and filtering across all media items.

**Description:**
- Most flexible query for browsing library
- Full-text search on titles
- Multi-filter support (type, state)
- Sortable by multiple fields
- Pagination support with page/limit

**Parameters:**

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `page` | i64 | ✗ | 1 | Page number (1-indexed) |
| `limit` | i64 | ✗ | 20 | Items per page (1-100) |
| `sort` | String | ✗ | created_at | Sort field (created_at, updated_at, title) |
| `types` | [MediaItemType] | ✗ | All | Filter by item type |
| `search` | String | ✗ | None | Search term (fuzzy match) |
| `states` | [MediaItemState] | ✗ | All | Filter by state |

**Example Request — Search with Filters:**
```graphql
query SearchLibrary {
  items(
    page: 1
    limit: 20
    search: "breaking"
    types: [SHOW]
    states: [AVAILABLE, DOWNLOADING]
    sort: "title"
  ) {
    items {
      id
      title
      itemType
      state
      createdAt
    }
    page
    limit
    totalItems
    totalPages
  }
}
```

**Example Response:**
```json
{
  "data": {
    "items": {
      "items": [
        {
          "id": "2",
          "title": "Breaking Bad",
          "itemType": "SHOW",
          "state": "AVAILABLE",
          "createdAt": "2024-05-31T15:20:00Z"
        }
      ],
      "page": 1,
      "limit": 20,
      "totalItems": 1,
      "totalPages": 1
    }
  }
}
```

**Sorting Options:**
```
created_at     → Newest first
updated_at     → Recently modified first
title          → Alphabetically
state          → By download status
```

**Pagination Example:**
```graphql
# Page 1 (items 1-20)
query { items(page: 1, limit: 20) { items { id } page totalPages } }

# Page 2 (items 21-40)
query { items(page: 2, limit: 20) { items { id } page totalPages } }
```

---

#### `filesystemEntries(mediaItemId: i64!): [FileSystemEntry]!`

Get all physical file entries associated with a media item.

**Description:**
- Returns all files on disk for an item
- Includes metadata files, subtitles, and video
- Multiple entries possible (multiple versions, quality levels)
- Includes file size and creation date

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mediaItemId` | i64 | ✓ | Media item ID |

**Example Request:**
```graphql
query GetFiles {
  filesystemEntries(mediaItemId: 11) {
    id
    filePath
    fileName
    fileSize
    entryType
    createdAt
    mediaMetadata {
      codec
      resolution
      duration
    }
  }
}
```

**Example Response:**
```json
{
  "data": {
    "filesystemEntries": [
      {
        "id": "501",
        "filePath": "/movies/BreakingBad/S01E01.mkv",
        "fileName": "S01E01.mkv",
        "fileSize": 2147483648,
        "entryType": "MEDIA",
        "createdAt": "2024-01-15T08:30:00Z",
        "mediaMetadata": {
          "codec": "h264",
          "resolution": "1920x1080",
          "duration": 2700
        }
      }
    ]
  }
}
```

---

### Settings Queries

#### `rankSettings: Json`

Get current quality/ranking profile settings.

**Description:**
- Returns active rank settings JSON
- Controls how streams are scored and ranked
- Complex nested object with weights and thresholds

**Example Request:**
```graphql
query {
  rankSettings {
    /* JSON object with ranking criteria */
  }
}
```

**Settings Structure:**
```json
{
  "profile_name": "Default",
  "min_resolution": "720p",
  "preferred_resolution": "1080p",
  "max_resolution": "4k",
  "audio_weights": {
    "english": 100,
    "spanish": 50
  },
  "codec_preferences": {
    "h264": 80,
    "h265": 100
  }
}
```

---

#### `qualityProfiles: Json`

Get all available quality profiles (built-in and custom).

**Description:**
- Lists all pre-configured profiles
- Built-in: HD, 4K, Low-Bandwidth, etc.
- Custom: User-created profiles
- Returns full profile definitions

**Example Request:**
```graphql
query {
  qualityProfiles {
    /* Profile definitions */
  }
}
```

---

#### `allSettings: Json`

Get complete system configuration.

**Description:**
- All settings in one request
- Includes rank, quality, plugin, and general settings
- Full configuration export/import capable
- Large response (may be slow)

**Use Cases:**
- Settings backup
- Configuration export
- Admin configuration review
- Settings migration

---

#### `instanceStatus: InstanceStatus!`

Get instance health and status information.

**Description:**
- Quick health check endpoint
- Returns version, uptime, status
- No authentication required
- Great for monitoring

**Example Request:**
```graphql
query {
  instanceStatus {
    status
    version
    uptime
    startTime
    dbConnection
    redisConnection
  }
}
```

**Example Response:**
```json
{
  "data": {
    "instanceStatus": {
      "status": "healthy",
      "version": "1.0.0",
      "uptime": 86400,
      "startTime": "2024-06-01T00:00:00Z",
      "dbConnection": "connected",
      "redisConnection": "connected"
    }
  }
}
```

**Use Cases:**
- Monitoring dashboards
- Health checks
- Service status pages
- Uptime tracking

---

#### `pluginInfo: [PluginInfo]!`

Get information about all loaded plugins.

**Description:**
- Lists every plugin loaded by Riven
- Includes name, version, status
- Shows enabled/disabled state
- Plugin list for admin UI

**Example Request:**
```graphql
query {
  pluginInfo {
    id
    name
    version
    description
    enabled
    author
  }
}
```

**Example Response:**
```json
{
  "data": {
    "pluginInfo": [
      {
        "id": "tmdb",
        "name": "TMDB Metadata Provider",
        "version": "1.0.0",
        "description": "Fetch metadata from The Movie Database",
        "enabled": true,
        "author": "Riven Team"
      },
      {
        "id": "stremthru",
        "name": "Stremthru Stream Provider",
        "version": "2.1.0",
        "description": "Real-Debrid and other debrid provider",
        "enabled": true,
        "author": "Community"
      }
    ]
  }
}
```

---

#### `pluginSettings(plugin: String!): Json`

Get configuration schema and current settings for a specific plugin.

**Description:**
- Returns plugin's settings template and values
- Includes validation rules
- Used for settings UI generation
- Plugin configuration endpoint

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `plugin` | String | ✓ | Plugin ID (e.g., "tmdb") |

**Example Request:**
```graphql
query {
  pluginSettings(plugin: "tmdb") {
    settings {
      api_key
      cache_ttl
    }
    schema {
      properties { /* validation schema */ }
    }
  }
}
```

---

### External Services Queries

#### `searchTmdb(query: String!, type: String): Json`

Search The Movie Database for movies or TV shows.

**Description:**
- Full-text search across TMDB
- Returns matching movies, shows, and people
- Paginated results with ratings
- Essential for adding new content

**Parameters:**

| Parameter | Type | Required | Values |
|-----------|------|----------|--------|
| `query` | String | ✓ | Search term |
| `type` | String | ✗ | "movie", "tv", "person" |

**Example Request:**
```graphql
query {
  searchTmdb(query: "breaking bad", type: "tv") {
    results {
      id
      name
      firstAirDate
      voteAverage
      posterPath
    }
  }
}
```

**Use Cases:**
- Search UI for adding content
- Content discovery
- User content selection

---

#### `trendingTmdb(type: String, window: String): Json`

Get trending content from TMDB.

**Description:**
- Daily and weekly trending movies/shows
- Popular and critically acclaimed content
- Great for discovery recommendations
- Updated regularly by TMDB

**Parameters:**

| Parameter | Type | Required | Values |
|-----------|------|----------|--------|
| `type` | String | ✗ | "movie", "tv" |
| `window` | String | ✗ | "day", "week" |

**Example Request:**
```graphql
query {
  trendingTmdb(type: "tv", window: "week") {
    results {
      id
      name
      popularity
      voteAverage
    }
  }
}
```

---

#### `trendingAnilist: Json`

Get trending anime from AniList database.

**Description:**
- Anime-specific trending content
- Separate from TMDB (anime focus)
- Used for anime library management
- Manga/anime curated selection

**Example Request:**
```graphql
query {
  trendingAnilist {
    results { id title score }
  }
}
```

---

### Usenet Queries

#### `nntpProviders: [NntpProviderHealth]!`

Get health status of configured NNTP (Usenet) providers.

**Description:**
- Shows each provider's connection status
- Articles available, retention, limits
- Connection failures and last check time
- Critical for usenet-based downloading

**Example Request:**
```graphql
query {
  nntpProviders {
    name
    status
    articlesAvailable
    retention
    connectionStatus
    lastCheck
  }
}
```

**Status Values:**
- `CONNECTED` — Working normally
- `CONNECTING` — Attempting connection
- `DISCONNECTED` — Not connected
- `ERROR` — Error state (quota, credentials, etc.)

**Use Cases:**
- Provider monitoring dashboard
- Download troubleshooting
- Provider health alerts
- Admin status page

---

#### `usenetTitleHealth: [UsenetTitleHealth]!`

Check usenet availability for specific titles.

**Description:**
- Checks if titles are available on usenet providers
- Shows retention information
- Helps predict download viability
- Pre-download availability check

**Example Request:**
```graphql
query {
  usenetTitleHealth {
    title
    available
    retention
    lastChecked
  }
}
```

---

#### `usenetTraffic: UsenetTraffic!`

Get current usenet bandwidth and connection statistics.

**Description:**
- Real-time download statistics
- Bytes downloaded in current period
- Active connections count
- Bandwidth throttling status

**Example Request:**
```graphql
query {
  usenetTraffic {
    bytesDownloaded
    connectionCount
    activeDownloads
    bandwidthLimit
  }
}
```

**Use Cases:**
- Bandwidth monitoring
- Download progress display
- Connection health dashboard
- Traffic alerts

---

### VFS (Virtual Filesystem) Queries

#### `vfsEntryStat(path: String!): VfsEntryStat!`

Get filesystem metadata for a VFS path (file or directory).

**Description:**
- Unix-style stat information
- Works for files and directories
- Used by FUSE mount point
- For media server browsing

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | String | ✓ | VFS path (e.g., "/Movies/BreakingBad") |

**Example Request:**
```graphql
query {
  vfsEntryStat(path: "/Movies/BreakingBad/S01E01.mkv") {
    mode
    size
    mtime
    atime
    ctime
  }
}
```

**Example Response:**
```json
{
  "data": {
    "vfsEntryStat": {
      "mode": 33188,  /* -rw-r--r-- */
      "size": 2147483648,
      "mtime": "2024-01-15T08:30:00Z",
      "atime": "2024-06-01T12:00:00Z",
      "ctime": "2024-01-15T08:30:00Z",
      "uid": 0,
      "gid": 0,
      "nlink": 1
    }
  }
}
```

**File Mode:**
- `0o100644` = Regular file (-rw-r--r--)
- `0o040755` = Directory (drwxr-xr-x)

**Use Cases:**
- FUSE mount support
- File browser UI
- Jellyfin integration
- Plex library mounting

---

#### `vfsEntry(path: String!): FileSystemEntry`

Get the filesystem entry (media file record) for a VFS path.

**Description:**
- Returns Riven's database record for the file
- Links to media item metadata
- Includes file metadata
- For file-to-item mapping

**Example Request:**
```graphql
query {
  vfsEntry(path: "/Movies/BreakingBad/S01E01.mkv") {
    id
    filePath
    fileSize
    mediaItemId
    createdAt
  }
}
```

---

#### `vfsDirectoryEntryPaths(path: String!): [String]!`

List child entries (files and directories) in a VFS directory.

**Description:**
- Directory listing for VFS paths
- Returns child names only (not full paths)
- Used for directory navigation
- FUSE mount readdir implementation

**Example Request:**
```graphql
query {
  vfsDirectoryEntryPaths(path: "/Movies") {
    /* returns ["BreakingBad", "Oppenheimer.mkv", "..."] */
  }
}
```

---

## 🔄 GraphQL Mutations

All mutations require API key authentication.

### Item Requests

#### `requestMovie(input: MovieRequestInput!): RequestItemMutationResponse!`

Request a movie to be tracked and indexed by Riven.

**Description:**
- Creates an item request for external services
- Triggers indexing and metadata fetch
- Returns detailed response with status
- Idempotent (won't create duplicates)

**Input Object:**
```graphql
input MovieRequestInput {
  title: String!                # Display title
  imdbId: String                # IMDb ID (tt0944947)
  tmdbId: String                # TMDB ID
  requestedBy: String           # Username or email
  externalRequestId: String     # Foreign key to external system
}
```

**Example Request:**
```graphql
mutation {
  requestMovie(input: {
    title: "Oppenheimer"
    imdbId: "tt15398776"
    tmdbId: "912649"
    requestedBy: "john@example.com"
    externalRequestId: "seerr_12345"
  }) {
    success
    message
    statusText
    item {
      id
      title
      state
    }
    errorCode
  }
}
```

**Example Response — Success:**
```json
{
  "data": {
    "requestMovie": {
      "success": true,
      "message": "Movie request created successfully",
      "statusText": "CREATED",
      "item": {
        "id": "100",
        "title": "Oppenheimer",
        "state": "UNRELEASED"
      },
      "errorCode": null
    }
  }
}
```

**Example Response — Already Exists:**
```json
{
  "data": {
    "requestMovie": {
      "success": true,
      "message": "Movie already requested",
      "statusText": "CONFLICT",
      "item": null,
      "errorCode": "CONFLICT"
    }
  }
}
```

**Status Codes:**
| Code | Meaning |
|------|---------|
| OK | Success |
| CREATED | New request created |
| CONFLICT | Already exists |
| BAD_REQUEST | Invalid input |
| NOT_FOUND | External service error |
| INTERNAL_SERVER_ERROR | Server error |

---

#### `requestShow(input: ShowRequestInput!): RequestItemMutationResponse!`

Request a TV show to be tracked.

**Description:**
- Request entire show or specific seasons
- Supports season selection
- Returns item request details
- Triggers show indexing

**Input Object:**
```graphql
input ShowRequestInput {
  title: String!                # Display title
  imdbId: String                # IMDb ID
  tvdbId: String                # TVDB ID (recommended for shows)
  seasons: [Int]                # Season numbers (all if omitted)
  requestedBy: String           # Requester identifier
  externalRequestId: String     # External system ID
}
```

**Example Request — All Seasons:**
```graphql
mutation {
  requestShow(input: {
    title: "Breaking Bad"
    tvdbId: "81189"
    requestedBy: "admin@example.com"
  }) {
    success
    item { id title }
  }
}
```

**Example Request — Specific Seasons:**
```graphql
mutation {
  requestShow(input: {
    title: "Breaking Bad"
    tvdbId: "81189"
    seasons: [1, 2, 3]
    requestedBy: "user@example.com"
  }) {
    success
    item { id title }
  }
}
```

---

#### `requestItems(items: [RequestInput]!): RequestItemsResult!`

Bulk request multiple movies or shows at once.

**Description:**
- Batch request operation
- More efficient than individual mutations
- Automatic deduplication
- Returns summary of new/updated items

**Example Request:**
```graphql
mutation {
  requestItems(items: [
    {
      title: "Oppenheimer"
      tmdbId: "912649"
      itemType: MOVIE
    }
    {
      title: "Breaking Bad"
      tvdbId: "81189"
      itemType: SHOW
    }
  ]) {
    count
    newItems {
      id
      title
    }
    updatedItems {
      id
      title
    }
  }
}
```

**Example Response:**
```json
{
  "data": {
    "requestItems": {
      "count": 2,
      "newItems": [
        {
          "id": "100",
          "title": "Oppenheimer"
        }
      ],
      "updatedItems": [
        {
          "id": "2",
          "title": "Breaking Bad"
        }
      ]
    }
  }
}
```

---

### Library Management

#### `addItem(title: String!, tmdbId: String, itemType: MediaItemType!): ItemMutationResponse!`

Add a media item to the library.

**Description:**
- Create item entry without external request
- For manual library population
- Triggers metadata indexing
- Must specify title and type

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `title` | String | ✓ | Item title |
| `tmdbId` | String | ✗ | TMDB ID |
| `itemType` | MediaItemType | ✓ | MOVIE or SHOW |

**Example Request:**
```graphql
mutation {
  addItem(
    title: "Breaking Bad"
    tmdbId: "1396"
    itemType: SHOW
  ) {
    success
    message
  }
}
```

---

#### `resetItems(ids: [i64]!): i64!`

Reset items to "pending" state (restart processing).

**Description:**
- Clears download progress
- Retries failed items
- Useful for items stuck in bad state
- Returns count of reset items

**Example Request:**
```graphql
mutation {
  resetItems(ids: [1, 2, 3])
}
```

**Returns:** Number of items reset (i64)

---

#### `retryItems(ids: [i64]!): i64!`

Retry processing failed items.

**Description:**
- Re-run indexing and downloading
- For items in failed state
- Useful after fixing provider issues
- Returns count of retried items

---

#### `removeItems(ids: [i64]!): i64!`

Remove items from library.

**Description:**
- Deletes item records
- Does NOT delete files (separate operation)
- Returns count of removed items
- Irreversible operation

---

#### `pauseItems(ids: [i64]!): i64!`

Pause items (stop processing).

**Description:**
- Halts download/indexing jobs
- Item remains in library
- Can be unpaused later
- Useful for temporary stopping

---

#### `unpauseItems(ids: [i64]!): i64!`

Resume paused items.

---

#### `scrapeItem(id: i64!): String!`

Manually trigger scraping (metadata/stream search) for an item.

**Description:**
- Forces immediate scrape job
- Searches for available streams
- Returns job ID for tracking
- Useful for manual refresh

**Parameters:**

| Parameter | Type | Required |
|-----------|------|----------|
| `id` | i64 | ✓ |

**Returns:** Job ID (String) for tracking progress

---

#### `deleteFilesystemEntry(id: i64!): Boolean!`

Delete a physical file from disk.

**Description:**
- Removes actual file
- Marked in database
- Triggers cleanup jobs
- Returns success status

---

### Stream Management

#### `discoverStreams(mediaItemId: i64!): StreamDiscoveryResponse!`

Discover available streams for a media item.

**Description:**
- Search stream providers
- Finds torrents, debrid links, usenet
- Returns list of available options
- Ranked by quality settings

**Parameters:**

| Parameter | Type | Required |
|-----------|------|----------|
| `mediaItemId` | i64 | ✓ |

**Example Request:**
```graphql
mutation {
  discoverStreams(mediaItemId: 2) {
    success
    streams {
      id
      title
      source
      quality
      seeders
      size
    }
  }
}
```

---

#### `downloadDiscoveredStream(mediaItemId: i64!, streamIndex: i32!): DownloadResponse!`

Initiate download of a discovered stream.

**Description:**
- Start actual download job
- Links stream to media item
- Tracks download progress
- Returns job information

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mediaItemId` | i64 | ✓ | Item to download |
| `streamIndex` | i32 | ✓ | Index in discovered streams |

---

### Settings Mutations

#### `updateRankSettings(settings: Json!): SettingsUpdateResponse!`

Update quality/ranking profile settings.

**Description:**
- Changes how streams are ranked
- Affects stream selection
- Complex JSON with weights
- Requires settings access

**Parameters:**

| Parameter | Type | Required |
|-----------|------|----------|
| `settings` | Json | ✓ |

**Settings Example:**
```json
{
  "min_resolution": "720p",
  "preferred_resolution": "1080p",
  "max_resolution": "4k",
  "minimum_seeders": 5,
  "language_preferences": ["en", "es"],
  "codec_priorities": {
    "h265": 100,
    "h264": 80
  }
}
```

---

#### `updateAllSettings(settings: Json!): SettingsUpdateResponse!`

Update complete system settings.

**Description:**
- Update all configuration at once
- Includes rank, quality, plugin settings
- Large JSON object
- Full configuration update

---

#### `updatePluginSettings(plugin: String!, settings: Json!): PluginSettingsResponse!`

Update settings for a specific plugin.

**Description:**
- Configure plugin parameters
- API keys, URLs, credentials
- Returns updated configuration
- Restart may be required

**Parameters:**

| Parameter | Type | Required | Example |
|-----------|------|----------|---------|
| `plugin` | String | ✓ | "tmdb", "stremthru" |
| `settings` | Json | ✓ | Plugin-specific config |

**Example Request:**
```graphql
mutation {
  updatePluginSettings(
    plugin: "tmdb"
    settings: { apiKey: "your-tmdb-api-key" }
  ) {
    success
    message
  }
}
```

---

#### `setPluginEnabled(plugin: String!, enabled: Boolean!): Boolean!`

Enable or disable a plugin.

**Parameters:**

| Parameter | Type | Required |
|-----------|------|----------|
| `plugin` | String | ✓ |
| `enabled` | Boolean | ✓ |

---

#### `completeInitialSetup: Boolean!`

Mark initial setup as complete.

**Description:**
- Hides setup wizard
- Enables normal operation
- Call after configuring plugins
- One-time operation

---

## 📡 GraphQL Subscriptions

Real-time event notifications via WebSocket.

### Connection Setup

```javascript
// Apollo Client example
import { WebSocketLink } from '@apollo/client/link/ws';

const wsLink = new WebSocketLink({
  uri: 'ws://localhost:8080/graphql',
  options: {
    reconnect: true,
    connectionParams: {
      authorization: `Bearer ${API_KEY}`
    }
  }
});
```

### Media Item Subscriptions

#### `showIndexed: MediaItem!`

Notifies when a show is indexed (metadata fetched).

**Description:**
- Fires when show indexing completes
- Includes full show details
- No filtering available
- All indexed shows trigger

**Example:**
```graphql
subscription {
  showIndexed {
    id
    title
    totalSeasons
    state
  }
}
```

---

#### `itemScraped: MediaItem!`

Notifies when an item is scraped (streams searched).

**Example:**
```graphql
subscription {
  itemScraped {
    id
    title
    state
  }
}
```

---

#### `itemDownloaded: MediaItem!`

Notifies when an item is downloaded.

---

#### `itemFailed: MediaItem!`

Notifies when an item fails processing.

---

#### `mediaItemStateUpdatesByTmdb(tmdbId: String!): MediaItemStateTree!`

Subscribe to state changes for an item (by TMDB ID).

**Description:**
- Real-time state updates
- Includes all seasons/episodes
- Per-item subscription
- Filtered by TMDB ID

**Parameters:**

| Parameter | Type | Required |
|-----------|------|----------|
| `tmdbId` | String | ✓ |

**Example:**
```graphql
subscription {
  mediaItemStateUpdatesByTmdb(tmdbId: "1399") {
    state
    seasons {
      state
      episodes { state }
    }
  }
}
```

---

#### `mediaItemStateUpdatesByTvdb(tvdbId: String!): MediaItemStateTree!`

Subscribe to state changes by TVDB ID.

---

### Request Subscriptions

#### `movieRequested: ItemRequest!`

Notifies when a movie is requested.

**Example:**
```graphql
subscription {
  movieRequested {
    id
    title
    requestedBy
    createdAt
  }
}
```

---

#### `showRequested: ItemRequest!`

Notifies when a show is requested.

---

#### `showRequestUpdated: ItemRequest!`

Notifies when a show request is updated (e.g., seasons added).

---

### System Subscriptions

#### `notifications: Notification!`

Subscribe to system notifications.

**Example:**
```graphql
subscription {
  notifications {
    message
    level
    timestamp
  }
}
```

---

#### `logLines: LogLine!`

Real-time application log streaming.

**Example:**
```graphql
subscription {
  logLines {
    line
    level
    timestamp
  }
}
```

---

## 🌐 REST Endpoints

### Media Streaming

#### `GET /media/{entry_id}`

Stream or download a media file.

**Description:**
- Serves media file content
- Supports HTTP range requests
- Used by Jellyfin, Plex, etc.
- VFS mount integration point

**Parameters:**

| Parameter | Type | Location | Description |
|-----------|------|----------|-------------|
| `entry_id` | i64 | Path | Filesystem entry ID |
| `Range` | Header | Header | Optional HTTP range (e.g., bytes=0-1023) |

**Example Requests:**

*Full File:*
```bash
curl http://localhost:8080/media/501 \
  -H "Authorization: Bearer api-key" \
  -o Breaking.Bad.S01E01.mkv
```

*Partial Content:*
```bash
curl http://localhost:8080/media/501 \
  -H "Range: bytes=0-1048575" \
  -H "Authorization: Bearer api-key"
```

**Response Codes:**
| Code | Meaning |
|------|---------|
| 200 | Full file |
| 206 | Partial content (range) |
| 404 | Entry not found |
| 416 | Invalid range |

**Response Headers:**
```
Content-Type: video/x-matroska
Content-Length: 2147483648
Accept-Ranges: bytes
Last-Modified: Mon, 15 Jan 2024 08:30:00 GMT
```

---

#### `HEAD /media/{entry_id}`

Get file metadata without downloading.

**Description:**
- Same as GET but no body
- Check file exists and size
- Get headers for caching

**Example:**
```bash
curl -I http://localhost:8080/media/501 \
  -H "Authorization: Bearer api-key"
```

---

### Board UI

#### `GET /board`

Apalis job queue visualization UI.

**Description:**
- Interactive job queue dashboard
- View pending/running/completed jobs
- Monitor workers
- Job status and logs

**URL:** `http://localhost:8080/board`

---

### Webhook Receivers

#### `POST /webhook/seerr`

Receive requests from Seerr service.

**Description:**
- Seerr integration endpoint
- Processes request notifications
- Creates item requests in Riven
- No authentication required (validate origin)

**Expected Payload:**
```json
{
  "notification_type": "request",
  "subject": "New Request",
  "message": "User requested breaking bad",
  "media": {
    "id": 1399,
    "type": "tv",
    "tmdb_id": 1399,
    "tvdb_id": 81189,
    "title": "Breaking Bad",
    "requested_by": "user@example.com"
  }
}
```

**Response:**
```json
{
  "status": "success",
  "message": "Request processed"
}
```

---

## 🪝 Webhooks

### Seerr Integration

Configure Seerr to send notifications to:
```
http://your-riven-server:8080/webhook/seerr
```

**Seerr Setup:**
1. Go to Settings → Notifications
2. Add Webhook
3. URL: `http://your-riven-server:8080/webhook/seerr`
4. Test connection

---

## 📦 Data Types & Objects

### Enums

#### MediaItemType
```graphql
enum MediaItemType {
  MOVIE
  SHOW
  SEASON
  EPISODE
}
```

#### MediaItemState
```graphql
enum MediaItemState {
  UNRELEASED   # Not yet aired
  RELEASED     # Aired but not requested
  DOWNLOADING  # Download in progress
  AVAILABLE    # Available and ready to stream
  UNKNOWN      # State unknown/error
  ONGOING      # Currently airing
}
```

#### MutationStatusText
```graphql
enum MutationStatusText {
  Ok
  Created
  BadRequest
  NotFound
  Conflict
  InternalServerError
}
```

---

### Input Objects

#### MovieRequestInput
```graphql
input MovieRequestInput {
  title: String!
  imdbId: String
  tmdbId: String
  requestedBy: String
  externalRequestId: String
}
```

#### ShowRequestInput
```graphql
input ShowRequestInput {
  title: String!
  imdbId: String
  tvdbId: String
  seasons: [Int]
  requestedBy: String
  externalRequestId: String
}
```

---

### Response Objects

#### MediaItem
```graphql
type MediaItem {
  id: i64!
  title: String!
  itemType: MediaItemType!
  state: MediaItemState!
  imdbId: String
  tmdbId: String
  tvdbId: String
  isRequested: Boolean!
  createdAt: DateTime!
  updatedAt: DateTime!
  seasonNumber: Int
  episodeNumber: Int
  absoluteNumber: Int
  posterPath: String
  parentId: i64
  releaseDate: String
  runtime: Int
  overview: String
  status: String
  networkName: String
}
```

#### MediaItemFull
```graphql
type MediaItemFull {
  item: MediaItem!
  filesystemEntry: FileSystemEntry
  filesystemEntries: [FileSystemEntry]!
  seasons: [SeasonFull]!
}
```

#### MediaItemStateTree
```graphql
type MediaItemStateTree {
  id: i64!
  state: MediaItemState!
  imdbId: String
  tmdbId: String
  tvdbId: String
  expectedFileCount: i64!
  seasons: [SeasonState]!
}
```

#### FileSystemEntry
```graphql
type FileSystemEntry {
  id: i64!
  mediaItemId: i64!
  filePath: String!
  fileName: String!
  originalFilename: String
  fileSize: i64!
  entryType: FileSystemEntryType!
  createdAt: DateTime!
  updatedAt: DateTime
  mediaMetadata: MediaMetadata
}
```

#### ItemsPage
```graphql
type ItemsPage {
  items: [MediaItem]!
  page: i64!
  limit: i64!
  totalItems: i64!
  totalPages: i64!
}
```

#### InstanceStatus
```graphql
type InstanceStatus {
  status: String!
  version: String!
  uptime: i64!
  startTime: DateTime!
  dbConnection: String!
  redisConnection: String!
}
```

#### PluginInfo
```graphql
type PluginInfo {
  id: String!
  name: String!
  version: String!
  description: String
  enabled: Boolean!
  author: String
}
```

#### VfsEntryStat
```graphql
type VfsEntryStat {
  mode: i32!           # Unix file mode
  size: i64!           # File size in bytes
  mtime: DateTime!     # Modification time
  atime: DateTime!     # Access time
  ctime: DateTime!     # Change time
  nlink: i32!          # Hard link count
  uid: i32!            # User ID
  gid: i32!            # Group ID
}
```

#### NntpProviderHealth
```graphql
type NntpProviderHealth {
  name: String!
  status: String!
  articlesAvailable: i64!
  retention: i32!
  connectionStatus: String!
  lastCheck: DateTime!
  connectionError: String
}
```

---

## ⚠️ Error Handling

### GraphQL Errors

**Format:**
```json
{
  "errors": [
    {
      "message": "Error description",
      "extensions": {
        "code": "ERROR_CODE"
      }
    }
  ]
}
```

### Common Error Codes

| Code | Meaning | HTTP Status |
|------|---------|-------------|
| UNAUTHENTICATED | Missing/invalid API key | 401 |
| FORBIDDEN | Insufficient permissions | 403 |
| NOT_FOUND | Resource doesn't exist | 404 |
| BAD_REQUEST | Invalid input parameters | 400 |
| CONFLICT | Resource already exists | 409 |
| INTERNAL_ERROR | Server error | 500 |

**Example Error Response:**
```json
{
  "errors": [
    {
      "message": "Authentication required",
      "extensions": {
        "code": "UNAUTHENTICATED"
      },
      "locations": [{ "line": 1, "column": 2 }]
    }
  ]
}
```

### HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 206 | Partial content (range request) |
| 400 | Bad request |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Not found |
| 409 | Conflict |
| 416 | Range not satisfiable |
| 429 | Too many requests |
| 500 | Internal server error |
| 503 | Service unavailable |

---

## ✨ Best Practices

### 1. Authentication
```javascript
// ✅ DO: Store API key securely
const API_KEY = process.env.RIVEN_API_KEY;

// ❌ DON'T: Hardcode keys
const API_KEY = 'secret-key-123';
```

### 2. Error Handling
```javascript
// ✅ DO: Handle errors gracefully
async function getMovie(id) {
  try {
    const response = await fetch('/graphql', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${API_KEY}` },
      body: JSON.stringify({
        query: `{ mediaItemById(id: ${id}) { title } }`
      })
    });
    const result = await response.json();
    if (result.errors) {
      console.error('GraphQL Error:', result.errors[0].message);
      return null;
    }
    return result.data?.mediaItemById;
  } catch (error) {
    console.error('Network Error:', error);
    return null;
  }
}
```

### 3. Pagination
```javascript
// ✅ DO: Use pagination for large datasets
async function getAllMovies() {
  const allMovies = [];
  let page = 1;
  
  while (true) {
    const response = await fetch('/graphql', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${API_KEY}` },
      body: JSON.stringify({
        query: `{
          items(page: ${page}, limit: 50, types: [MOVIE]) {
            items { id title }
            totalPages
          }
        }`
      })
    });
    const result = await response.json();
    const data = result.data.items;
    
    allMovies.push(...data.items);
    
    if (page >= data.totalPages) break;
    page++;
  }
  
  return allMovies;
}
```

### 4. Caching
```javascript
// ✅ DO: Cache frequently accessed data
const cache = new Map();
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

async function getCachedInstanceStatus() {
  const cached = cache.get('instanceStatus');
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return cached.data;
  }
  
  const response = await fetch('/graphql', {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${API_KEY}` },
    body: JSON.stringify({ query: '{ instanceStatus { status uptime } }' })
  });
  const result = await response.json();
  
  cache.set('instanceStatus', {
    data: result.data.instanceStatus,
    timestamp: Date.now()
  });
  
  return result.data.instanceStatus;
}
```

### 5. Batch Operations
```javascript
// ✅ DO: Batch requests
mutation {
  requestItems(items: [
    { title: "Movie 1", tmdbId: "111" }
    { title: "Movie 2", tmdbId: "222" }
  ]) {
    count
  }
}

// ❌ DON'T: Individual requests
mutation { requestMovie(input: { title: "Movie 1", tmdbId: "111" }) }
mutation { requestMovie(input: { title: "Movie 2", tmdbId: "222" }) }
```

---

## 💻 Code Examples

### JavaScript/Node.js
```javascript
const fetch = require('node-fetch');

const API_KEY = process.env.RIVEN_API_KEY;
const RIVEN_URL = 'http://localhost:8080';

async function getRecentItems() {
  const query = `
    query {
      mediaItems {
        id
        title
        itemType
        state
        createdAt
      }
    }
  `;

  const response = await fetch(`${RIVEN_URL}/graphql`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${API_KEY}`
    },
    body: JSON.stringify({ query })
  });

  const result = await response.json();
  
  if (result.errors) {
    console.error('Error:', result.errors);
    return null;
  }

  return result.data.mediaItems;
}

getRecentItems().then(items => {
  console.log('Recent items:', items);
});
```

### Python
```python
import requests
import json
import os

API_KEY = os.environ.get('RIVEN_API_KEY')
RIVEN_URL = 'http://localhost:8080'

def get_all_shows():
    query = """
    query {
      shows {
        id
        title
        state
        totalSeasons
      }
    }
    """
    
    response = requests.post(
        f'{RIVEN_URL}/graphql',
        headers={
            'Authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        },
        json={'query': query}
    )
    
    result = response.json()
    
    if 'errors' in result:
        print(f'Error: {result["errors"]}')
        return None
    
    return result['data']['shows']

if __name__ == '__main__':
    shows = get_all_shows()
    for show in shows:
        print(f"{show['title']} - {show['state']}")
```

### cURL
```bash
#!/bin/bash

API_KEY="${RIVEN_API_KEY}"
RIVEN_URL="http://localhost:8080"

# Get instance status
curl -X POST "$RIVEN_URL/graphql" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "{ instanceStatus { status version uptime } }"
  }' | jq .

# Request a movie
curl -X POST "$RIVEN_URL/graphql" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "mutation { requestMovie(input: {title: \"Oppenheimer\", tmdbId: \"912649\"}) { success message } }"
  }' | jq .
```

---

## 📚 Additional Resources

- **Repository:** https://github.com/olivertgwalton/riven-rs
- **GraphQL Playground:** http://localhost:8080/graphql
- **Apalis Board:** http://localhost:8080/board
- **TVDB API:** https://thetvdb.com/api
- **TMDB API:** https://www.themoviedb.org/settings/api
- **Stremio:** https://www.stremio.com/

---

## 🎯 Roadmap

- [ ] GraphQL mutations for VFS mount management
- [ ] Webhook filtering and event-based triggers
- [ ] Advanced search with boolean operators
- [ ] Job queue management via API
- [ ] Rate limiting and quota management
- [ ] Multi-tenant support
- [ ] OpenAPI/REST alternative to GraphQL

---

**Version:** 1.0.0 | **Last Updated:** June 2026 | **Maintainer:** Riven Team

For support and issues, visit the [GitHub repository](https://github.com/olivertgwalton/riven-rs/issues).
