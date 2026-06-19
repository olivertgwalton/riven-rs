# Riven Postgres Schema

Live schema of the `riven` database (`postgres:18-alpine`). Rendered as a Mermaid ER diagram.

```mermaid
erDiagram
    media_items ||--o{ media_items : "parent_id (self)"
    media_items ||--o{ filesystem_entries : "media_item_id"
    media_items ||--o{ media_item_streams : "media_item_id"
    media_items ||--o{ media_item_blacklisted_streams : "media_item_id"
    media_items ||--o{ usenet_file_health : "media_item_id"
    media_items }o--o| streams : "active_stream_id"
    media_items }o--o| item_requests : "item_request_id"

    streams ||--o{ media_item_streams : "stream_id"
    streams ||--o{ media_item_blacklisted_streams : "stream_id"
    streams ||--o{ filesystem_entries : "stream_id"

    media_items {
        bigint id PK
        text title
        text imdb_id
        text tvdb_id
        text tmdb_id
        media_item_type item_type
        media_item_state state
        bigint parent_id FK
        bigint active_stream_id FK
        bigint item_request_id FK
        int season_number
        int episode_number
        int absolute_number
        date aired_at
        timestamptz aired_at_utc
        boolean is_anime
        boolean is_requested
        int failed_attempts
        int scraped_times
        timestamptz last_scrape_attempt_at
    }

    streams {
        bigint id PK
        text info_hash UK
        jsonb parsed_data
        bigint rank
        bigint file_size_bytes
        text magnet
    }

    filesystem_entries {
        bigint id PK
        bigint media_item_id FK
        bigint stream_id FK
        filesystem_entry_type entry_type
        text path
        bigint file_size
        text original_filename
        text download_url
        text stream_url
        text provider
        jsonb library_profiles
        text language
        text usenet_info_hash
        int usenet_file_index
    }

    media_item_streams {
        bigint media_item_id PK,FK
        bigint stream_id PK,FK
    }

    media_item_blacklisted_streams {
        bigint media_item_id PK,FK
        bigint stream_id PK,FK
        boolean permanent
    }

    item_requests {
        bigint id PK
        text imdb_id UK
        text tmdb_id UK
        text tvdb_id UK
        item_request_type request_type
        item_request_state state
        jsonb seasons
        boolean is_partial_request
        text requested_by
    }

    ranking_profiles {
        int id PK
        text name UK
        jsonb settings
        boolean is_builtin
        boolean enabled
    }

    settings {
        text key PK
        jsonb value
    }

    usenet_file_health {
        text info_hash PK
        int file_index PK
        bigint media_item_id FK
        text status
        int total_segments
        int missing_segments
        int error_segments
        int repair_attempts
        timestamptz next_repair_at
    }

    usenet_meta {
        text info_hash PK
        jsonb meta
    }

    usenet_provider_traffic {
        text host PK
        bigint bytes_downloaded
        bigint articles_downloaded
    }

    usenet_traffic_daily {
        date day PK
        text host PK
        bigint bytes_downloaded
        bigint articles_downloaded
    }
```

## Enums

| Type | Values |
|------|--------|
| `media_item_type` | movie, show, season, episode |
| `media_item_state` | indexed, unreleased, scraped, ongoing, partially_completed, completed, paused, failed |
| `show_status` | continuing, ended |
| `content_rating` | G, PG, PG-13, R, NC-17, TV-Y, TV-Y7, TV-G, TV-PG, TV-14, TV-MA |
| `filesystem_entry_type` | media, subtitle |
| `item_request_type` | movie, show |
| `item_request_state` | requested, completed, failed, ongoing, unreleased, requested_additional_seasons |

> `seaql_migrations` (SeaORM migration bookkeeping) omitted from the diagram.
