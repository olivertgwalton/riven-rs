import type {
	ActivePlaybackSession,
	DebridUserInfo,
	IndexerStats,
	LibraryStats,
	NntpProviderHealth,
	UsenetStreamingHealth,
	UsenetTitleHealth,
	UsenetTitleHealthSummary,
	UsenetTraffic,
	YearRelease,
} from "$lib/gql/schema";

// Shared by the load (initial fetch) and the page (live refresh / polling).
// Every selection below is the full field set of its schema type, so the
// generated types are used as-is.

export const STATS_QUERY = `
    query DashboardStats {
        stats {
            totalMovies
            totalShows
            totalSeasons
            totalEpisodes
            totalItems
            incompleteItems
            completionRate
            completed
            scraped
            indexed
            failed
            paused
            ongoing
            partiallyCompleted
            unreleased
        }
        activity
        yearReleases {
            year
            count
        }
    }
`;

export type DashboardStats = {
	stats: LibraryStats;
	activity: Record<string, number>;
	yearReleases: YearRelease[];
};

export const ACTIVE_PLAYBACK_QUERY = `
    query {
        activePlaybackSessions {
            server
            userName
            parentTitle
            itemTitle
            itemType
            seasonNumber
            episodeNumber
            playbackState
            playbackMethod
            positionSeconds
            durationSeconds
            deviceName
            clientName
            imageUrl
        }
    }
`;

export type ActivePlaybackResult = {
	activePlaybackSessions: ActivePlaybackSession[];
};

export const USENET_HEALTH_QUERY = `
    query {
        nntpProviders {
            host
            port
            priority
            isBackup
            maxConnections
            openConnections
            idleConnections
            activeConnections
            demoted
            consecutiveNotFound
        }
        usenetStreamingHealth {
            caches {
                name
                bytesUsed
                bytesMax
                entries
                hits
                misses
                hitRate
            }
            cacheHitRate
            fetchesOk
            fetchesFailed
            fetchSuccessRate
            bytesDecoded
            inFlight
            deadSegments
            activeStreams
        }
        usenetTitleHealth {
            infoHash
            fileIndex
            mediaItemId
            status
            totalSegments
            sampledSegments
            missingSegments
            errorSegments
            missingPct
            checkedAt
            repairAttempts
            nextRepairAt
            title
            subtitle
            posterPath
            mediaType
        }
        usenetTitleHealthSummary {
            healthy
            unhealthy
            notIngested
            unknown
            total
        }
        usenetTraffic {
            totalBytesDownloaded
            totalArticlesDownloaded
            providers {
                host
                bytesDownloaded
                articlesDownloaded
            }
            daily {
                day
                host
                bytesDownloaded
                articlesDownloaded
            }
        }
    }
`;

export type UsenetHealthResult = {
	nntpProviders: NntpProviderHealth[];
	usenetStreamingHealth: UsenetStreamingHealth | null;
	usenetTitleHealth: UsenetTitleHealth[];
	usenetTitleHealthSummary: UsenetTitleHealthSummary;
	usenetTraffic: UsenetTraffic | null;
};

export const EMPTY_TITLE_SUMMARY: UsenetTitleHealthSummary = {
	healthy: 0,
	unhealthy: 0,
	notIngested: 0,
	unknown: 0,
	total: 0,
};

export const INDEXER_STATS_QUERY = `
    query {
        indexerStats {
            indexer
            searchQueries
            capsQueries
            successfulGrabs
        }
    }
`;

export type IndexerStatsResult = { indexerStats: IndexerStats[] };

export const DEBRID_ACCOUNT_INFO_QUERY = `
    query {
        debridAccountInfo {
            store
            email
            username
            subscriptionStatus
            premiumUntil
            cooldownUntil
            totalDownloadedBytes
            points
        }
    }
`;

export type DebridAccountInfoResult = { debridAccountInfo: DebridUserInfo[] };
