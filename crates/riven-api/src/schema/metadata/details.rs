//! Typed detail shapes for TMDB/TVDB metadata.
//!
//! These used to be built in the frontend (`src/lib/metadata/parser.ts`) from
//! raw upstream payloads handed over a `JSON!` scalar. The transforms live here
//! now, so the API describes what it returns and the UI only renders it.
//!
//! Movies and shows land on one [`MediaDetails`], because that is how the UI
//! reads them. Fields that only differ by name across the two upstreams are
//! absorbed with `#[serde(alias)]`; the handful that genuinely differ branch on
//! [`Source`]. Everything the transforms need but the schema does not expose
//! stays in the flattened `rest` map rather than earning a struct of its own.

use async_graphql::{ComplexObject, SimpleObject};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use riven_core::entities::helpers::{Artwork, artwork_url, tvdb_artwork_url};

use super::{TmdbListItem, transform_item};

/// Which upstream a payload came from. Set by the resolver; the wire has no
/// field for it.
#[derive(Copy, Clone, PartialEq, Eq, Default)]
pub enum Source {
    #[default]
    Tmdb,
    Tvdb,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Upstream hands back a bare path (`/abc.jpg`); the UI needs a URL. Applying
/// that at deserialisation keeps it off every use site. The sizes themselves
/// live in `riven-core` beside [`Artwork`], so there is one table of them.
fn prefixed(path: Option<String>, kind: Artwork) -> Option<String> {
    artwork_url(path.as_deref(), kind)
}

macro_rules! image_field {
    ($($name:ident => $kind:expr),* $(,)?) => { $(
        fn $name<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
            Ok(prefixed(Option::<String>::deserialize(d)?, $kind))
        }
    )* };
}

image_field! {
    profile => Artwork::Profile,
    poster => Artwork::Poster,
    backdrop => Artwork::Backdrop,
    portrait => Artwork::Portrait,
}

/// TVDB serves one size per asset, and these fields are only ever TVDB's.
fn artwork<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    Ok(tvdb_artwork_url(
        Option::<String>::deserialize(d)?.as_deref(),
    ))
}

fn text(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// Year component of an ISO-ish date (`2024-01-20`, `2024-01-20T00:00:00`,
/// `2024-01-20 00:00:00`), matching the frontend's `getYearFromISO`.
fn year_of(date: Option<&str>) -> Option<i32> {
    date?
        .split(['T', ' '])
        .next()?
        .split('-')
        .next()?
        .parse::<i32>()
        .ok()
        .filter(|year| *year > 0)
}

fn format_runtime(minutes: Option<i64>) -> Option<String> {
    let total = minutes.filter(|m| *m > 0)?;
    Some(match (total / 60, total % 60) {
        (0, m) => format!("{m}m"),
        (h, 0) => format!("{h}h"),
        (h, m) => format!("{h}h {m}m"),
    })
}

/// Descending release-date sort with missing dates last, matching the
/// frontend's `sortByReleaseDateDesc`.
fn newest_first(credits: &mut [PersonCredit]) {
    credits.sort_by(|a, b| match (&a.release_date, &b.release_date) {
        (Some(a), Some(b)) => b.cmp(a),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
}

/// Shape a raw TMDB list payload into list items, dropping repeats by id the
/// way the frontend's `transformTMDBList` did.
fn list(section: &Value) -> Vec<TmdbListItem> {
    let mut seen = std::collections::HashSet::new();
    section
        .get("results")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter(|item| {
            item.get("id")
                .and_then(Value::as_i64)
                .is_some_and(|id| seen.insert(id))
        })
        .map(|item| transform_item(item, "movie"))
        .collect()
}

/// Bare source name for an outbound link, from TMDB's `imdb_id` key or TVDB's
/// `themoviedb.com` label.
fn source_name(raw: &str) -> String {
    match raw.to_lowercase().as_str() {
        "themoviedb.com" | "themoviedb" | "tmdb" => "tmdb".to_owned(),
        "imdb.com" | "imdb" => "imdb".to_owned(),
        "official website" => "official".to_owned(),
        other => other.strip_suffix("_id").unwrap_or(other).to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Sub-shapes
// ---------------------------------------------------------------------------

#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default)]
pub struct Genre {
    pub id: i64,
    pub name: String,
    /// TVDB only.
    pub slug: Option<String>,
}

#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default)]
pub struct SpokenLanguage {
    pub english_name: Option<String>,
    pub iso_639_1: Option<String>,
    pub name: Option<String>,
}

#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default)]
pub struct ProductionCompany {
    pub id: i64,
    pub name: String,
    #[serde(deserialize_with = "profile")]
    pub logo_path: Option<String>,
    #[serde(alias = "country")]
    pub origin_country: Option<String>,
}

#[derive(SimpleObject, Clone)]
pub struct CastMember {
    pub id: i64,
    pub name: String,
    pub character: Option<String>,
    pub profile_path: Option<String>,
    /// `tmdb` or `tvdb` — which indexer this person can be looked up in.
    pub external_source: String,
}

#[derive(SimpleObject, Clone)]
pub struct Trailer {
    pub id: Option<String>,
    pub name: String,
    pub site: Option<String>,
    pub key: Option<String>,
    pub url: Option<String>,
}

#[derive(SimpleObject, Clone)]
pub struct ExternalId {
    pub source: String,
    pub id: String,
}

/// The franchise a movie belongs to, when TMDB says it belongs to one.
#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default)]
pub struct MovieCollection {
    pub id: i64,
    pub name: String,
    #[serde(deserialize_with = "poster")]
    pub poster_path: Option<String>,
    #[serde(deserialize_with = "backdrop")]
    pub backdrop_path: Option<String>,
}

#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
pub struct SeasonSummary {
    pub id: i64,
    pub number: Option<i64>,
    pub name: Option<String>,
    #[serde(deserialize_with = "artwork")]
    pub image: Option<String>,
    pub overview: Option<String>,
    /// Derived from the episode list rather than carried by TVDB.
    #[serde(skip)]
    pub episode_count: usize,
    #[serde(skip)]
    pub air_date: Option<String>,
    /// Aired Order / DVD Order / …; used to filter, never exposed.
    #[graphql(skip)]
    #[serde(rename = "type")]
    kind: Option<Value>,
}

#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
pub struct EpisodeSummary {
    pub id: i64,
    pub name: Option<String>,
    pub overview: Option<String>,
    pub aired: Option<String>,
    pub runtime: Option<i64>,
    #[serde(deserialize_with = "artwork")]
    pub image: Option<String>,
    pub number: Option<i64>,
    pub absolute_number: Option<i64>,
    pub season_number: Option<i64>,
}

// ---------------------------------------------------------------------------
// Media
// ---------------------------------------------------------------------------

/// A TMDB `/3/movie/{id}` payload (appended with `external_ids,images,
/// recommendations,similar,videos,credits,release_dates`) or a TVDB
/// `/series/{id}/extended` payload unwrapped from its `data` envelope.
#[derive(Deserialize, SimpleObject, Default)]
#[serde(default)]
#[graphql(complex)]
pub struct MediaDetails {
    pub id: i64,
    /// TMDB calls it `title`, TVDB `name`; the resolver has already replaced
    /// TVDB's with the English translation, so neither needs choosing here.
    #[serde(alias = "name")]
    pub title: Option<String>,
    pub overview: Option<String>,
    pub budget: Option<i64>,
    pub revenue: Option<i64>,
    pub genres: Vec<Genre>,
    pub spoken_languages: Vec<SpokenLanguage>,
    #[serde(alias = "companies")]
    pub production_companies: Vec<ProductionCompany>,
    #[serde(alias = "originalLanguage")]
    pub original_language: Option<String>,
    #[serde(alias = "firstAired")]
    pub release_date: Option<String>,
    #[graphql(name = "collection")]
    pub belongs_to_collection: Option<MovieCollection>,

    #[serde(alias = "averageRuntime")]
    pub runtime: Option<i64>,
    /// TMDB scores out of 10; TVDB's `score` is its own scale.
    #[serde(alias = "score")]
    pub vote_average: Option<f64>,
    #[graphql(skip)]
    seasons: Vec<SeasonSummary>,
    /// Replaced wholesale when a localised episode list is preferred.
    #[graphql(skip)]
    pub episodes: Vec<EpisodeSummary>,
    /// Everything the transforms read but the schema does not expose:
    /// `credits`, `images`, `videos`, `release_dates`, `external_ids`,
    /// `artworks`, `translations`, `characters`, `remoteIds`, …
    #[graphql(skip)]
    #[serde(flatten)]
    rest: Map<String, Value>,

    #[graphql(skip)]
    #[serde(skip)]
    pub source: Source,
    /// Filled in by the resolver, not by the upstream payload.
    #[graphql(skip)]
    #[serde(skip)]
    pub trakt: Vec<TmdbListItem>,
}

/// TVDB artwork type ids: poster (2/14), background (3/15), logo (23/25).
const POSTER_TYPES: [i64; 2] = [2, 14];
const BACKGROUND_TYPES: [i64; 2] = [3, 15];
const LOGO_TYPES: [i64; 2] = [23, 25];

impl MediaDetails {
    fn at(&self, key: &str) -> &Value {
        self.rest.get(key).unwrap_or(&Value::Null)
    }

    fn items(&self, path: &[&str]) -> &[Value] {
        path.iter()
            .fold(self.at(path[0]), |value, key| {
                if std::ptr::eq(*key, path[0]) {
                    value
                } else {
                    value.get(key).unwrap_or(&Value::Null)
                }
            })
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    fn is_show(&self) -> bool {
        self.source == Source::Tvdb
    }

    /// Highest-scoring TVDB artwork of the given types, preferring `language`
    /// when any candidate carries it. Ties keep the earlier entry, which is
    /// what the sort this replaced did — TVDB leaves plenty scored 0.
    fn artwork(&self, types: &[i64], language: Option<&str>) -> Option<String> {
        let all = self.items(&["artworks"]);
        let matching = || {
            all.iter().filter(|a| {
                a.get("type")
                    .and_then(Value::as_i64)
                    .is_some_and(|k| types.contains(&k))
            })
        };
        let preferred: Vec<&Value> = match language {
            Some(language) => matching()
                .filter(|a| a.get("language").and_then(Value::as_str) == Some(language))
                .collect(),
            None => vec![],
        };
        let pool = if preferred.is_empty() {
            matching().collect()
        } else {
            preferred
        };
        let score = |a: &Value| a.get("score").and_then(Value::as_f64).unwrap_or(0.0);
        pool.into_iter()
            .reduce(|best, a| if score(a) > score(best) { a } else { best })
            .and_then(|a| tvdb_artwork_url(a.get("image").and_then(text).as_deref()))
    }

    fn ids(&self) -> Vec<ExternalId> {
        if self.is_show() {
            self.items(&["remoteIds"])
                .iter()
                .filter_map(|remote| {
                    Some(ExternalId {
                        source: remote.get("sourceName").and_then(text).map_or_else(
                            || {
                                format!(
                                    "source_{}",
                                    remote
                                        .get("type")
                                        .and_then(Value::as_i64)
                                        .unwrap_or_default()
                                )
                            },
                            |name| source_name(&name),
                        ),
                        id: remote.get("id").and_then(text)?,
                    })
                })
                .collect()
        } else {
            self.at("external_ids")
                .as_object()
                .map(|map| {
                    map.iter()
                        .filter_map(|(key, value)| {
                            Some(ExternalId {
                                source: source_name(key),
                                id: text(value)?,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default()
        }
    }

    fn id_for(&self, source: &str) -> Option<String> {
        self.ids()
            .into_iter()
            .find(|external| external.source == source)
            .map(|external| external.id)
    }

    /// Aired-order seasons, specials dropped, in broadcast order.
    fn aired_seasons(&self) -> Vec<SeasonSummary> {
        let mut seasons: Vec<SeasonSummary> = self
            .seasons
            .iter()
            .filter(|season| {
                season
                    .kind
                    .as_ref()
                    .and_then(|k| k.get("name"))
                    .and_then(Value::as_str)
                    == Some("Aired Order")
                    && season.number != Some(0)
            })
            .cloned()
            .collect();
        seasons.sort_by_key(|season| season.number.unwrap_or_default());

        for season in &mut seasons {
            let mut aired: Vec<&str> = self
                .episodes
                .iter()
                .filter(|episode| episode.season_number == season.number)
                .filter_map(|episode| episode.aired.as_deref())
                .collect();
            aired.sort_unstable();
            season.episode_count = self
                .episodes
                .iter()
                .filter(|episode| episode.season_number == season.number)
                .count();
            season.air_date = aired.first().map(|date| (*date).to_owned());
        }
        seasons
    }

    fn aired_episodes(&self) -> Vec<EpisodeSummary> {
        self.episodes
            .iter()
            .filter(|episode| episode.season_number != Some(0))
            .cloned()
            .collect()
    }
}

/// Crew jobs the detail page surfaces; TMDB returns the whole unit otherwise.
const CREDITED_JOBS: [&str; 4] = ["Director", "Producer", "Screenplay", "Writer"];

impl MediaDetails {
    fn kind(&self) -> &'static str {
        if self.is_show() { "show" } else { "movie" }
    }

    fn status_text(&self) -> Option<String> {
        let status = self.at("status");
        text(status).or_else(|| status.get("name").and_then(text))
    }

    fn release_year(&self) -> Option<i32> {
        self.at("year")
            .as_str()
            .and_then(|year| year.parse().ok())
            .or_else(|| year_of(self.release_date.as_deref()))
    }

    fn runtime_label(&self) -> Option<String> {
        format_runtime(self.runtime)
    }

    fn homepage_url(&self) -> Option<String> {
        self.at("slug")
            .as_str()
            .map(|slug| format!("https://thetvdb.com/series/{slug}"))
            .or_else(|| text(self.at("homepage")))
    }

    fn poster(&self) -> Option<String> {
        if self.is_show() {
            self.artwork(&POSTER_TYPES, Some("eng"))
                .or_else(|| tvdb_artwork_url(text(self.at("image")).as_deref()))
        } else {
            prefixed(text(self.at("poster_path")), Artwork::Poster)
        }
    }

    fn backdrop(&self) -> Option<String> {
        if self.is_show() {
            self.artwork(&BACKGROUND_TYPES, None)
        } else {
            prefixed(text(self.at("backdrop_path")), Artwork::Backdrop)
        }
    }

    /// TVDB picks from artwork; TMDB prefers an English logo, else the first.
    fn logo_url(&self) -> Option<String> {
        if self.is_show() {
            return self.artwork(&LOGO_TYPES, Some("eng"));
        }
        let logos = self.items(&["images", "logos"]);
        logos
            .iter()
            .find(|logo| logo.get("iso_639_1").and_then(Value::as_str) == Some("en"))
            .or_else(|| logos.first())
            .and_then(|logo| prefixed(logo.get("file_path").and_then(text), Artwork::Logo))
    }

    /// TMDB: the largest official trailer, newest first among equals.
    /// TVDB: the first entry carrying a URL.
    fn best_trailer(&self) -> Option<Trailer> {
        let (entry, id, name) = if self.is_show() {
            let entry = self
                .items(&["trailers"])
                .iter()
                .find(|t| t.get("url").is_some_and(|u| !u.is_null()))?;
            (
                entry,
                entry
                    .get("id")
                    .and_then(Value::as_i64)
                    .map(|i| i.to_string()),
                entry.get("name").and_then(text),
            )
        } else {
            let entry = self
                .items(&["videos", "results"])
                .iter()
                .filter(|v| {
                    v.get("type").and_then(Value::as_str) == Some("Trailer")
                        && v.get("official").and_then(Value::as_bool).unwrap_or(false)
                })
                .max_by_key(|v| {
                    (
                        v.get("size").and_then(Value::as_i64).unwrap_or(0),
                        v.get("published_at").and_then(Value::as_str).unwrap_or(""),
                    )
                })?;
            (
                entry,
                entry.get("id").and_then(text),
                entry.get("name").and_then(text),
            )
        };

        let key = entry.get("key").and_then(text);
        let url = entry.get("url").and_then(text);
        let site = match url.as_deref().and_then(host) {
            Some(host) if host.contains("youtube") => Some("YouTube".to_owned()),
            Some(host) if host.contains("vimeo") => Some("Vimeo".to_owned()),
            Some(host) if host.contains("dailymotion") => Some("Dailymotion".to_owned()),
            Some(host) => Some(host),
            None => entry.get("site").and_then(text),
        };

        Some(Trailer {
            url: url
                .clone()
                .or_else(|| match (site.as_deref(), key.as_deref()) {
                    (Some("YouTube"), Some(key)) => {
                        Some(format!("https://www.youtube.com/watch?v={key}"))
                    }
                    _ => None,
                }),
            key: key.or_else(|| url.as_deref().and_then(youtube_key)),
            id,
            name: name.unwrap_or_default(),
            site,
        })
    }

    /// TVDB: the show's own country, else the first listed.
    /// TMDB: the US rating, first entry that carries one.
    fn rating(&self) -> String {
        let found = if self.is_show() {
            let ratings = self.items(&["contentRatings"]);
            let own = self.at("originalCountry").as_str();
            ratings
                .iter()
                .find(
                    |rating| match (rating.get("country").and_then(Value::as_str), own) {
                        (Some(country), Some(own)) => country.eq_ignore_ascii_case(own),
                        _ => false,
                    },
                )
                .or_else(|| ratings.first())
                .and_then(|rating| rating.get("name").and_then(text))
        } else {
            self.items(&["release_dates", "results"])
                .iter()
                .find(|entry| entry.get("iso_3166_1").and_then(Value::as_str) == Some("US"))
                .and_then(|entry| {
                    entry
                        .get("release_dates")?
                        .as_array()?
                        .iter()
                        .find_map(|rd| rd.get("certification").and_then(text))
                })
        };
        found.unwrap_or_else(|| "N/A".to_owned())
    }

    /// TVDB character type 3 is the acting credit; TMDB gives a billed order.
    fn cast_members(&self) -> Vec<CastMember> {
        let (path, source) = if self.is_show() {
            (vec!["characters"], "tvdb")
        } else {
            (vec!["credits", "cast"], "tmdb")
        };
        self.items(&path)
            .iter()
            .filter(|entry| !self.is_show() || entry.get("type").and_then(Value::as_i64) == Some(3))
            .take(10)
            .map(|entry| CastMember {
                id: entry
                    .get("peopleId")
                    .or_else(|| entry.get("id"))
                    .and_then(Value::as_i64)
                    .unwrap_or_default(),
                name: entry
                    .get("personName")
                    .or_else(|| entry.get("name"))
                    .and_then(text)
                    .unwrap_or_default(),
                character: entry
                    .get("character")
                    .or_else(|| entry.get("name"))
                    .and_then(text),
                profile_path: if self.is_show() {
                    tvdb_artwork_url(
                        entry
                            .get("personImgURL")
                            .or_else(|| entry.get("image"))
                            .and_then(text)
                            .as_deref(),
                    )
                } else {
                    prefixed(entry.get("profile_path").and_then(text), Artwork::Profile)
                },
                external_source: source.to_owned(),
            })
            .collect()
    }

    /// TVDB carries no crew on the series payload.
    fn crew_members(&self) -> Vec<CastMember> {
        self.items(&["credits", "crew"])
            .iter()
            .filter(|member| {
                member
                    .get("job")
                    .and_then(Value::as_str)
                    .is_some_and(|job| CREDITED_JOBS.contains(&job))
            })
            .map(|member| CastMember {
                id: member.get("id").and_then(Value::as_i64).unwrap_or_default(),
                name: member.get("name").and_then(text).unwrap_or_default(),
                character: member.get("job").and_then(text),
                profile_path: prefixed(member.get("profile_path").and_then(text), Artwork::Profile),
                external_source: "tmdb".to_owned(),
            })
            .collect()
    }

    fn countries(&self) -> Vec<String> {
        self.at("originalCountry")
            .as_str()
            .map(|country| vec![country.to_owned()])
            .unwrap_or_else(|| {
                self.items(&["origin_country"])
                    .iter()
                    .filter_map(text)
                    .collect()
            })
    }
}

#[ComplexObject]
impl MediaDetails {
    #[graphql(name = "type")]
    async fn kind_field(&self) -> &'static str {
        self.kind()
    }

    async fn status(&self) -> Option<String> {
        self.status_text()
    }

    async fn year(&self) -> Option<i32> {
        self.release_year()
    }

    async fn formatted_runtime(&self) -> Option<String> {
        self.runtime_label()
    }

    async fn homepage(&self) -> Option<String> {
        self.homepage_url()
    }

    async fn poster_path(&self) -> Option<String> {
        self.poster()
    }

    async fn backdrop_path(&self) -> Option<String> {
        self.backdrop()
    }

    async fn logo(&self) -> Option<String> {
        self.logo_url()
    }

    async fn trailer(&self) -> Option<Trailer> {
        self.best_trailer()
    }

    async fn certification(&self) -> String {
        self.rating()
    }

    async fn cast(&self) -> Vec<CastMember> {
        self.cast_members()
    }

    async fn crew(&self) -> Vec<CastMember> {
        self.crew_members()
    }

    async fn origin_country(&self) -> Vec<String> {
        self.countries()
    }

    async fn imdb_id(&self) -> Option<String> {
        self.id_for("imdb")
    }

    /// A show carries TMDB's id among its remote ids; a movie is one.
    async fn tmdb_id(&self) -> Option<i64> {
        if self.is_show() {
            self.id_for("tmdb").and_then(|id| id.parse().ok())
        } else {
            Some(self.id)
        }
    }

    async fn external_ids(&self) -> Vec<ExternalId> {
        self.ids()
    }

    async fn seasons(&self) -> Vec<SeasonSummary> {
        self.aired_seasons()
    }

    async fn episodes(&self) -> Vec<EpisodeSummary> {
        self.aired_episodes()
    }

    async fn episode_count(&self) -> usize {
        self.aired_episodes().len()
    }

    /// TVDB has no recommendation feed; the fields exist so the UI reads movies
    /// and shows through one shape.
    async fn recommendations(&self) -> Vec<TmdbListItem> {
        list(self.at("recommendations"))
    }

    async fn similar(&self) -> Vec<TmdbListItem> {
        list(self.at("similar"))
    }

    async fn trakt_recommendations(&self) -> Vec<TmdbListItem> {
        self.trakt.clone()
    }
}

fn host(url: &str) -> Option<String> {
    Some(url::Url::parse(url).ok()?.host_str()?.to_owned())
}

fn youtube_key(url: &str) -> Option<String> {
    if url.contains("youtube.com/watch") {
        return url::Url::parse(url)
            .ok()?
            .query_pairs()
            .find(|(key, _)| key == "v")
            .map(|(_, value)| value.into_owned());
    }
    url.contains("youtu.be/")
        .then(|| url.rsplit('/').next()?.split('?').next()?.to_owned().into())
        .flatten()
        .filter(|key: &String| !key.is_empty())
}

// ---------------------------------------------------------------------------
// Person / company
// ---------------------------------------------------------------------------

#[derive(Deserialize, SimpleObject, Default, Clone)]
#[serde(default)]
pub struct PersonCredit {
    pub id: i64,
    #[serde(alias = "name")]
    pub title: String,
    #[serde(alias = "original_name")]
    pub original_title: String,
    pub character: Option<String>,
    pub job: Option<String>,
    pub department: Option<String>,
    #[serde(deserialize_with = "poster")]
    pub poster_path: Option<String>,
    #[serde(deserialize_with = "backdrop")]
    pub backdrop_path: Option<String>,
    #[serde(alias = "first_air_date")]
    pub release_date: Option<String>,
    pub media_type: String,
    pub vote_average: Option<f64>,
    pub vote_count: Option<i64>,
    pub popularity: Option<f64>,
    #[serde(skip)]
    pub year: Option<i32>,
    #[serde(skip)]
    pub indexer: String,
}

impl PersonCredit {
    /// Fields TMDB does not carry on a credit directly.
    fn finished(mut self, indexer: &str) -> Self {
        self.year = year_of(self.release_date.as_deref());
        self.indexer = indexer.to_owned();
        if self.media_type != "tv" {
            self.media_type = "movie".to_owned();
        }
        self
    }
}

#[derive(SimpleObject)]
pub struct PersonDetails {
    pub id: i64,
    pub indexer: String,
    pub name: String,
    pub biography: Option<String>,
    pub birthday: Option<String>,
    pub deathday: Option<String>,
    pub place_of_birth: Option<String>,
    pub profile_path: Option<String>,
    pub known_for_department: Option<String>,
    pub gender: Option<String>,
    pub homepage: Option<String>,
    pub imdb_id: Option<String>,
    /// Surfaced beside `externalIds` so the UI can link to TMDB without
    /// searching the list for it.
    pub tmdb_id: Option<i64>,
    pub tvdb_url: Option<String>,
    pub external_ids: Vec<ExternalId>,
    pub also_known_as: Vec<String>,
    pub cast_credits: Vec<PersonCredit>,
    pub crew_credits: Vec<PersonCredit>,
}

/// A TMDB `/3/person/{id}` payload appended with `combined_credits,external_ids`.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct TmdbPerson {
    id: i64,
    name: String,
    biography: Option<String>,
    birthday: Option<String>,
    deathday: Option<String>,
    place_of_birth: Option<String>,
    #[serde(deserialize_with = "portrait")]
    profile_path: Option<String>,
    known_for_department: Option<String>,
    gender: Option<i64>,
    homepage: Option<String>,
    also_known_as: Vec<String>,
    external_ids: Map<String, Value>,
    combined_credits: Map<String, Value>,
}

impl From<TmdbPerson> for PersonDetails {
    fn from(person: TmdbPerson) -> Self {
        let credits = |key: &str| {
            let mut credits: Vec<PersonCredit> = person
                .combined_credits
                .get(key)
                .cloned()
                .and_then(|list| serde_json::from_value(list).ok())
                .unwrap_or_default();
            credits = credits.into_iter().map(|c| c.finished("tmdb")).collect();
            newest_first(&mut credits);
            credits
        };
        let external_ids: Vec<ExternalId> = person
            .external_ids
            .iter()
            .filter_map(|(key, value)| {
                Some(ExternalId {
                    source: source_name(key),
                    id: text(value)?,
                })
            })
            .collect();

        Self {
            id: person.id,
            indexer: "tmdb".to_owned(),
            name: person.name,
            biography: person.biography,
            birthday: person.birthday,
            deathday: person.deathday,
            place_of_birth: person.place_of_birth,
            profile_path: person.profile_path,
            known_for_department: person.known_for_department,
            gender: match person.gender {
                Some(1) => Some("Female".to_owned()),
                Some(2) => Some("Male".to_owned()),
                Some(3) => Some("Non-binary".to_owned()),
                _ => None,
            },
            homepage: person.homepage,
            imdb_id: external_ids
                .iter()
                .find(|e| e.source == "imdb")
                .map(|e| e.id.clone()),
            tmdb_id: Some(person.id),
            tvdb_url: None,
            external_ids,
            also_known_as: person.also_known_as,
            cast_credits: credits("cast"),
            crew_credits: credits("crew"),
        }
    }
}

/// A TVDB `/people/{id}/extended` payload, unwrapped from its `data` envelope.
/// Kept as a map because almost every field it carries is read through a
/// fallback chain rather than directly.
pub struct TvdbPerson(pub Value);

impl From<TvdbPerson> for PersonDetails {
    fn from(TvdbPerson(data): TvdbPerson) -> Self {
        let at = |key: &str| data.get(key).and_then(text);
        let pick = |keys: &[&str]| keys.iter().find_map(|key| at(key));
        let items = |path: &[&str]| -> Vec<Value> {
            path.iter()
                .try_fold(&data, |value, key| value.get(key))
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
        };

        let mut cast_credits: Vec<PersonCredit> = items(&["characters"])
            .iter()
            .filter_map(tvdb_credit)
            .collect();
        newest_first(&mut cast_credits);

        let canonical = at("name");
        let mut also_known_as: Vec<String> = Vec::new();
        for name in items(&["aliases"])
            .iter()
            .filter_map(|alias| alias.get("name").and_then(text))
        {
            if Some(&name) != canonical.as_ref() && !also_known_as.contains(&name) {
                also_known_as.push(name);
            }
        }

        let external_ids: Vec<ExternalId> = items(&["remoteIds"])
            .iter()
            .filter_map(|remote| {
                Some(ExternalId {
                    source: source_name(&remote.get("sourceName").and_then(text)?),
                    id: remote.get("id").and_then(text)?,
                })
            })
            .collect();
        let url = at("url").map(|url| format!("https://thetvdb.com{url}"));

        Self {
            id: data.get("id").and_then(Value::as_i64).unwrap_or_default(),
            indexer: "tvdb".to_owned(),
            name: canonical.unwrap_or_default(),
            biography: pick(&["biography", "overview", "bio"]),
            birthday: pick(&["birth", "birthday", "birthDate"]),
            deathday: pick(&["death", "deathday", "deathDate"]),
            place_of_birth: pick(&["birthPlace", "placeOfBirth", "birthplace"]),
            profile_path: tvdb_artwork_url(
                pick(&["image", "personImgURL", "photo", "thumbnail"]).as_deref(),
            ),
            known_for_department: pick(&["peopleType", "type", "knownForDepartment"]),
            // TVDB codes gender 1 = Male, 2 = Female — the opposite of TMDB.
            gender: match data.get("gender") {
                Some(Value::Number(n)) if n.as_i64() == Some(1) => Some("Male".to_owned()),
                Some(Value::Number(n)) if n.as_i64() == Some(2) => Some("Female".to_owned()),
                Some(value @ Value::String(_)) => text(value),
                _ => None,
            },
            homepage: url.clone(),
            imdb_id: external_ids
                .iter()
                .find(|e| e.source == "imdb")
                .map(|e| e.id.clone()),
            tmdb_id: external_ids
                .iter()
                .find(|e| e.source == "tmdb")
                .and_then(|e| e.id.parse().ok()),
            tvdb_url: url,
            external_ids,
            also_known_as,
            cast_credits,
            crew_credits: vec![],
        }
    }
}

/// One TVDB character row, which nests the work it belongs to.
fn tvdb_credit(character: &Value) -> Option<PersonCredit> {
    let num = |value: &Value, key: &str| value.get(key).and_then(Value::as_i64);
    let movie = character.get("movie").filter(|v| v.is_object());
    let work = movie.or_else(|| character.get("series").filter(|v| v.is_object()));
    let from_work = |keys: &[&str]| keys.iter().find_map(|key| work?.get(key).and_then(text));

    let id = [
        num(character, "movieId"),
        num(character, "seriesId"),
        num(character, "parentId"),
        work.and_then(|w| num(w, "id")),
    ]
    .into_iter()
    .flatten()
    .next()?;
    let title = from_work(&["name", "title", "originalTitle"])?;
    let release_date = from_work(&["releaseDate", "firstAired", "year"]);
    let score = work.and_then(|w| w.get("score")).and_then(Value::as_f64);

    Some(PersonCredit {
        id,
        original_title: from_work(&["originalTitle", "originalName"])
            .unwrap_or_else(|| title.clone()),
        title,
        character: character.get("name").and_then(text),
        job: None,
        department: None,
        poster_path: tvdb_artwork_url(from_work(&["image", "poster"]).as_deref()),
        backdrop_path: tvdb_artwork_url(from_work(&["background", "artwork"]).as_deref()),
        year: from_work(&["year"])
            .and_then(|year| year.parse().ok())
            .or_else(|| year_of(release_date.as_deref())),
        release_date,
        media_type: if movie.is_some() || character.get("movieId").is_some() {
            "movie"
        } else {
            "tv"
        }
        .to_owned(),
        vote_average: score,
        vote_count: None,
        popularity: character.get("sort").and_then(Value::as_f64).or(score),
        indexer: "tvdb".to_owned(),
    })
}

/// A TMDB `/3/company/{id}` payload, rendered through the person shape — its
/// filmography becomes the credit list.
pub fn company_details(
    data: &Value,
    movies: Vec<TmdbListItem>,
    shows: Vec<TmdbListItem>,
) -> PersonDetails {
    let at = |key: &str| data.get(key).and_then(text);
    let credit = |item: TmdbListItem, media_type: &str| PersonCredit {
        id: item.id,
        original_title: item.original_title.unwrap_or_else(|| item.title.clone()),
        title: item.title,
        character: Some("Production".to_owned()),
        job: None,
        department: None,
        poster_path: item.poster_path,
        backdrop_path: item.backdrop_path,
        year: item.year.parse().ok(),
        release_date: if media_type == "movie" {
            item.release_date
        } else {
            item.first_air_date
        },
        media_type: media_type.to_owned(),
        vote_average: item.vote_average,
        vote_count: item.vote_count,
        popularity: item.popularity,
        indexer: "tmdb".to_owned(),
    };

    let mut credits: Vec<PersonCredit> = movies
        .into_iter()
        .map(|item| credit(item, "movie"))
        .chain(shows.into_iter().map(|item| credit(item, "tv")))
        .collect();
    newest_first(&mut credits);

    PersonDetails {
        id: data.get("id").and_then(Value::as_i64).unwrap_or_default(),
        indexer: "tmdb".to_owned(),
        name: at("name").unwrap_or_default(),
        biography: at("description").or_else(|| {
            Some(format!(
                "Headquarters: {}",
                at("headquarters").unwrap_or_else(|| "Unknown".to_owned())
            ))
        }),
        birthday: None,
        deathday: None,
        place_of_birth: at("origin_country"),
        profile_path: prefixed(at("logo_path"), Artwork::Logo),
        known_for_department: Some("Production".to_owned()),
        gender: None,
        homepage: at("homepage"),
        imdb_id: None,
        tmdb_id: data.get("id").and_then(Value::as_i64),
        tvdb_url: None,
        external_ids: vec![],
        also_known_as: vec![],
        cast_credits: credits,
        crew_credits: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A TMDB movie payload.
    fn movie(value: Value) -> MediaDetails {
        serde_json::from_value(value).expect("a movie payload")
    }

    /// A TVDB series payload, as the resolver hands it over: unwrapped from its
    /// `data` envelope, with `name`/`overview` already replaced by the English
    /// translation.
    fn show(value: Value) -> MediaDetails {
        let mut details: MediaDetails = serde_json::from_value(value).expect("a show payload");
        details.source = Source::Tvdb;
        details
    }

    #[test]
    fn a_runtime_reads_as_hours_and_minutes() {
        assert_eq!(format_runtime(Some(0)), None);
        assert_eq!(format_runtime(None), None);
        assert_eq!(format_runtime(Some(45)), Some("45m".to_owned()));
        assert_eq!(format_runtime(Some(120)), Some("2h".to_owned()));
        assert_eq!(format_runtime(Some(154)), Some("2h 34m".to_owned()));
    }

    #[test]
    fn a_year_comes_off_any_iso_date_shape() {
        assert_eq!(year_of(Some("2024-01-20")), Some(2024));
        assert_eq!(year_of(Some("2024-01-20T00:00:00")), Some(2024));
        assert_eq!(year_of(Some("2024-01-20 00:00:00")), Some(2024));
        assert_eq!(year_of(Some("")), None);
        assert_eq!(year_of(None), None);
    }

    #[test]
    fn an_image_path_becomes_a_url_and_a_full_one_is_left_alone() {
        let details = movie(json!({ "poster_path": "/p.jpg", "backdrop_path": "/b.jpg" }));
        assert_eq!(
            details.poster().as_deref(),
            Some("https://image.tmdb.org/t/p/w500/p.jpg")
        );
        // A backdrop is a hero, so it takes the source rather than a resize —
        // TMDB will upscale past the original if asked for a wider size.
        assert_eq!(
            details.backdrop().as_deref(),
            Some("https://image.tmdb.org/t/p/original/b.jpg")
        );
        assert_eq!(
            movie(json!({ "poster_path": "https://cdn/p.jpg" }))
                .poster()
                .as_deref(),
            Some("https://cdn/p.jpg")
        );
        assert_eq!(movie(json!({ "poster_path": null })).poster(), None);
    }

    #[test]
    fn a_youtube_key_comes_off_both_url_forms() {
        assert_eq!(
            youtube_key("https://www.youtube.com/watch?v=dQw4w9WgXcQ"),
            Some("dQw4w9WgXcQ".to_owned())
        );
        assert_eq!(
            youtube_key("https://youtu.be/dQw4w9WgXcQ?t=10"),
            Some("dQw4w9WgXcQ".to_owned())
        );
        assert_eq!(youtube_key("https://vimeo.com/1"), None);
    }

    #[test]
    fn source_names_normalise_across_both_upstreams() {
        assert_eq!(source_name("imdb_id"), "imdb");
        assert_eq!(source_name("facebook_id"), "facebook");
        assert_eq!(source_name("TheMovieDB.com"), "tmdb");
        assert_eq!(source_name("IMDB"), "imdb");
        assert_eq!(source_name("Official Website"), "official");
    }

    #[test]
    fn a_movie_without_any_appended_sections_still_parses() {
        let details = movie(json!({ "id": 1, "title": "Bare" }));
        assert_eq!(details.id, 1);
        assert_eq!(details.title.as_deref(), Some("Bare"));
        assert_eq!(details.rating(), "N/A");
        assert!(details.cast_members().is_empty());
        assert!(details.ids().is_empty());
        assert!(details.best_trailer().is_none());
        assert!(details.belongs_to_collection.is_none());
    }

    #[test]
    fn a_movie_takes_the_us_certification_and_the_english_logo() {
        let details = movie(json!({
            "id": 7, "title": "Film", "runtime": 100, "release_date": "2019-10-04",
            "release_dates": { "results": [
                { "iso_3166_1": "GB", "release_dates": [{ "certification": "15" }] },
                { "iso_3166_1": "US", "release_dates": [
                    { "certification": "" },
                    { "certification": "R" },
                ] },
            ] },
            "images": { "logos": [
                { "iso_639_1": "de", "file_path": "/de.png" },
                { "iso_639_1": "en", "file_path": "/en.png" },
            ] },
            "credits": { "crew": [
                { "id": 1, "name": "Dir", "job": "Director" },
                { "id": 2, "name": "Grip", "job": "Key Grip" },
            ] },
        }));
        assert_eq!(details.rating(), "R");
        assert_eq!(
            details.logo_url().as_deref(),
            Some("https://image.tmdb.org/t/p/w500/en.png")
        );
        assert_eq!(details.release_year(), Some(2019));
        assert_eq!(details.runtime_label().as_deref(), Some("1h 40m"));
        let crew = details.crew_members();
        assert_eq!(crew.len(), 1, "only the credited jobs are surfaced");
        assert_eq!(crew[0].character.as_deref(), Some("Director"));
    }

    #[test]
    fn the_best_trailer_is_the_largest_official_one() {
        let details = movie(json!({ "videos": { "results": [
            { "type": "Trailer", "official": true, "size": 720, "key": "small",
              "site": "YouTube", "published_at": "2024-01-01", "name": "Small" },
            { "type": "Trailer", "official": true, "size": 1080, "key": "big",
              "site": "YouTube", "published_at": "2023-01-01", "name": "Big" },
            { "type": "Teaser", "official": true, "size": 2160, "key": "teaser",
              "site": "YouTube", "published_at": "2024-06-01", "name": "Teaser" },
            { "type": "Trailer", "official": false, "size": 2160, "key": "fan",
              "site": "YouTube", "published_at": "2024-06-01", "name": "Fan" },
        ] } }));
        let trailer = details.best_trailer().expect("a trailer");
        assert_eq!(trailer.key.as_deref(), Some("big"));
        assert_eq!(
            trailer.url.as_deref(),
            Some("https://www.youtube.com/watch?v=big")
        );
    }

    #[test]
    fn external_ids_lose_the_id_suffix_and_the_empty_entries() {
        let details = movie(json!({ "id": 3, "external_ids": {
            "imdb_id": "tt123", "facebook_id": "", "twitter_id": null, "wikidata_id": "Q1",
        } }));
        let mut ids = details.ids();
        ids.sort_by(|a, b| a.source.cmp(&b.source));
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].source, "imdb");
        assert_eq!(ids[0].id, "tt123");
        assert_eq!(ids[1].source, "wikidata");
        assert_eq!(details.id_for("imdb").as_deref(), Some("tt123"));
    }

    #[test]
    fn recommendations_drop_repeated_ids() {
        let details = movie(json!({ "recommendations": { "results": [
            { "id": 1, "title": "One" },
            { "id": 1, "title": "One again" },
            { "id": 2, "title": "Two" },
        ] } }));
        assert_eq!(list(details.at("recommendations")).len(), 2);
    }

    #[test]
    fn a_show_reads_its_title_straight_off_the_localised_payload() {
        // The resolver already replaced `name` with the English translation, so
        // nothing here has to choose between them.
        let details = show(json!({ "id": 42, "name": "English Name", "overview": "English." }));
        assert_eq!(details.title.as_deref(), Some("English Name"));
        assert_eq!(details.overview.as_deref(), Some("English."));
    }

    #[test]
    fn a_show_drops_specials_from_both_seasons_and_episodes() {
        let details = show(json!({
            "id": 42, "averageRuntime": 25, "firstAired": "2011-04-17",
            "seasons": [
                { "id": 1, "number": 0, "type": { "name": "Aired Order" } },
                { "id": 3, "number": 2, "type": { "name": "Aired Order" } },
                { "id": 2, "number": 1, "type": { "name": "Aired Order" } },
                { "id": 4, "number": 1, "type": { "name": "DVD Order" } },
            ],
            "episodes": [
                { "id": 10, "seasonNumber": 0, "number": 1, "name": "Special" },
                { "id": 11, "seasonNumber": 1, "number": 1, "name": "Pilot" },
            ],
        }));
        let seasons = details.aired_seasons();
        assert_eq!(seasons.len(), 2, "specials and DVD order drop out");
        assert_eq!(seasons[0].number, Some(1), "and the rest sort by number");
        assert_eq!(seasons[1].number, Some(2));
        let episodes = details.aired_episodes();
        assert_eq!(episodes.len(), 1);
        assert_eq!(episodes[0].name.as_deref(), Some("Pilot"));
        assert_eq!(details.release_year(), Some(2011));
        assert_eq!(details.runtime_label().as_deref(), Some("25m"));
    }

    #[test]
    fn a_show_takes_its_own_countrys_rating_then_the_first_listed() {
        let matched = show(
            json!({ "id": 1, "originalCountry": "usa", "contentRatings": [
            { "id": 1, "name": "18", "country": "gbr" },
            { "id": 2, "name": "TV-MA", "country": "usa" },
        ] }),
        );
        assert_eq!(matched.rating(), "TV-MA");

        let unmatched = show(
            json!({ "id": 1, "originalCountry": "jpn", "contentRatings": [
            { "id": 1, "name": "18", "country": "gbr" },
        ] }),
        );
        assert_eq!(unmatched.rating(), "18");
        assert_eq!(show(json!({ "id": 1 })).rating(), "N/A");
    }

    #[test]
    fn a_show_prefers_english_artwork_then_the_best_score() {
        let details = show(json!({ "id": 1, "artworks": [
            { "type": 2, "language": "deu", "image": "/de.jpg", "score": 100 },
            { "type": 2, "language": "eng", "image": "/en-low.jpg", "score": 1 },
            { "type": 2, "language": "eng", "image": "/en-high.jpg", "score": 50 },
            { "type": 3, "language": "jpn", "image": "/bg.jpg", "score": 5 },
        ] }));
        assert_eq!(
            details.poster().as_deref(),
            Some("https://artworks.thetvdb.com/en-high.jpg")
        );
        // Backdrops take no language preference, so score alone decides.
        assert_eq!(
            details.backdrop().as_deref(),
            Some("https://artworks.thetvdb.com/bg.jpg")
        );
        assert_eq!(
            details.logo_url(),
            None,
            "nothing of that type yields nothing"
        );
    }

    #[test]
    fn a_show_falls_back_to_its_own_image_without_poster_artwork() {
        let details = show(json!({ "id": 1, "image": "/fallback.jpg" }));
        assert_eq!(
            details.poster().as_deref(),
            Some("https://artworks.thetvdb.com/fallback.jpg")
        );
    }

    #[test]
    fn a_show_pulls_its_ids_out_of_the_remote_id_list() {
        let details = show(json!({ "id": 1, "remoteIds": [
            { "id": "tt999", "sourceName": "IMDB" },
            { "id": "1234", "sourceName": "TheMovieDB.com" },
            { "id": "abc", "type": 7 },
        ] }));
        assert_eq!(details.id_for("imdb").as_deref(), Some("tt999"));
        assert_eq!(details.id_for("tmdb").as_deref(), Some("1234"));
        assert!(details.ids().iter().any(|e| e.source == "source_7"));
    }

    #[test]
    fn a_show_cast_takes_the_acting_credits_only() {
        let details = show(json!({ "id": 1, "characters": [
            { "id": 1, "type": 3, "peopleId": 50, "personName": "Actor", "name": "Role" },
            { "id": 2, "type": 1, "peopleId": 60, "personName": "Director", "name": "Self" },
        ] }));
        let cast = details.cast_members();
        assert_eq!(cast.len(), 1);
        assert_eq!(cast[0].id, 50);
        assert_eq!(cast[0].name, "Actor");
        assert_eq!(cast[0].character.as_deref(), Some("Role"));
        assert_eq!(cast[0].external_source, "tvdb");
    }

    #[test]
    fn person_credits_sort_newest_first_with_undated_ones_last() {
        let person: TmdbPerson = serde_json::from_value(json!({
            "id": 1, "name": "Someone", "gender": 2,
            "combined_credits": { "cast": [
                { "id": 1, "title": "Old", "release_date": "1999-01-01" },
                { "id": 2, "title": "Undated" },
                { "id": 3, "title": "New", "release_date": "2024-01-01" },
            ] },
        }))
        .expect("a person payload");
        let details = PersonDetails::from(person);
        assert_eq!(details.gender.as_deref(), Some("Male"));
        let titles: Vec<&str> = details
            .cast_credits
            .iter()
            .map(|credit| credit.title.as_str())
            .collect();
        assert_eq!(titles, vec!["New", "Old", "Undated"]);
        assert_eq!(details.cast_credits[0].year, Some(2024));
        assert_eq!(details.cast_credits[0].media_type, "movie");
    }

    #[test]
    fn a_tv_credit_keeps_its_media_type_and_reads_the_air_date() {
        let person: TmdbPerson = serde_json::from_value(json!({
            "id": 1, "name": "Someone",
            "combined_credits": { "cast": [
                { "id": 9, "name": "A Show", "media_type": "tv", "first_air_date": "2015-06-01" },
            ] },
        }))
        .expect("a person payload");
        let credit = &PersonDetails::from(person).cast_credits[0];
        assert_eq!(credit.media_type, "tv");
        assert_eq!(credit.title, "A Show");
        assert_eq!(credit.year, Some(2015));
    }

    #[test]
    fn tvdb_gender_uses_its_own_coding() {
        let gender = |value: Value| {
            PersonDetails::from(TvdbPerson(json!({ "id": 1, "gender": value }))).gender
        };
        // TVDB codes 1 = Male, 2 = Female — the opposite of TMDB.
        assert_eq!(gender(json!(1)), Some("Male".to_owned()));
        assert_eq!(gender(json!(2)), Some("Female".to_owned()));
        assert_eq!(gender(json!("Other")), Some("Other".to_owned()));
        assert_eq!(gender(json!(null)), None);
    }

    #[test]
    fn tvdb_aliases_skip_the_canonical_name_and_repeats() {
        let person = TvdbPerson(json!({
            "id": 1,
            "name": "Canonical",
            "aliases": [{ "name": "Alias One" }, { "name": "Alias One" }, { "name": "Canonical" }],
        }));
        assert_eq!(PersonDetails::from(person).also_known_as, vec!["Alias One"]);
    }

    #[test]
    fn a_tvdb_character_credit_reads_a_series_or_a_movie() {
        let credit = |value: Value| tvdb_credit(&value);

        let series = credit(json!({
            "seriesId": 90, "name": "Role",
            "series": { "id": 90, "name": "Some Show", "firstAired": "2005-03-01", "score": 12 },
        }))
        .expect("a credit");
        assert_eq!(series.media_type, "tv");
        assert_eq!(series.title, "Some Show");
        assert_eq!(series.year, Some(2005));

        let movie = credit(json!({
            "movieId": 5, "name": "Lead",
            "movie": { "id": 5, "name": "Some Film", "year": "2020" },
        }))
        .expect("a credit");
        assert_eq!(movie.media_type, "movie");
        assert_eq!(movie.year, Some(2020));

        // Nothing identifiable yields nothing rather than a blank row.
        assert!(credit(json!({ "name": "Nobody" })).is_none());
    }

    #[test]
    fn a_company_renders_its_filmography_as_credits() {
        let item = |id: i64, title: &str, date: &str| TmdbListItem {
            id,
            title: title.to_owned(),
            poster_path: None,
            media_type: "movie".to_owned(),
            year: date.split('-').next().unwrap_or("N/A").to_owned(),
            vote_average: None,
            vote_count: None,
            popularity: None,
            overview: None,
            backdrop_path: None,
            genre_ids: vec![],
            genres: vec![],
            release_date: Some(date.to_owned()),
            first_air_date: Some(date.to_owned()),
            original_title: None,
            original_language: None,
            indexer: "tmdb".to_owned(),
        };
        let details = company_details(
            &json!({ "id": 5, "name": "A Studio", "headquarters": "Somewhere" }),
            vec![item(1, "Older", "1999-01-01")],
            vec![item(2, "Newer", "2024-01-01")],
        );
        assert_eq!(details.name, "A Studio");
        assert_eq!(details.known_for_department.as_deref(), Some("Production"));
        assert_eq!(
            details.biography.as_deref(),
            Some("Headquarters: Somewhere")
        );
        let titles: Vec<&str> = details
            .cast_credits
            .iter()
            .map(|c| c.title.as_str())
            .collect();
        assert_eq!(titles, vec!["Newer", "Older"], "newest first across both");
        assert_eq!(details.cast_credits[0].media_type, "tv");
    }
}
