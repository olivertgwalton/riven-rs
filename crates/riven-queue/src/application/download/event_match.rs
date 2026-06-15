//! Event-based episode matching for sports-style seasons.
//!
//! Sports seasons (F1, MotoGP, ...) index episodes as venue + session
//! ("Austria (Practice 1)", "Hungary (Race)") with per-day air dates, while
//! releases are named by event ("Formula1.2020.Bahrein.GP.Qualifying...",
//! "Formula.1.70th.Anniversary.Grand.Prix.08.09.2020.RACE...") and never carry
//! SxxExx numbering, so `matches_episode_lookup` can never place them. This
//! module maps a release/file name onto an episode by requiring the session
//! kind to agree and then accepting either a venue-name match or an air-date
//! match. A candidate is only used when it resolves to exactly one episode —
//! ambiguity means no match, never a guess.

use chrono::NaiveDate;
use riven_db::entities::MediaItem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    Race,
    Sprint,
    SprintQualifying,
    Qualifying,
    Practice(u8),
}

/// Pre-parsed event identity of one episode, built from its TVDB-style title
/// ("Venue (Session)") and air date.
pub struct EpisodeEvent {
    pub episode_index: usize,
    pub venue_words: Vec<String>,
    pub session: SessionKind,
    pub aired: Option<NaiveDate>,
}

fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(str::to_ascii_lowercase)
        .collect()
}

/// Parse a session descriptor out of a token stream. Returns the session and
/// the token index where it starts (so callers can treat preceding tokens as
/// the venue).
fn parse_session(toks: &[String]) -> Option<(SessionKind, usize)> {
    for (i, tok) in toks.iter().enumerate() {
        let next = toks.get(i + 1).map(String::as_str);
        let practice_no = |n: Option<&str>| n.and_then(|s| s.parse::<u8>().ok());
        match tok.as_str() {
            "race" => return Some((SessionKind::Race, i)),
            "sprint" => {
                return match next {
                    Some("qualifying" | "shootout" | "quali" | "qualy") => {
                        Some((SessionKind::SprintQualifying, i))
                    }
                    _ => Some((SessionKind::Sprint, i)),
                };
            }
            "qualifying" | "quali" | "qualy" | "qualification" => {
                return Some((SessionKind::Qualifying, i));
            }
            "practice" => {
                if let Some(n) = practice_no(next) {
                    return Some((SessionKind::Practice(n), i));
                }
            }
            _ => {
                if let Some(rest) = tok.strip_prefix("fp")
                    && let Ok(n) = rest.parse::<u8>()
                {
                    return Some((SessionKind::Practice(n), i));
                }
            }
        }
    }
    None
}

/// Parse an episode title of the form "Venue (Session)" into its event
/// identity. Titles that don't follow the pattern (regular shows, test days)
/// return `None` and are simply never event-matched.
pub fn parse_episode_event(title: &str) -> Option<(Vec<String>, SessionKind)> {
    let open = title.find('(')?;
    let close = title.rfind(')')?;
    if close <= open {
        return None;
    }
    let session_toks = tokens(&title[open + 1..close]);
    let (session, at) = parse_session(&session_toks)?;
    if at != 0 {
        return None;
    }
    let venue_words = tokens(&title[..open]);
    if venue_words.is_empty() {
        return None;
    }
    Some((venue_words, session))
}

/// Build the event table for a season's episodes. Episodes whose titles don't
/// parse are skipped; an empty result disables event matching entirely.
pub fn episode_events(episodes: &[MediaItem]) -> Vec<EpisodeEvent> {
    episodes
        .iter()
        .enumerate()
        .filter_map(|(i, ep)| {
            let (venue_words, session) = parse_episode_event(&ep.title)?;
            Some(EpisodeEvent {
                episode_index: i,
                venue_words,
                session,
                aired: ep.aired_at,
            })
        })
        .collect()
}

/// `a` and `b` differ by at most one substitution/insertion/deletion.
fn within_one_edit(a: &str, b: &str) -> bool {
    let (short, long) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if long.len() - short.len() > 1 {
        return false;
    }
    let (sb, lb) = (short.as_bytes(), long.as_bytes());
    let mut i = 0;
    while i < sb.len() && sb[i] == lb[i] {
        i += 1;
    }
    if i == sb.len() {
        return true;
    }
    if sb.len() == lb.len() {
        sb[i + 1..] == lb[i + 1..]
    } else {
        sb[i..] == lb[i + 1..]
    }
}

/// Adjectival (demonym) forms used in release names mapped to the country
/// names sports metadata uses for episode titles. Prefix/edit-distance rules
/// can't bridge these ("Spanish"/"Spain" share only 3 letters).
const DEMONYMS: &[(&str, &str)] = &[
    ("australian", "australia"),
    ("austrian", "austria"),
    ("bahraini", "bahrain"),
    ("belgian", "belgium"),
    ("brazilian", "brazil"),
    ("british", "britain"),
    ("canadian", "canada"),
    ("chinese", "china"),
    ("dutch", "netherlands"),
    ("french", "france"),
    ("german", "germany"),
    ("hungarian", "hungary"),
    ("italian", "italy"),
    ("japanese", "japan"),
    ("mexican", "mexico"),
    ("monegasque", "monaco"),
    ("portuguese", "portugal"),
    ("qatari", "qatar"),
    ("russian", "russia"),
    ("singaporean", "singapore"),
    ("spanish", "spain"),
    ("turkish", "turkey"),
];

fn canonical_venue_word(word: &str) -> &str {
    DEMONYMS
        .iter()
        .find(|(adj, _)| *adj == word)
        .map_or(word, |(_, country)| country)
}

/// Venue-word fuzzy equality: exact (after demonym canonicalisation), prefix
/// (≥5 chars), or a single typo on longer words — covers
/// "Hungary"/"Hungarian", "Bahrain"/"Bahrein", "Styria"/"Styrian".
fn venue_word_matches(venue: &str, tok: &str) -> bool {
    let venue = canonical_venue_word(venue);
    let tok = canonical_venue_word(tok);
    if venue == tok {
        return true;
    }
    let min = venue.len().min(tok.len());
    if min >= 5 && (venue.starts_with(tok) || tok.starts_with(venue)) {
        return true;
    }
    min >= 6 && within_one_edit(venue, tok)
}

/// All venue words must appear in the candidate, except that multi-word
/// venues tolerate one absent word — "Great Britain" still matches a
/// "British.Grand.Prix" release, "Mexico City" matches plain "Mexico".
fn venue_matches(venue_words: &[String], toks: &[String]) -> bool {
    let matched = venue_words
        .iter()
        .filter(|w| toks.iter().any(|t| venue_word_matches(w, t)))
        .count();
    matched == venue_words.len() || (venue_words.len() >= 2 && matched >= venue_words.len() - 1)
}

/// Extract candidate dates from a token stream: three consecutive numeric
/// tokens forming `d.m.yyyy`, `m.d.yyyy` or `yyyy.m.d`. Release-group date
/// order is unknowable, so both day/month interpretations are returned and the
/// air-date comparison disambiguates.
fn extract_dates(toks: &[String]) -> Vec<NaiveDate> {
    let nums: Vec<Option<u32>> = toks.iter().map(|t| t.parse().ok()).collect();
    let mut dates = Vec::new();
    for w in nums.windows(3) {
        let (Some(a), Some(b), Some(c)) = (w[0], w[1], w[2]) else {
            continue;
        };
        let combos: [(u32, u32, u32); 3] = [(c, b, a), (c, a, b), (a, b, c)];
        for (y, m, d) in combos {
            if (1990..=2100).contains(&y)
                && let Some(date) = NaiveDate::from_ymd_opt(y as i32, m, d)
            {
                dates.push(date);
            }
        }
    }
    dates
}

/// Session kinds agree and the release names the venue or carries the air
/// date — the core hit test shared by season-pack and single-episode matching.
fn event_hit(
    toks: &[String],
    session: SessionKind,
    dates: &[NaiveDate],
    venue_words: &[String],
    ev_session: SessionKind,
    aired: Option<NaiveDate>,
) -> bool {
    if ev_session != session {
        return false;
    }
    venue_matches(venue_words, toks)
        || aired.is_some_and(|a| dates.iter().any(|d| (*d - a).num_days().abs() <= 1))
}

/// Match one release/file name against the season's episode events. Returns
/// the matching episode's index into the original `episodes` slice only when
/// the candidate resolves unambiguously to a single episode.
pub fn match_release_to_episode(candidate: &str, events: &[EpisodeEvent]) -> Option<usize> {
    let toks = tokens(candidate);
    let (session, _) = parse_session(&toks)?;
    let dates = extract_dates(&toks);

    let mut hits = events.iter().filter(|ev| {
        event_hit(
            &toks,
            session,
            &dates,
            &ev.venue_words,
            ev.session,
            ev.aired,
        )
    });
    let first = hits.next()?;
    if hits.next().is_some() {
        return None;
    }
    Some(first.episode_index)
}

/// Match one release name against a single episode's event identity. Used at
/// scrape-rank time (episode items have no sibling context, so there is no
/// uniqueness check — the venue/date requirement does the discriminating) and
/// again at episode persist time.
pub fn release_matches_episode(
    candidate: &str,
    episode_title: &str,
    aired: Option<NaiveDate>,
) -> bool {
    let Some((venue_words, ep_session)) = parse_episode_event(episode_title) else {
        return false;
    };
    let toks = tokens(candidate);
    let Some((session, _)) = parse_session(&toks) else {
        return false;
    };
    let dates = extract_dates(&toks);
    event_hit(&toks, session, &dates, &venue_words, ep_session, aired)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(idx: usize, title: &str, aired: Option<&str>) -> EpisodeEvent {
        let (venue_words, session) = parse_episode_event(title).expect("title should parse");
        EpisodeEvent {
            episode_index: idx,
            venue_words,
            session,
            aired: aired.map(|s| s.parse().unwrap()),
        }
    }

    fn f1_2020_sample() -> Vec<EpisodeEvent> {
        vec![
            ev(0, "Austria (Practice 1)", Some("2020-07-03")),
            ev(1, "Austria (Qualifying)", Some("2020-07-04")),
            ev(2, "Austria (Race)", Some("2020-07-05")),
            ev(3, "Hungary (Race)", Some("2020-07-19")),
            ev(4, "70th Anniversary (Race)", Some("2020-08-09")),
            ev(5, "Bahrain (Qualifying)", Some("2020-11-28")),
            ev(6, "Bahrain (Race)", Some("2020-11-29")),
        ]
    }

    #[test]
    fn matches_by_venue_and_session() {
        let events = f1_2020_sample();
        assert_eq!(
            match_release_to_episode(
                "Formula1.2020.Bahrein.GP.Qualifying.MULTi.1080p.WEB-DL.H264-ENGiNES",
                &events,
            ),
            Some(5),
        );
        assert_eq!(
            match_release_to_episode(
                "Formula1.2020.Hungarian.Grand.Prix.Race.1080p.WEB-DL.H264",
                &events,
            ),
            Some(3),
        );
    }

    #[test]
    fn matches_by_date_when_venue_is_unrecognisable() {
        let events = f1_2020_sample();
        assert_eq!(
            match_release_to_episode(
                "Formula.1.70th.Anniversary.Grand.Prix.08.09.2020.RACE.1080p.WEB-WDTeam",
                &events,
            ),
            Some(4),
        );
    }

    #[test]
    fn adjectival_and_practice_forms_match() {
        let events = f1_2020_sample();
        assert_eq!(
            match_release_to_episode("Formula1.2020.Austrian.GP.FP1.1080p.WEB", &events),
            Some(0),
        );
        assert_eq!(
            match_release_to_episode("Formula1.2020.Austrian.GP.Practice.1.1080p", &events),
            Some(0),
        );
    }

    #[test]
    fn season_packs_and_ambiguity_do_not_match() {
        let events = f1_2020_sample();
        assert_eq!(
            match_release_to_episode(
                "Formula.1.2020.Complete.Races.SkyF1HD.1080p-smcgill",
                &events
            ),
            None,
        );
        assert_eq!(
            match_release_to_episode("Formula1.2020.Race.1080p.WEB", &events),
            None,
        );
    }

    #[test]
    fn single_episode_match_requires_venue_or_date() {
        let aired = Some("2020-11-28".parse().unwrap());
        assert!(release_matches_episode(
            "Formula1.2020.Bahrein.GP.Qualifying.MULTi.1080p.WEB-DL.H264-ENGiNES",
            "Bahrain (Qualifying)",
            aired,
        ));
        assert!(!release_matches_episode(
            "Formula1.2020.Bahrein.GP.Race.1080p.WEB-DL",
            "Bahrain (Qualifying)",
            aired,
        ));
        assert!(!release_matches_episode(
            "Formula1.2020.Qualifying.1080p.WEB-DL",
            "Bahrain (Qualifying)",
            aired,
        ));
    }

    #[test]
    fn regular_episode_titles_do_not_parse_as_events() {
        assert!(parse_episode_event("Pilot").is_none());
        assert!(parse_episode_event("Test 1 - Day 1, Session 1").is_none());
        assert!(parse_episode_event("The One Where It Ends (Part 2)").is_none());
    }
}
