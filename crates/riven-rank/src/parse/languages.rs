use regex::Regex;
use std::sync::LazyLock;

pub(crate) struct LangPattern {
    pub code: &'static str,
    pub re: Regex,
}

pub(crate) static LANG_PATTERNS: LazyLock<Vec<LangPattern>> = LazyLock::new(|| {
    let defs: &[(&str, &str)] = &[
        ("multi", r"(?i)\b(?:multi(?:ple)?(?:[ .\-]*(?:lang|sub|audio|dub)(?:s|bed)?)?|dual[ .\-]*audio)\b"),
        ("en", r"(?i)\b(?:english?|eng|ENG)\b"),
        ("ja", r"(?i)(?:\b(?:japanese|jpn?|jap)\b|[\p{Hiragana}\p{Katakana}]{2,})"),
        ("zh", r"(?i)(?:\b(?:chinese|CH[IT]|mandarin|cantonese|CHN)\b|[\p{Han}]{2,})"),
        ("fr", r"(?i)\b(?:french|fran[cç]ais|VFF?|TRUEFRENCH|FR)\b"),
        ("es", r"(?i)\b(?:spanish|esp|espa[nñ]ol|castellano|latino|ESP?|SPA)\b"),
        ("pt", r"(?i)\b(?:portuguese|portugu[eê]s|PT|POR|BR)\b"),
        ("de", r"(?i)\b(?:german|deutsch|GER|DEU|GERMAN)\b"),
        ("ru", r"(?i)(?:\b(?:russian?|RUS?)\b|[\p{Cyrillic}]{3,})"),
        ("it", r"(?i)\b(?:italian|italiano|ITA)\b"),
        ("ko", r"(?i)(?:\b(?:korean?|KOR)\b|[\p{Hangul}]{2,})"),
        ("hi", r"(?i)\b(?:hindi?|HIN)\b"),
        ("ta", r"(?i)\b(?:tamil?|TAM)\b"),
        ("te", r"(?i)\b(?:telugu?|TEL)\b"),
        ("ml", r"(?i)\b(?:malayalam|MAL)\b"),
        ("kn", r"(?i)\b(?:kannada?|KAN)\b"),
        ("bn", r"(?i)\b(?:bengali?|bangla|BEN)\b"),
        ("mr", r"(?i)\b(?:marathi?|MAR)\b"),
        ("pa", r"(?i)\b(?:punjabi?|PAN)\b"),
        ("gu", r"(?i)\b(?:gujarati?|GUJ)\b"),
        ("ur", r"(?i)\b(?:urdu?|URD)\b"),
        ("pl", r"(?i)\b(?:polish?|polski?|POL|PL|PLDUB)\b"),
        ("cs", r"(?i)\b(?:czech?|[cč]e[sš]k|CZE|CZ)\b"),
        ("hu", r"(?i)\b(?:hungarian?|magyar|HUN|HU)\b"),
        ("ro", r"(?i)\b(?:romanian?|rom[aâ]n|ROU|RO)\b"),
        ("bg", r"(?i)\b(?:bulgarian?|BUL|BG)\b"),
        ("hr", r"(?i)\b(?:croatian?|HRV|HR)\b"),
        ("sr", r"(?i)\b(?:serbian?|SRP|SR)\b"),
        ("sk", r"(?i)\b(?:slovak?|SLK|SK)\b"),
        ("sl", r"(?i)\b(?:slovenian?|sloven|SLV|SL)\b"),
        ("uk", r"(?i)\b(?:ukrainian?|UKR)\b"),
        ("el", r"(?i)\b(?:greek?|GRE|GR|ELL)\b"),
        ("tr", r"(?i)\b(?:turkish?|t[uü]rk|TUR|TR)\b"),
        ("th", r"(?i)\b(?:thai?|THA|TH)\b"),
        ("vi", r"(?i)\b(?:vietnam(?:ese)?|VIE|VN)\b"),
        ("id", r"(?i)\b(?:indonesian?|IND|ID)\b"),
        ("ms", r"(?i)\b(?:malay(?:sian)?|MSA|MS)\b"),
        ("tl", r"(?i)\b(?:tagalog|filipino|TGL|FIL)\b"),
        ("ar", r"(?i)(?:\b(?:arabic?|ARA|AR)\b|[\p{Arabic}]{3,})"),
        ("he", r"(?i)\b(?:hebrew?|HEB|HE)\b"),
        ("fa", r"(?i)\b(?:persian|farsi|PER|FA)\b"),
        ("nl", r"(?i)\b(?:dutch?|nederland|NLD|NL)\b"),
        ("sv", r"(?i)\b(?:swedish?|svensk|SWE|SV)\b"),
        ("no", r"(?i)\b(?:norwegian?|norsk|NOR|NO)\b"),
        ("da", r"(?i)\b(?:danish?|dansk|DAN|DA)\b"),
        ("fi", r"(?i)\b(?:finnish?|suomi|FIN|FI)\b"),
        ("nb", r"(?i)\b(?:norwegian\s*bokm[aå]l|NOB|NB)\b"),
        ("nn", r"(?i)\b(?:nynorsk|NNO|NN)\b"),
        ("et", r"(?i)\b(?:estonian?|EST|ET)\b"),
        ("lv", r"(?i)\b(?:latvian?|LAV|LV)\b"),
        ("lt", r"(?i)\b(?:lithuanian?|LIT|LT)\b"),
        ("ca", r"(?i)\b(?:catalan?|CAT)\b"),
        ("eu", r"(?i)\b(?:basque|euskara|BAQ|EUS)\b"),
        ("gl", r"(?i)\b(?:galician?|GLG|GL)\b"),
        ("la", r"(?i)\b(?:latin?|LAT)\b"),
    ];
    defs.iter()
        .map(|(code, pat)| LangPattern {
            code,
            re: Regex::new(pat).unwrap(),
        })
        .collect()
});
