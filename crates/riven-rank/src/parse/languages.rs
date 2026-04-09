use regex::Regex;
use std::sync::LazyLock;

pub(crate) struct LangPattern {
    pub code: &'static str,
    pub re: Regex,
}

/// Language detection patterns ordered to match PTT handler priority.
/// More specific patterns come first to avoid false positives.
pub(crate) static LANG_PATTERNS: LazyLock<Vec<LangPattern>> = LazyLock::new(|| {
    let defs: &[(&str, &str)] = &[
        // --- Pre-language contextual hints ---
        ("es", r"(?i)\b(?:temporadas?|completa)\b"),
        ("fr", r"(?i)\b(?:INT[EÉ]GRALE?|Saison)\b"),
        // --- Multi / dual audio ---
        (
            "multi",
            r"(?i)\b(?:multi(?:ple)?(?:[ .\-]*(?:lang|sub|audio|dub)(?:s|bed)?)?)\b",
        ),
        // --- English ---
        ("en", r"(?i)\bengl?(?:sub[A-Z]*)?\b"),
        ("en", r"(?i)\beng?sub[A-Z]*\b"),
        ("en", r"(?i)\bing(?:l[eéê]s)?\b"),
        ("en", r"(?i)\besub\b"),
        ("en", r"(?i)\benglish\W+(?:subs?|sdh|hi)\b"),
        ("en", r"(?i)\beng?\b"),
        ("en", r"(?i)\benglish?\b"),
        // --- Japanese ---
        (
            "ja",
            r"(?i)(?:^|[\[\]._ /\-])(?:JP|JAP|JPN)(?:$|[\[\]._ /\-])",
        ),
        ("ja", r"(?i)\b(?:JP|JAP|JPN)\b"),
        ("ja", r"(?i)\b(?:japanese|japon[eê]s)\b"),
        // --- Korean ---
        ("ko", r"(?i)\b(?:KOR|kor[ .\-]?sub)\b"),
        ("ko", r"(?i)\b(?:korean|coreano)\b"),
        // --- Chinese ---
        (
            "zh",
            r"(?i)\b(?:traditional\W*chinese|chinese\W*traditional)(?:\Wchi)?\b",
        ),
        ("zh", r"(?i)\bzh-hant\b"),
        ("zh", r"(?i)(?:^|[\W_])chi(?:$|[\W_])"),
        ("zh", r"(?i)\b(?:mand[ae]rin|ch[sn])\b"),
        ("zh", r"(?i)\bCH[IT]\b"),
        ("zh", r"(?i)\b(?:chinese|chin[eê]s)\b"),
        ("zh", r"(?i)\bzh-hans\b"),
        // --- French ---
        ("fr", r"(?i)\bFR(?:a|e|anc[eê]s|VF[FQIB2]?)\b"),
        ("fr", r"(?i)\b\[?VF[FQRIB2]?\]?\b"),
        ("fr", r"(?i)(?:VOST)?FR2?\b"),
        ("fr", r"(?i)\b(?:TRUE|SUB)\.?FRENCH\b"),
        ("fr", r"(?i)\bFRENCH\b"),
        ("fr", r"(?i)\bFre?\b"),
        ("fr", r"(?i)\bVOST(?:FR?|A)?\b"),
        // --- Latin American Spanish ---
        ("la", r"(?i)\bspanish\W?latin|american\W*(?:spa|esp?)\b"),
        ("la", r"(?i)\b(?:audio\.?)?lat(?:in?|ino)?\b"),
        // --- Spanish ---
        (
            "es",
            r"(?i)\b(?:audio\.?)?(?:ESP?|spa|(?:en[ .]+)?espa[nñ]ola?|castellano)\b",
        ),
        ("es", r"(?i)\bspanish\W+subs?\b"),
        ("es", r"(?i)\b(?:spanish|espanhol)\b"),
        // --- Portuguese ---
        ("pt", r"(?i)\b(?:p[rt]|en|port)[. (\\/-]*BR\b"),
        ("pt", r"(?i)\bbr(?:a|azil|azilian)\W+(?:pt|por)\b"),
        (
            "pt",
            r"(?i)\b(?:leg(?:endado|endas?)?|dub(?:lado)?|portugu[eèê]se?)[. \-]*BR\b",
        ),
        ("pt", r"(?i)\bleg(?:endado|endas?)\b"),
        ("pt", r"(?i)\bportugu[eèê]s[ea]?\b"),
        ("pt", r"(?i)\bPT[. \-]*(?:PT|ENG?|sub(?:s|titles?)?)\b"),
        ("pt", r"(?i)\bPT\b"),
        ("pt", r"(?i)\bpor\b"),
        // --- Italian ---
        ("it", r"(?i)\b-?ITA\b"),
        ("it", r"(?i)\bitaliano?\b"),
        // --- Greek ---
        (
            "el",
            r"(?i)\bgreek[ .\-]*(?:audio|lang(?:uage)?|subs?(?:titles?)?)?\b",
        ),
        // --- German ---
        ("de", r"(?i)\b(?:GER|DEU)\b"),
        ("de", r"(?i)\b(?:german|alem[aã]o)\b"),
        // --- Russian ---
        ("ru", r"(?i)\bRUS?\b"),
        ("ru", r"(?i)\b(?:russian|russo)\b"),
        // --- Ukrainian ---
        ("uk", r"(?i)\bUKR\b"),
        ("uk", r"(?i)\bukrainian\b"),
        // --- Hindi ---
        ("hi", r"(?i)\bhin(?:di)?\b"),
        // --- Telugu ---
        ("te", r"(?i)\b(?:tel(?:ugu)?)\b"),
        // --- Tamil ---
        ("ta", r"(?i)\bt[aâ]m(?:il)?\b"),
        // --- Malayalam ---
        ("ml", r"(?i)\b(?:MAL(?:ay)?|malayalam)\b"),
        // --- Kannada ---
        ("kn", r"(?i)\b(?:KAN(?:nada)?|kannada)\b"),
        // --- Marathi ---
        ("mr", r"(?i)\b(?:MAR(?:a(?:thi)?)?|marathi)\b"),
        // --- Gujarati ---
        ("gu", r"(?i)\b(?:GUJ(?:arati)?|gujarati)\b"),
        // --- Punjabi ---
        ("pa", r"(?i)\b(?:PUN(?:jabi)?|punjabi)\b"),
        // --- Bengali ---
        ("bn", r"(?i)\b(?:bengali|bangla)\b"),
        // --- Urdu ---
        ("ur", r"(?i)\b(?:URD|urdu)\b"),
        // --- Lithuanian ---
        ("lt", r"(?i)\bLT\b"),
        ("lt", r"(?i)\blithuanian\b"),
        // --- Latvian ---
        ("lv", r"(?i)\blatvian\b"),
        // --- Estonian ---
        ("et", r"(?i)\bestonian\b"),
        // --- Polish ---
        ("pl", r"(?i)\b(?:PL|pol)\b"),
        ("pl", r"(?i)\b(?:polish|polon[eê]s|polaco)\b"),
        (
            "pl",
            r"(?i)\b(?:PLDUB|PLSUB|DUBPL|DubbingPL|LekPL|LektorPL)\b",
        ),
        // --- Czech ---
        ("cs", r"(?i)\bCZ[EH]?\b"),
        ("cs", r"(?i)\bczech\b"),
        // --- Slovak ---
        ("sk", r"(?i)\bslo(?:vak|vakian)\b"),
        // --- Hungarian ---
        ("hu", r"(?i)\bHU\b"),
        ("hu", r"(?i)\bHUN(?:garian)?\b"),
        // --- Romanian ---
        ("ro", r"(?i)(?:^|[\W_])rosub(?:$|[\W_])"),
        ("ro", r"(?i)(?:^|[\W_])RO(?:$|[\W_])"),
        ("ro", r"(?i)\bROM(?:anian)?\b"),
        // --- Bulgarian ---
        ("bg", r"(?i)\bbul(?:garian)?\b"),
        // --- Serbian ---
        ("sr", r"(?i)\b(?:srp|serbian)\b"),
        // --- Croatian ---
        ("hr", r"(?i)\bHR\b"),
        ("hr", r"(?i)\b(?:HRV|croatian)\b"),
        // --- Slovenian ---
        ("sl", r"(?i)\bslovenian\b"),
        // --- Dutch ---
        ("nl", r"(?i)\b(?:NL|dut|holand[eê]s)\b"),
        ("nl", r"(?i)\bdutch\b"),
        ("nl", r"(?i)\bflemish\b"),
        // --- Danish ---
        ("da", r"(?i)\b(?:DK|danska|dansub|nordic)\b"),
        ("da", r"(?i)\b(?:danish|dinamarqu[eê]s)\b"),
        // --- Finnish ---
        ("fi", r"(?i)\b(?:FI|finsk|finsub|nordic)\b"),
        ("fi", r"(?i)\bfinnish\b"),
        // --- Swedish ---
        ("sv", r"(?i)\b(?:SE|swe|swesubs?|sv(?:ensk)?|nordic)\b"),
        ("sv", r"(?i)\b(?:swedish|sueco)\b"),
        // --- Norwegian ---
        ("no", r"(?i)\bNO\b"),
        ("no", r"(?i)\b(?:NOR|norsk|norsub|nordic)\b"),
        ("no", r"(?i)\b(?:norwegian|noruegu[eê]s|bokm[aå]l|nob)\b"),
        // --- Norwegian Bokmål ---
        ("nb", r"(?i)\b(?:norwegian\s*bokm[aå]l|NOB|NB)\b"),
        // --- Nynorsk ---
        ("nn", r"(?i)\b(?:nynorsk|NNO|NN)\b"),
        // --- Arabic ---
        ("ar", r"(?i)\barab(?:ic)?(?:\W+subtitle)?\b"),
        ("ar", r"(?i)\b(?:arabic|[aá]rabe|ara)\b"),
        // --- Turkish ---
        ("tr", r"(?i)\b(?:turkish|tur(?:co)?|t[uü]rk)\b"),
        ("tr", r"(?i)\b(?:TİVİBU|tivibu|bitturk)\b"),
        // --- Vietnamese ---
        ("vi", r"(?i)(?:^|[\W_])vie(?:$|[\W_])"),
        ("vi", r"(?i)\bvietnamese\b"),
        // --- Indonesian ---
        ("id", r"(?i)(?:^|[\W_])ind(?:$|[\W_])"),
        ("id", r"(?i)\bind(?:onesian)?\b"),
        // --- Thai ---
        ("th", r"(?i)\b(?:thai|tailand[eê]s)\b"),
        ("th", r"(?i)\bTHA\b"),
        // --- Malay ---
        ("ms", r"(?i)(?:^|[\W_])may(?:$|[\W_])"),
        ("ms", r"(?i)\bmalay(?:sian)?\b"),
        // --- Hebrew ---
        ("he", r"(?i)\bheb(?:rew|raico)?\b"),
        // --- Persian ---
        ("fa", r"(?i)\b(?:persian|persa|farsi)\b"),
        // --- Tagalog ---
        ("tl", r"(?i)\b(?:tagalog|filipino)\b"),
        // --- Catalan ---
        ("ca", r"(?i)\bcatalan?\b"),
        // --- Basque ---
        ("eu", r"(?i)\b(?:basque|euskara)\b"),
        // --- Galician ---
        ("gl", r"(?i)\bgalician?\b"),
        // --- Latin ---
        ("la", r"(?i)\blatin?\b"),
        // --- Unicode script detection ---
        ("ja", r"[\p{Hiragana}\p{Katakana}]{2,}"), // Japanese
        ("ja", r"[\x{FF66}-\x{FF9F}]{2,}"),        // Half-width Katakana
        ("zh", r"[\p{Han}]{2,}"),                  // Chinese CJK
        ("ru", r"[\p{Cyrillic}]{3,}"),             // Russian / Cyrillic
        ("ar", r"[\p{Arabic}]{3,}"),               // Arabic
        ("kn", r"[\x{0C80}-\x{0CFF}]{2,}"),        // Kannada
        ("ml", r"[\x{0D00}-\x{0D7F}]{2,}"),        // Malayalam
        ("th", r"[\x{0E00}-\x{0E7F}]{2,}"),        // Thai
        ("hi", r"[\x{0900}-\x{097F}]{2,}"),        // Hindi / Devanagari
        ("bn", r"[\x{0980}-\x{09FF}]{2,}"),        // Bengali
        ("gu", r"[\x{0A00}-\x{0A7F}]{2,}"),        // Gujarati
        ("ko", r"[\p{Hangul}]{2,}"),               // Korean
    ];

    defs.iter()
        .map(|(code, pat)| LangPattern {
            code,
            re: Regex::new(pat).unwrap(),
        })
        .collect()
});

pub(crate) fn translate_langs(langs: &[String]) -> Vec<String> {
    langs
        .iter()
        .filter_map(|lang| match lang.as_str() {
            "en" => Some("English"),
            "ja" => Some("Japanese"),
            "zh" => Some("Chinese"),
            "ru" => Some("Russian"),
            "ar" => Some("Arabic"),
            "pt" => Some("Portuguese"),
            "es" => Some("Spanish"),
            "fr" => Some("French"),
            "de" => Some("German"),
            "it" => Some("Italian"),
            "ko" => Some("Korean"),
            "hi" => Some("Hindi"),
            "bn" => Some("Bengali"),
            "pa" => Some("Punjabi"),
            "mr" => Some("Marathi"),
            "gu" => Some("Gujarati"),
            "ta" => Some("Tamil"),
            "te" => Some("Telugu"),
            "kn" => Some("Kannada"),
            "ml" => Some("Malayalam"),
            "th" => Some("Thai"),
            "vi" => Some("Vietnamese"),
            "id" => Some("Indonesian"),
            "tr" => Some("Turkish"),
            "he" => Some("Hebrew"),
            "fa" => Some("Persian"),
            "uk" => Some("Ukrainian"),
            "el" => Some("Greek"),
            "lt" => Some("Lithuanian"),
            "lv" => Some("Latvian"),
            "et" => Some("Estonian"),
            "pl" => Some("Polish"),
            "cs" => Some("Czech"),
            "sk" => Some("Slovak"),
            "hu" => Some("Hungarian"),
            "ro" => Some("Romanian"),
            "bg" => Some("Bulgarian"),
            "sr" => Some("Serbian"),
            "hr" => Some("Croatian"),
            "sl" => Some("Slovenian"),
            "nl" => Some("Dutch"),
            "da" => Some("Danish"),
            "fi" => Some("Finnish"),
            "sv" => Some("Swedish"),
            "no" => Some("Norwegian"),
            "ms" => Some("Malay"),
            "la" => Some("Latino"),
            _ => None,
        })
        .map(str::to_string)
        .collect()
}
