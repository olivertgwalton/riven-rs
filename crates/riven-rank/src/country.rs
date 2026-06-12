//! Country-code helpers shared by release parsing and stream validation.
//!
//! Release names tag countries with ISO 3166-1 alpha-2 codes (plus the
//! non-ISO "UK"), while metadata providers store alpha-3 codes (TVDB's
//! "gbr"). These helpers fold both into a canonical alpha-2 form so the two
//! sources can be compared.

/// Canonical ISO 3166-1 alpha-2 form of a country code, accepting the alpha-2
/// codes the release parser emits (including the non-ISO "UK") and the alpha-3
/// codes metadata providers store (e.g. TVDB's "gbr"). Returns `None` for
/// unrecognized values so callers can fall back to a direct comparison.
#[must_use]
pub fn normalize_country_code(code: &str) -> Option<&'static str> {
    let upper = code.to_ascii_uppercase();
    let canonical = match upper.as_str() {
        "US" | "USA" => "US",
        "UK" | "GB" | "GBR" => "GB",
        "AU" | "AUS" => "AU",
        "NZ" | "NZL" => "NZ",
        "CA" | "CAN" => "CA",
        "IE" | "IRL" => "IE",
        "FR" | "FRA" => "FR",
        "DE" | "DEU" => "DE",
        "ES" | "ESP" => "ES",
        "IT" | "ITA" => "IT",
        "NL" | "NLD" => "NL",
        "BE" | "BEL" => "BE",
        "AT" | "AUT" => "AT",
        "CH" | "CHE" => "CH",
        "SE" | "SWE" => "SE",
        "NO" | "NOR" => "NO",
        "DK" | "DNK" => "DK",
        "FI" | "FIN" => "FI",
        "JP" | "JPN" => "JP",
        "KR" | "KOR" => "KR",
        "CN" | "CHN" => "CN",
        "TW" | "TWN" => "TW",
        "HK" | "HKG" => "HK",
        "IN" | "IND" => "IN",
        "BR" | "BRA" => "BR",
        "MX" | "MEX" => "MX",
        "AR" | "ARG" => "AR",
        "CL" | "CHL" => "CL",
        "CO" | "COL" => "CO",
        "RU" | "RUS" => "RU",
        "PL" | "POL" => "PL",
        "CZ" | "CZE" => "CZ",
        "HU" | "HUN" => "HU",
        "RO" | "ROU" => "RO",
        "BG" | "BGR" => "BG",
        "HR" | "HRV" => "HR",
        "RS" | "SRB" => "RS",
        "SK" | "SVK" => "SK",
        "SI" | "SVN" => "SI",
        "UA" | "UKR" => "UA",
        "GR" | "GRC" => "GR",
        "TR" | "TUR" => "TR",
        "TH" | "THA" => "TH",
        "PH" | "PHL" => "PH",
        "MY" | "MYS" => "MY",
        "SG" | "SGP" => "SG",
        "ID" | "IDN" => "ID",
        "VN" | "VNM" => "VN",
        _ => return None,
    };
    Some(canonical)
}

/// Compare two country codes, tolerating the alpha-2 vs alpha-3 format
/// mismatch between release names and metadata providers. Unrecognized codes
/// fall back to a direct case-insensitive comparison.
#[must_use]
pub fn countries_match(a: &str, b: &str) -> bool {
    match (normalize_country_code(a), normalize_country_code(b)) {
        (Some(na), Some(nb)) => na == nb,
        _ => a.eq_ignore_ascii_case(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_country_code() {
        assert_eq!(normalize_country_code("UK"), Some("GB"));
        assert_eq!(normalize_country_code("gbr"), Some("GB"));
        assert_eq!(normalize_country_code("GB"), Some("GB"));
        assert_eq!(normalize_country_code("usa"), Some("US"));
        assert_eq!(normalize_country_code("US"), Some("US"));
        assert_eq!(normalize_country_code("XX"), None);
    }

    #[test]
    fn test_countries_match_cross_format() {
        assert!(countries_match("UK", "gbr"));
        assert!(countries_match("US", "usa"));
        assert!(!countries_match("US", "gbr"));
        assert!(!countries_match("FR", "gbr"));
        // Unknown codes fall back to direct comparison.
        assert!(countries_match("XX", "xx"));
        assert!(!countries_match("XX", "YY"));
    }
}
