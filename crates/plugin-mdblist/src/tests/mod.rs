use super::*;

#[test]
fn normalize_list_name_accepts_slugs_and_common_urls() {
    assert_eq!(
        normalize_list_name("owner/list-name"),
        Some("owner/list-name".to_string())
    );
    assert_eq!(
        normalize_list_name("https://mdblist.com/lists/owner/list-name/"),
        Some("owner/list-name".to_string())
    );
    assert_eq!(
        normalize_list_name("https://mdblist.com/owner/list-name"),
        Some("owner/list-name".to_string())
    );
    assert_eq!(
        normalize_list_name("https://mdblist.com/single"),
        Some("single".to_string())
    );
    assert_eq!(normalize_list_name("   "), None);
}
