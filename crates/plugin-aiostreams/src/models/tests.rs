use super::*;

#[test]
fn error_message_prefers_structured_error_over_detail() {
    let response: AioStreamsResponse = serde_json::from_value(serde_json::json!({
        "success": false,
        "detail": "fallback",
        "error": { "message": "specific failure" }
    }))
    .expect("aiostreams response should deserialize");

    assert_eq!(response.error_message(), Some("specific failure"));
}

#[test]
fn stream_deserializes_provider_field_names() {
    let response: AioStreamsResponse = serde_json::from_value(serde_json::json!({
        "success": true,
        "data": {
            "results": [
                {
                    "infoHash": "ABC",
                    "folderName": "Folder",
                    "filename": "file.mkv",
                    "name": "Name"
                }
            ]
        }
    }))
    .expect("aiostreams response should deserialize");

    let stream = &response.data.expect("data").results[0];
    assert_eq!(stream.info_hash.as_deref(), Some("ABC"));
    assert_eq!(stream.folder_name.as_deref(), Some("Folder"));
    assert_eq!(stream.filename.as_deref(), Some("file.mkv"));
}
