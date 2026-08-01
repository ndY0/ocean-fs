//! S3-compatible XML response formatting.
//!
//! Generates XML payloads for ListObjects and Error responses
//! that are compatible with the S3 REST API. All serialization
//! is manual (no XML library dependency) because S3 XML is a
//! fixed, simple schema.

/// Builds an S3-compatible error XML response body.
///
/// # Examples
///
/// ```
/// use oceanfs_server::s3_xml::s3_error_xml;
///
/// let xml = s3_error_xml("NoSuchKey", "The specified key does not exist.",
///                         "key", "abc123");
/// assert!(xml.contains("<Code>NoSuchKey</Code>"));
/// ```
pub fn s3_error_xml(
    code: &str,
    message: &str,
    resource: &str,
    request_id: &str,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <Resource>{}</Resource>
  <RequestId>{}</RequestId>
</Error>"#,
        escape_xml(code),
        escape_xml(message),
        escape_xml(resource),
        escape_xml(request_id),
    )
}

/// Builds an S3-compatible ListBucketResult XML response body.
///
/// # Examples
///
/// ```
/// use oceanfs_server::s3_xml::list_bucket_xml;
///
/// let entries = vec![("cat.jpg".to_string(), 1024, "abc".to_string())];
/// let xml = list_bucket_xml("my-bucket", &entries, false, None, "prefix/");
/// assert!(xml.contains("<Key>cat.jpg</Key>"));
/// ```
pub fn list_bucket_xml(
    bucket_name: &str,
    contents: &[(String, u64, String)], // (key, size, etag)
    is_truncated: bool,
    next_continuation_token: Option<&str>,
    prefix: &str,
) -> String {
    let mut xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  <KeyCount>{}</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>{}</IsTruncated>"#,
        escape_xml(bucket_name),
        escape_xml(prefix),
        contents.len(),
        is_truncated,
    );

    if let Some(token) = next_continuation_token {
        xml.push_str(&format!(
            "\n  <NextContinuationToken>{}</NextContinuationToken>",
            escape_xml(token)
        ));
    }

    if !contents.is_empty() {
        for (key, size, etag) in contents {
            xml.push_str(&format!(
                r#"
  <Contents>
    <Key>{}</Key>
    <Size>{}</Size>
    <ETag>&quot;{}&quot;</ETag>
    <StorageClass>STANDARD</StorageClass>
  </Contents>"#,
                escape_xml(key),
                size,
                escape_xml(etag),
            ));
        }
    }

    xml.push_str("\n</ListBucketResult>");
    xml
}

/// Escapes special XML characters in a string.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_xml_contains_code_and_message() {
        let xml = s3_error_xml("NoSuchKey", "Not found", "my-bucket/cat.jpg", "req-1");
        assert!(xml.contains("<Code>NoSuchKey</Code>"));
        assert!(xml.contains("<Message>Not found</Message>"));
        assert!(xml.contains("<Resource>my-bucket/cat.jpg</Resource>"));
        assert!(xml.contains("<RequestId>req-1</RequestId>"));
    }

    #[test]
    fn error_xml_escapes_special_chars() {
        let xml = s3_error_xml("CodeWith<>&", "m & m's <ok>", "resource", "id");
        // The XML structure contains <> tags, but the VALUES should be escaped
        // The Code value "CodeWith<>&" should not appear as raw < > & "
        assert!(!xml.contains("<Code>CodeWith<>&</Code>"));
        assert!(xml.contains("&gt;"));
        assert!(xml.contains("&amp;"));
    }

    #[test]
    fn list_bucket_xml_empty() {
        let xml = list_bucket_xml("test-bucket", &[], false, None, "");
        assert!(xml.contains("<Name>test-bucket</Name>"));
        assert!(xml.contains("<KeyCount>0</KeyCount>"));
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
    }

    #[test]
    fn list_bucket_xml_with_entries() {
        let entries = vec![
            ("photo.jpg".to_string(), 2048, "abc123".to_string()),
            ("notes.txt".to_string(), 512, "def456".to_string()),
        ];
        let xml = list_bucket_xml("bkt", &entries, true, Some("ct-token"), "photo");
        assert!(xml.contains("<Key>photo.jpg</Key>"));
        assert!(xml.contains("<Size>2048</Size>"));
        assert!(xml.contains("<ETag>&quot;abc123&quot;</ETag>"));
        assert!(xml.contains("<Key>notes.txt</Key>"));
        assert!(xml.contains("<KeyCount>2</KeyCount>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextContinuationToken>ct-token</NextContinuationToken>"));
    }

    #[test]
    fn list_bucket_xml_no_continuation_token_when_none() {
        let xml = list_bucket_xml("bk", &[("k".into(), 0, "e".into())], false, None, "");
        assert!(!xml.contains("NextContinuationToken"));
    }
}
