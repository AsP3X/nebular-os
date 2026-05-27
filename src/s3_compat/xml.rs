use chrono::{DateTime, Utc};

use crate::storage::types::ListResult;

/// Human: Escape text for inclusion inside S3 ListObjectsV2 XML elements.
/// Agent: REPLACES & < > " ' with XML entities.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Human: Render ListObjectsV2 XML for S3-compatible bucket listings.
/// Agent: EMITS ListBucketResult; maps items to <Contents>; prefixes to <CommonPrefixes>.
pub fn list_objects_v2_xml(bucket: &str, result: &ListResult) -> String {
    let mut out = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n");
    out.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket)));
    if let Some(prefix) = &result.prefix {
        out.push_str(&format!(
            "  <Prefix>{}</Prefix>\n",
            xml_escape(prefix)
        ));
    } else {
        out.push_str("  <Prefix></Prefix>\n");
    }
    if let Some(delimiter) = &result.delimiter {
        out.push_str(&format!(
            "  <Delimiter>{}</Delimiter>\n",
            xml_escape(delimiter)
        ));
    }
    out.push_str("  <MaxKeys>");
    out.push_str(&result.items.len().to_string());
    out.push_str("</MaxKeys>\n");
    out.push_str(&format!(
        "  <IsTruncated>{}</IsTruncated>\n",
        result.is_truncated
    ));
    if let Some(token) = &result.next_start_after {
        out.push_str(&format!(
            "  <NextContinuationToken>{}</NextContinuationToken>\n",
            xml_escape(token)
        ));
    }
    out.push_str(&format!("  <KeyCount>{}</KeyCount>\n", result.items.len()));

    for prefix in &result.common_prefixes {
        out.push_str("  <CommonPrefixes>\n");
        out.push_str(&format!(
            "    <Prefix>{}</Prefix>\n",
            xml_escape(prefix)
        ));
        out.push_str("  </CommonPrefixes>\n");
    }

    for item in &result.items {
        out.push_str("  <Contents>\n");
        out.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&item.key)));
        if let Some(etag) = &item.etag {
            out.push_str(&format!("    <ETag>\"{}\"</ETag>\n", xml_escape(etag)));
        }
        out.push_str(&format!("    <Size>{}</Size>\n", item.size));
        let ts: DateTime<Utc> = item.last_modified;
        out.push_str(&format!(
            "    <LastModified>{}</LastModified>\n",
            ts.format("%Y-%m-%dT%H:%M:%S%.3fZ")
        ));
        out.push_str("  </Contents>\n");
    }

    out.push_str("</ListBucketResult>\n");
    out
}

/// Human: Minimal S3 error envelope shared by compat handlers.
/// Agent: EMITS <Error><Code/><Message/></Error>.
pub fn error_xml(code: &str, message: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>\n",
        xml_escape(code),
        xml_escape(message)
    )
}
