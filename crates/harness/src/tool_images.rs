//! Extract image blocks from native tool results without publishing media bytes.

use serde_json::Value;
use zeron_proto::AgentEvent;

// An encoded 24 MiB raster is at most 32 MiB of Base64. The engine checks the
// decoded size and raster signature before putting the asset in profile uploads.
const MAX_BASE64: usize = 32 * 1024 * 1024;

pub(crate) fn events(tool_id: &str, content: &Value) -> Vec<AgentEvent> {
    let Some(blocks) = content.as_array() else {
        return Vec::new();
    };
    blocks
        .iter()
        .enumerate()
        .filter_map(|(index, block)| {
            let (data, mime_type) = match block.get("type").and_then(Value::as_str)? {
                "image" => (
                    block.get("data")?.as_str()?,
                    block.get("mimeType")?.as_str()?,
                ),
                "input_image" | "inputImage" => {
                    let url = block
                        .get("image_url")
                        .or_else(|| block.get("imageUrl"))?
                        .as_str()?;
                    let (header, data) = url.split_once(',')?;
                    let mime = header.strip_prefix("data:")?.strip_suffix(";base64")?;
                    (data, mime)
                }
                _ => return None,
            };
            if !matches!(
                mime_type,
                "image/png" | "image/jpeg" | "image/webp" | "image/gif"
            ) {
                return None;
            }
            if data.len() > MAX_BASE64 {
                return Some(AgentEvent::Error {
                    message: "Tool image exceeds the 24 MiB limit".into(),
                });
            }
            Some(AgentEvent::InlineImage {
                id: format!("{tool_id}:image:{index}"),
                data: data.to_owned(),
                mime_type: mime_type.to_owned(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_mixed_native_tool_images_in_order() {
        let result = events(
            "call-1",
            &json!([
                {"type":"text","text":"before"},
                {"type":"image","mimeType":"image/png","data":"aGVsbG8="},
                {"type":"input_image","image_url":"data:image/jpeg;base64,d29ybGQ="}
            ]),
        );
        assert!(
            matches!(&result[0], AgentEvent::InlineImage { id, mime_type, .. } if id == "call-1:image:1" && mime_type == "image/png")
        );
        assert!(
            matches!(&result[1], AgentEvent::InlineImage { id, mime_type, .. } if id == "call-1:image:2" && mime_type == "image/jpeg")
        );
    }
}
