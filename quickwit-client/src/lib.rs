pub mod batcher;

use progenitor::generate_api;
use serde_json::{Map, Value, to_string};

generate_api!("openapi.json");

/// Convert an array of JSON objects to a new-line delimited JSON string.
pub fn to_ndjson(values: &[Map<String, Value>]) -> serde_json::Result<String> {
    Ok(values
        .iter()
        .map(|v| to_string(v))
        .collect::<serde_json::Result<Vec<_>>>()?
        .join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn as_maps(values: &[serde_json::Value]) -> Vec<Map<String, Value>> {
        values
            .iter()
            .map(|v| v.as_object().unwrap().clone())
            .collect()
    }

    #[test]
    fn test_ndjson_empty_slice() {
        assert_eq!(to_ndjson(&[]).unwrap(), "");
    }

    #[test]
    fn test_ndjson_single_value() {
        let values = as_maps(&[json!({"key": "value"})]);
        assert_eq!(to_ndjson(&values).unwrap(), r#"{"key":"value"}"#);
    }

    #[test]
    fn test_ndjson_multiple_values() {
        let values = as_maps(&[
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ]);
        let result = to_ndjson(&values).unwrap();
        let lines: Vec<&str> = result.split('\n').collect();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], r#"{"id":1,"name":"Alice"}"#);
        assert_eq!(lines[1], r#"{"id":2,"name":"Bob"}"#);
    }

    #[test]
    fn test_ndjson_no_trailing_newline() {
        let values = as_maps(&[json!({"a": 1}), json!({"b": 2})]);
        let result = to_ndjson(&values).unwrap();

        assert!(!result.ends_with('\n'));
    }

    #[test]
    fn parse_response_no_hits() {
        let input = r#"{"num_hits":0,"hits":[],"elapsed_time_micros":2027,"errors":[]}"#;
        let _output: types::SearchResponseRest = serde_json::from_str(input).unwrap();
    }
}
