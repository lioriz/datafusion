use std::sync::Arc;

use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{ScalarFunctionImplementation, Signature, Volatility};
use datafusion_physical_expr::functions::make_scalar_function;
use regex::Regex;

/// Creates the `regexp_extract` function for DataFusion
pub fn create_regexp_extract_udf() -> datafusion_expr::ScalarUDF {
    let regexp_extract = make_scalar_function(regexp_extract_fn);

    datafusion_expr::create_udf(
        "regexp_extract",
        Signature::exact(
            vec![
                datafusion_common::DataType::Utf8,
                datafusion_common::DataType::Utf8,
                datafusion_common::DataType::Int32,
            ],
            Volatility::Immutable,
        ),
        Arc::new(datafusion_common::DataType::Utf8),
        regexp_extract,
    )
}

/// The core implementation logic of regexp_extract
fn regexp_extract_fn(args: &[ScalarValue]) -> Result<ScalarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Internal(
            "regexp_extract expects exactly 3 arguments".to_string(),
        ));
    }

    let input = &args[0];
    let pattern = &args[1];
    let idx = &args[2];

    // If any are null, return null
    if input.is_null() || pattern.is_null() || idx.is_null() {
        return Ok(ScalarValue::Utf8(None));
    }

    let input_str = input.as_utf8().unwrap();
    let pattern_str = pattern.as_utf8().unwrap();
    let idx = idx.as_i32().unwrap();

    let re = Regex::new(pattern_str).map_err(|e| {
        DataFusionError::Execution(format!("Invalid regex pattern: {e}"))
    })?;

    match re.captures(input_str) {
        Some(caps) => match caps.get(idx as usize) {
            Some(m) => Ok(ScalarValue::Utf8(Some(m.as_str().to_string()))),
            None => Ok(ScalarValue::Utf8(Some("".to_string()))),
        },
        None => Ok(ScalarValue::Utf8(Some("".to_string()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::ScalarValue;

    #[test]
    fn test_basic_match() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(Some(r"(\d+)".to_string())),
            ScalarValue::Int32(Some(0)),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("123".to_string())));
    }

    #[test]
    fn test_named_groups_not_supported() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(Some(r"(?P<num>\d+)".to_string())),
            ScalarValue::Int32(Some(0)),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("123".to_string())));
    }

    #[test]
    fn test_group_match() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(Some(r"(a)(b)(c)".to_string())),
            ScalarValue::Int32(Some(2)),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("b".to_string())));
    }

    #[test]
    fn test_index_out_of_bounds() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(Some(r"(\d+)".to_string())),
            ScalarValue::Int32(Some(5)), // Too high
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("".to_string())));
    }

    #[test]
    fn test_no_match_returns_empty_string() {
        let args = vec![
            ScalarValue::Utf8(Some("abc".to_string())),
            ScalarValue::Utf8(Some(r"(\d+)".to_string())),
            ScalarValue::Int32(Some(1)),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(Some("".to_string())));
    }

    #[test]
    fn test_invalid_regex_returns_error() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(Some("(".to_string())), // Invalid regex
            ScalarValue::Int32(Some(0)),
        ];
        let result = regexp_extract_fn(&args);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("Invalid regex pattern"),
            "Expected regex error"
        );
    }

    #[test]
    fn test_null_input_returns_null() {
        let args = vec![
            ScalarValue::Utf8(None),
            ScalarValue::Utf8(Some(r"(\d+)".to_string())),
            ScalarValue::Int32(Some(0)),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(None));
    }

    #[test]
    fn test_null_pattern_returns_null() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(None),
            ScalarValue::Int32(Some(0)),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(None));
    }

    #[test]
    fn test_null_index_returns_null() {
        let args = vec![
            ScalarValue::Utf8(Some("abc123".to_string())),
            ScalarValue::Utf8(Some(r"(\d+)".to_string())),
            ScalarValue::Int32(None),
        ];
        let result = regexp_extract_fn(&args).unwrap();
        assert_eq!(result, ScalarValue::Utf8(None));
    }
}
