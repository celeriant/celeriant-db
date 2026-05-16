pub const BUILTIN_DICT_NAME: &str = "json-web-events-v1";
pub const BUILTIN_DICT_BYTES: &[u8] =
    include_bytes!("../dicts/json_web_events_v1.zstd_dict");

/// Returns `Some(BUILTIN_DICT_BYTES)` when `name` matches `BUILTIN_DICT_NAME`, else `None`.
///
/// Shaped as `fn(&str) -> Option<&'static [u8]>` for `validate_or_create`.
pub fn resolve_builtin_dict(name: &str) -> Option<&'static [u8]> {
    if name == BUILTIN_DICT_NAME {
        Some(BUILTIN_DICT_BYTES)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_dict_bytes_is_nonempty() {
        assert!(BUILTIN_DICT_BYTES.len() >= 256, "dict too small: {} bytes", BUILTIN_DICT_BYTES.len());
    }

    #[test]
    fn resolve_builtin_dict_returns_some_for_known_name() {
        assert!(resolve_builtin_dict(BUILTIN_DICT_NAME).is_some());
    }

    #[test]
    fn resolve_builtin_dict_returns_none_for_unknown_name() {
        assert!(resolve_builtin_dict("not-a-real-dict").is_none());
        assert!(resolve_builtin_dict("").is_none());
    }
}
