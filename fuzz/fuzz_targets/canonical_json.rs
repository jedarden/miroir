#![no_main]
use libfuzzer_sys::fuzz_target;
use serde_json::Value;
use std::collections::BTreeMap;
use twox_hash::XxHash64;
use std::hash::{Hash, Hasher};

/// Canonicalize a JSON value by sorting object keys.
fn canonicalize_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map.iter().collect();
            serde_json::to_string(&sorted).unwrap_or_else(|_| "{}".to_string())
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

/// Compute hash of canonicalized JSON.
fn hash_canonical(value: &Value) -> u64 {
    let canonical = canonicalize_json(value);
    let mut hasher = XxHash64::with_seed(0);
    hasher.write(canonical.as_bytes());
    hasher.finish()
}

fuzz_target!(|data: &[u8]| {
    // Try to parse as JSON
    let json1 = match serde_json::from_slice::<Value>(data) {
        Ok(v) => v,
        Err(_) => return, // Skip invalid JSON
    };

    // Canonicalize and hash - should never panic
    let hash1 = hash_canonical(&json1);

    // Round-trip through canonical string and verify
    let canonical = canonicalize_json(&json1);
    if let Ok(json2) = serde_json::from_str::<Value>(&canonical) {
        let hash2 = hash_canonical(&json2);
        // Hashes must be identical after roundtrip
        assert_eq!(hash1, hash2, "Canonical JSON roundtrip produced different hash");
    }

    // Verify that parsing the canonical string again produces the same hash
    if let Ok(json3) = serde_json::from_str::<Value>(&canonical) {
        let hash3 = hash_canonical(&json3);
        assert_eq!(hash1, hash3, "Second canonicalization produced different hash");
    }
});
