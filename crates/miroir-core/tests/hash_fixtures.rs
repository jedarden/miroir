//! Test to verify hash fixture values

use std::hash::{Hash, Hasher};
use twox_hash::XxHash64;

fn hash_for_key(key: &str) -> u64 {
    let mut h = XxHash64::with_seed(0);
    key.hash(&mut h);
    h.finish()
}

fn shard_for_key(key: &str, shard_count: u32) -> u32 {
    let hash = hash_for_key(key);
    (hash % shard_count as u64) as u32
}

#[test]
fn print_actual_hash_values() {
    let fixtures = [
        ("user:12345", 64),
        ("product:abc", 64),
        ("order:99999", 64),
        ("test", 16),
        ("hello", 32),
    ];

    println!("\n=== ACTUAL HASH VALUES ===");
    for (key, shard_count) in fixtures {
        let hash = hash_for_key(key);
        let shard = shard_for_key(key, shard_count);
        println!(
            "(\"{key}\", {shard_count}, {shard}),  // hash={hash}"
        );
    }
    println!("========================\n");
}
