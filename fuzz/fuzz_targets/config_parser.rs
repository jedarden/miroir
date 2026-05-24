#![no_main]
use libfuzzer_sys::fuzz_target;
use miroir_core::config::MiroirConfig;

fuzz_target!(|data: &[u8]| {
    // Convert bytes to UTF-8 string, replacing invalid sequences
    let yaml = String::from_utf8_lossy(data);

    // Parse the YAML - this should never panic
    let _ = MiroirConfig::from_yaml(&yaml);
});
