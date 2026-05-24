#![no_main]
use libfuzzer_sys::fuzz_target;
use miroir_core::query_planner::QueryPlanner;
use miroir_core::query_planner::QueryPlannerConfig;

fuzz_target!(|data: &[u8]| {
    // Convert bytes to UTF-8 string, replacing invalid sequences
    let filter = String::from_utf8_lossy(data);

    // Create a query planner with default config
    let planner = QueryPlanner::new(QueryPlannerConfig::default());

    // Set a dummy primary key
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        planner.set_primary_key("test_index".to_string(), "id".to_string()).await;

        // Try to plan a query with this filter - should never panic
        let _plan = planner.plan("test_index", &Some(filter.to_string()), 64).await;
    });
});
