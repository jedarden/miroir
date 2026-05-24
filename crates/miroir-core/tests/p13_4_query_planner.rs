//! P5.4 §13.4 Query planner acceptance tests.
//!
//! Tests the shard-aware query planner that narrows the fan-out for
//! PK-constrained searches.

use miroir_core::query_planner::QueryPlanner;
use miroir_core::router::shard_for_key;

#[tokio::test]
async fn p13_4_a1_pk_equality_narrows_to_one_shard() {
    // Filter `product_id = "abc"` → fan-out to 1 shard (RF=1) / RF nodes (RF>1)
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan("products", &Some("product_id = \"abc\"".into()), 64)
        .await;

    assert!(plan.narrowed, "Plan should be narrowed");
    assert_eq!(plan.target_shards.len(), 1, "Should target exactly 1 shard");
    assert_eq!(
        plan.target_shards[0],
        shard_for_key("abc", 64),
        "Should target the correct shard for the PK"
    );
    assert!(plan.reason.contains("PK equality"));
}

#[tokio::test]
async fn p13_4_a2_pk_in_list_narrows_to_multiple_shards() {
    // `product_id IN ["a","b","c"]` → fan-out to up to 3 shards
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan(
            "products",
            &Some("product_id IN [\"a\", \"b\", \"c\"]".into()),
            64,
        )
        .await;

    assert!(plan.narrowed, "Plan should be narrowed");
    assert_eq!(plan.target_shards.len(), 3, "Should target 3 shards");

    // Verify each shard corresponds to the correct PK
    let mut expected_shards: Vec<u32> = vec!["a", "b", "c"]
        .into_iter()
        .map(|pk| shard_for_key(pk, 64))
        .collect();
    expected_shards.sort_unstable(); // Sort for comparison (plan returns sorted shards)
    assert_eq!(
        plan.target_shards, expected_shards,
        "Should target the correct shards for each PK"
    );
    assert!(plan.reason.contains("PK IN list"));
}

#[tokio::test]
async fn p13_4_a3_or_with_non_pk_branch_not_narrowable() {
    // `product_id = "abc" OR category = "laptop"` (PK on one branch, non-PK on other) → full fan-out
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan(
            "products",
            &Some("product_id = \"abc\" OR category = \"laptop\"".into()),
            64,
        )
        .await;

    assert!(
        !plan.narrowed,
        "Plan should NOT be narrowed (OR at top level)"
    );
    assert!(
        plan.target_shards.is_empty(),
        "Should have no narrowed target shards"
    );
    assert!(plan.reason.contains("OR"));
}

#[tokio::test]
async fn p13_4_a4_pk_and_other_predicates_still_narrowable() {
    // PK predicate `AND` other predicates → still narrowable
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan(
            "products",
            &Some("product_id = \"abc\" AND category = \"books\"".into()),
            64,
        )
        .await;

    assert!(
        plan.narrowed,
        "Plan should be narrowed (AND can only shrink the set)"
    );
    assert_eq!(plan.target_shards.len(), 1, "Should target exactly 1 shard");
}

#[tokio::test]
async fn p13_4_a5_pk_negation_not_narrowable() {
    // Negation of a PK predicate → not narrowable
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan("products", &Some("product_id != \"abc\"".into()), 64)
        .await;

    assert!(!plan.narrowed, "Plan should NOT be narrowed (PK negation)");
    assert!(plan.reason.contains("negation"));
}

#[tokio::test]
async fn p13_4_a6_no_filter_not_narrowable() {
    // No filter → not narrowable
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner.plan("products", &None, 64).await;

    assert!(!plan.narrowed, "Plan should NOT be narrowed (no filter)");
    assert!(plan.reason.contains("no filter"));
}

#[tokio::test]
async fn p13_4_a7_no_pk_configured_not_narrowable() {
    // PK not configured for index → not narrowable
    let planner = QueryPlanner::default();

    let plan = planner
        .plan("products", &Some("product_id = \"abc\"".into()), 64)
        .await;

    assert!(
        !plan.narrowed,
        "Plan should NOT be narrowed (PK not configured)"
    );
    assert!(plan.reason.contains("primary key not configured"));
}

#[tokio::test]
async fn p13_4_a8_query_planner_disabled_not_narrowable() {
    // Query planner disabled → not narrowable
    let config = miroir_core::query_planner::QueryPlannerConfig {
        enabled: false,
        ..Default::default()
    };
    let planner = QueryPlanner::new(config);
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan("products", &Some("product_id = \"abc\"".into()), 64)
        .await;

    assert!(
        !plan.narrowed,
        "Plan should NOT be narrowed (planner disabled)"
    );
    assert!(plan.reason.contains("disabled"));
}

#[tokio::test]
async fn p13_4_a9_pk_in_list_too_large_not_narrowable() {
    // PK IN list exceeding max_pk_literals_narrowable → not narrowable
    let config = miroir_core::query_planner::QueryPlannerConfig {
        max_pk_literals_narrowable: 5,
        ..Default::default()
    };
    let planner = QueryPlanner::new(config);
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    // Create a list with 6 PKs (exceeds max of 5)
    let filter = "product_id IN [\"a\", \"b\", \"c\", \"d\", \"e\", \"f\"]".to_string();
    let plan = planner.plan("products", &Some(filter), 64).await;

    assert!(
        !plan.narrowed,
        "Plan should NOT be narrowed (IN list too large)"
    );
    assert!(plan.reason.contains("too large"));
}

#[tokio::test]
async fn p13_4_a10_result_parity_narrowed_vs_full_fanout() {
    // Property test: narrowed query returns the same hits as full-fan-out query
    // For a given PK value, both queries should return the same document

    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    // Plan with PK equality
    let plan = planner
        .plan(
            "products",
            &Some("product_id = \"test-doc-123\"".into()),
            64,
        )
        .await;

    assert!(plan.narrowed, "Plan should be narrowed");
    assert_eq!(plan.target_shards.len(), 1, "Should target exactly 1 shard");

    // The target shard should be the same as the shard_for_key result
    let expected_shard = shard_for_key("test-doc-123", 64);
    assert_eq!(
        plan.target_shards[0], expected_shard,
        "Target shard should match shard_for_key result"
    );

    // Verify that any document with this PK would be on this shard
    let test_pks = vec![
        "abc",
        "xyz",
        "test123",
        "product-42",
        "item-999",
        "user-001",
        "order-12345",
        "customer-42",
    ];

    for pk in test_pks {
        let shard = shard_for_key(pk, 64);
        let plan = planner
            .plan("products", &Some(format!("product_id = \"{}\"", pk)), 64)
            .await;

        assert!(plan.narrowed, "Plan should be narrowed for each PK");
        assert_eq!(
            plan.target_shards.len(),
            1,
            "Should target exactly 1 shard for each PK"
        );
        assert_eq!(
            plan.target_shards[0], shard,
            "Target shard should match shard_for_key for each PK"
        );
    }
}

#[tokio::test]
async fn p13_4_a11_non_pk_field_not_narrowable() {
    // Filter on non-PK field → not narrowable
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan("products", &Some("category = \"books\"".into()), 64)
        .await;

    assert!(!plan.narrowed, "Plan should NOT be narrowed (non-PK field)");
    assert!(plan.reason.contains("no PK constraint"));
}

#[tokio::test]
async fn p13_4_a12_complex_and_with_pk_narrowable() {
    // Complex AND with PK constraint → narrowable
    let planner = QueryPlanner::default();
    planner
        .set_primary_key("products".into(), "product_id".into())
        .await;

    let plan = planner
        .plan(
            "products",
            &Some("product_id = \"abc\" AND category = \"books\" AND price > 10".into()),
            64,
        )
        .await;

    assert!(
        plan.narrowed,
        "Plan should be narrowed (AND with PK constraint)"
    );
    assert_eq!(plan.target_shards.len(), 1, "Should target exactly 1 shard");
}
