#!/usr/bin/env python3
"""
Test values.schema.json validation constraints.

Tests the SQLite + multiple replicas rejection rule:
- replicas: 1 + sqlite -> PASS
- replicas: 2 + sqlite -> FAIL
- replicas: 2 + redis -> PASS

Usage:
  python3 test_schema.py              # Run tests
  helm lint --strict -f tests/replicas-2-sqlite.yaml .  # Run with helm
"""

import json
import sys
from pathlib import Path


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def evaluate_condition(instance, if_cond):
    """Evaluate a JSON Schema if condition against an instance."""
    if "properties" in if_cond:
        for prop_path, schema in if_cond["properties"].items():
            parts = prop_path.split(".")
            value = instance
            for part in parts:
                if not isinstance(value, dict):
                    return False
                if part not in value:
                    return False
                value = value[part]

            # Check the constraint
            if "const" in schema:
                if value != schema["const"]:
                    return False
            elif "minimum" in schema:
                if not isinstance(value, (int, float)):
                    return False
                if value < schema["minimum"]:
                    return False
            elif "type" in schema:
                if schema["type"] == "boolean":
                    if value != schema.get(True, False):
                        return False

    if "required" in if_cond:
        for req in if_cond["required"]:
            if req not in instance:
                return False

    return True


def validate_schema(schema: dict, instance: dict, path: str = "") -> list:
    """Validate instance against schema, return list of errors."""
    errors = []

    # Check allOf constraints
    if "allOf" in schema:
        for constraint in schema["allOf"]:
            if "if" in constraint and "then" in constraint:
                if evaluate_condition(instance, constraint["if"]):
                    # The 'if' condition is true, check 'then' constraint
                    then_schema = constraint["then"]

                    # Check nested properties in 'then'
                    if "properties" in then_schema:
                        for prop, prop_schema in then_schema["properties"].items():
                            # Handle direct property constraints (e.g., taskStore.backend)
                            if prop in instance:
                                if "properties" in prop_schema:
                                    for nested, nested_schema in prop_schema["properties"].items():
                                        if nested in instance[prop]:
                                            actual = instance[prop][nested]
                                            if "const" in nested_schema:
                                                if actual != nested_schema["const"]:
                                                    msg = constraint.get("errorMessage",
                                                        f"{path}{prop}.{nested}: expected {nested_schema['const']}, got {actual}")
                                                    errors.append(msg)

                                # Handle minimum constraints (e.g., replicas minimum)
                                if "minimum" in prop_schema:
                                    if instance[prop] < prop_schema["minimum"]:
                                        msg = constraint.get("errorMessage",
                                            f"{path}{prop}: must be at least {prop_schema['minimum']}")
                                        errors.append(msg)

                            # Handle nested object constraints (e.g., search_ui.rate_limit.backend)
                            if "properties" in prop_schema:
                                for nested, nested_schema in prop_schema["properties"].items():
                                    if "properties" in nested_schema:
                                        for double_nested, double_nested_schema in nested_schema["properties"].items():
                                            if "const" in double_nested_schema:
                                                # Check if the nested path exists in instance
                                                if prop in instance and isinstance(instance[prop], dict):
                                                    if nested in instance[prop] and isinstance(instance[prop][nested], dict):
                                                        if double_nested in instance[prop][nested]:
                                                            actual = instance[prop][nested][double_nested]
                                                            if actual != double_nested_schema["const"]:
                                                                msg = constraint.get("errorMessage",
                                                                    f"{path}{prop}.{nested}.{double_nested}: expected {double_nested_schema['const']}, got {actual}")
                                                                errors.append(msg)

                    # Check required fields in 'then'
                    if "required" in then_schema:
                        for req in then_schema["required"]:
                            if req not in instance:
                                errors.append(f"{path}{req} is required")

    return errors


def test_schema_constraints():
    chart_dir = Path(__file__).parent.parent
    schema_path = chart_dir / "values.schema.json"
    tests_dir = Path(__file__).parent

    schema = load_json(schema_path)

    test_cases = [
        # (replicas, backend, should_pass, description)
        (1, "sqlite", True, "replicas: 1 + sqlite should PASS"),
        (2, "sqlite", False, "replicas: 2 + sqlite should FAIL"),
        (2, "redis", True, "replicas: 2 + redis should PASS"),
    ]

    passed = 0
    failed = 0

    for replicas, backend, should_pass, description in test_cases:
        instance = {
            "replicas": replicas,
            "taskStore": {"backend": backend}
        }

        miroir_schema = schema["properties"]["miroir"]
        errors = validate_schema(miroir_schema, instance)

        is_valid = len(errors) == 0

        if is_valid == should_pass:
            print(f"✓ {description}")
            passed += 1
        else:
            print(f"✗ {description}")
            for err in errors:
                print(f"  Error: {err}")
            failed += 1

    # Test search_ui.rate_limit.backend constraint
    search_ui_tests = [
        # (replicas, rate_limit_backend, should_pass, description)
        (1, "local", True, "replicas: 1 + search_ui.rate_limit.backend: local should PASS"),
        (2, "local", False, "replicas: 2 + search_ui.rate_limit.backend: local should FAIL"),
        (2, "redis", True, "replicas: 2 + search_ui.rate_limit.backend: redis should PASS"),
    ]

    for replicas, rate_limit_backend, should_pass, description in search_ui_tests:
        instance = {
            "replicas": replicas,
            "taskStore": {"backend": "redis"},
            "search_ui": {
                "rate_limit": {"backend": rate_limit_backend}
            }
        }

        miroir_schema = schema["properties"]["miroir"]
        errors = validate_schema(miroir_schema, instance)

        is_valid = len(errors) == 0

        if is_valid == should_pass:
            print(f"✓ {description}")
            passed += 1
        else:
            print(f"✗ {description}")
            for err in errors:
                print(f"  Error: {err}")
            failed += 1

    # Test admin_ui.rate_limit.backend constraint
    admin_ui_tests = [
        # (replicas, rate_limit_backend, should_pass, description)
        (1, "local", True, "replicas: 1 + admin_ui.rate_limit.backend: local should PASS"),
        (2, "local", False, "replicas: 2 + admin_ui.rate_limit.backend: local should FAIL"),
        (2, "redis", True, "replicas: 2 + admin_ui.rate_limit.backend: redis should PASS"),
    ]

    for replicas, rate_limit_backend, should_pass, description in admin_ui_tests:
        instance = {
            "replicas": replicas,
            "taskStore": {"backend": "redis"},
            "admin_ui": {
                "rate_limit": {"backend": rate_limit_backend}
            }
        }

        miroir_schema = schema["properties"]["miroir"]
        errors = validate_schema(miroir_schema, instance)

        is_valid = len(errors) == 0

        if is_valid == should_pass:
            print(f"✓ {description}")
            passed += 1
        else:
            print(f"✗ {description}")
            for err in errors:
                print(f"  Error: {err}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    return failed == 0


if __name__ == "__main__":
    success = test_schema_constraints()
    sys.exit(0 if success else 1)
