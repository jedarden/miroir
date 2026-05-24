#!/usr/bin/env python3
"""
Miroir SDK smoke test — Python
Tests: create index, add documents, search, update settings, delete index

Requirements:
    pip install meilisearch

Run against docker-compose-dev:
    MIROIR_URL=http://localhost:7700 MIROIR_MASTER_KEY=dev-key python3 python_smoke_test.py
"""

import os
import sys
import time
from meilisearch import Client

def main():
    url = os.getenv("MIROIR_URL", "http://localhost:7700")
    api_key = os.getenv("MIROIR_MASTER_KEY", "dev-key")

    print(f"=== Miroir Python SDK Smoke Test ===")
    print(f"Target: {url}")

    client = Client(url, api_key)
    index_name = "test_python_sdk"

    # Clean up any existing test index
    try:
        client.delete_index(index_name)
        print(f"✓ Cleaned up existing index '{index_name}'")
    except Exception:
        pass  # Index doesn't exist, that's fine

    # 1. Create index
    print("\n1. Creating index...")
    index = client.create_index(index_name, {"primaryKey": "id"})
    print(f"   ✓ Created index '{index_name}' with primary key 'id'")

    # Wait for index to be ready
    time.sleep(0.5)

    # 2. Add documents
    print("\n2. Adding documents...")
    documents = [
        {"id": 1, "title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "year": 1925},
        {"id": 2, "title": "To Kill a Mockingbird", "author": "Harper Lee", "year": 1960},
        {"id": 3, "title": "1984", "author": "George Orwell", "year": 1949},
    ]
    task = index.add_documents(documents)
    print(f"   ✓ Added {len(documents)} documents (task {task.task_uid})")

    # Wait for indexing
    time.sleep(1)

    # 3. Search
    print("\n3. Searching...")
    results = index.search("gatsby")
    print(f"   ✓ Found {len(results['hits'])} hits for 'gatsby'")
    assert len(results["hits"]) == 1
    assert results["hits"][0]["title"] == "The Great Gatsby"

    # 4. Update settings
    print("\n4. Updating settings...")
    index.update_settings({
        "searchableAttributes": ["title", "author"],
        "filterableAttributes": ["year"],
    })
    print("   ✓ Updated settings")

    # Wait for settings to propagate
    time.sleep(1)

    # 5. Delete index
    print("\n5. Deleting index...")
    client.delete_index(index_name)
    print(f"   ✓ Deleted index '{index_name}'")

    print("\n=== All Python SDK tests passed! ===")
    return 0

if __name__ == "__main__":
    sys.exit(main())
