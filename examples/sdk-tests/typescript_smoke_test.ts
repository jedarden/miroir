#!/usr/bin/env ts-node
/**
 * Miroir SDK smoke test — TypeScript
 * Tests: create index, add documents, search, update settings, delete index
 *
 * Requirements:
 *   npm install meilisearch
 *   npm install -D ts-node @types/node
 *
 * Run against docker-compose-dev:
 *   MIROIR_URL=http://localhost:7700 MIROIR_MASTER_KEY=dev-key npx ts-node typescript_smoke_test.ts
 */

import { Index, MeiliSearch } from 'meilisearch';

interface Document {
  id: number;
  title: string;
  author: string;
  year: number;
}

const url = process.env.MIROIR_URL || 'http://localhost:7700';
const apiKey = process.env.MIROIR_MASTER_KEY || 'dev-key';
const indexName = 'test_typescript_sdk';

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main(): Promise<number> {
  console.log('=== Miroir TypeScript SDK Smoke Test ===');
  console.log(`Target: ${url}`);

  const client = new MeiliSearch({ host: url, apiKey });

  // Clean up any existing test index
  try {
    await client.deleteIndex(indexName);
    console.log(`✓ Cleaned up existing index '${indexName}'`);
  } catch (e) {
    // Index doesn't exist, that's fine
  }

  // 1. Create index
  console.log('\n1. Creating index...');
  const index: Index<Document> = await client.createIndex(indexName, { primaryKey: 'id' });
  console.log(`   ✓ Created index '${indexName}' with primary key 'id'`);

  await sleep(500);

  // 2. Add documents
  console.log('\n2. Adding documents...');
  const documents: Document[] = [
    { id: 1, title: 'The Great Gatsby', author: 'F. Scott Fitzgerald', year: 1925 },
    { id: 2, title: 'To Kill a Mockingbird', author: 'Harper Lee', year: 1960 },
    { id: 3, title: '1984', author: 'George Orwell', year: 1949 },
  ];
  const task = await index.addDocuments(documents);
  console.log(`   ✓ Added ${documents.length} documents (task ${task.taskUid})`);

  await sleep(1000);

  // 3. Search
  console.log('\n3. Searching...');
  const results = await index.search('gatsby');
  console.log(`   ✓ Found ${results.hits.length} hits for 'gatsby'`);

  if (results.hits.length !== 1) {
    throw new Error(`Expected 1 hit, got ${results.hits.length}`);
  }

  const hit = results.hits[0] as Document;
  if (hit.title !== 'The Great Gatsby') {
    throw new Error(`Expected 'The Great Gatsby', got '${hit.title}'`);
  }

  // 4. Update settings
  console.log('\n4. Updating settings...');
  await index.updateSettings({
    searchableAttributes: ['title', 'author'],
    filterableAttributes: ['year'],
  });
  console.log('   ✓ Updated settings');

  await sleep(1000);

  // 5. Delete index
  console.log('\n5. Deleting index...');
  await client.deleteIndex(indexName);
  console.log(`   ✓ Deleted index '${indexName}'`);

  console.log('\n=== All TypeScript SDK tests passed! ===');
  return 0;
}

main().catch(err => {
  console.error('Error:', err);
  process.exit(1);
});
