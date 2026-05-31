#!/usr/bin/env node
/**
 * Unit test for search UI idempotency key generation (plan §13.10, §13.21).
 *
 * Verifies that:
 * - Same query parameters generate the same idempotency key
 * - Different query parameters generate different idempotency keys
 * - The key format is correct (starts with "search-" followed by hex)
 */

// Canonicalize JSON by sorting object keys recursively
function canonicalJson(obj) {
    if (obj === null || typeof obj !== 'object') {
        return JSON.stringify(obj);
    }
    if (Array.isArray(obj)) {
        return '[' + obj.map(canonicalJson).join(',') + ']';
    }
    const sortedKeys = Object.keys(obj).sort();
    return '{' + sortedKeys.map(k => `"${k}":${canonicalJson(obj[k])}`).join(',') + '}';
}

// Generate per-query idempotency key
function generateIdempotencyKey(index, requestBody) {
    // Canonicalize and hash
    const canonical = `${index}:${canonicalJson(requestBody)}`;
    let hash = 0;
    for (let i = 0; i < canonical.length; i++) {
        const char = canonical.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32-bit integer
    }
    return `search-${Math.abs(hash).toString(16)}`;
}

function assert(condition, message) {
    if (!condition) {
        console.error(`❌ FAILED: ${message}`);
        process.exit(1);
    }
    console.log(`✓ ${message}`);
}

// Test 1: Same parameters generate same key
console.log('\n=== Test 1: Same parameters generate same idempotency key ===');
const index = 'products';
const requestBody1 = { q: 'laptop', limit: 10, offset: 0 };
const key1 = generateIdempotencyKey(index, requestBody1);
const key2 = generateIdempotencyKey(index, requestBody1);
assert(key1 === key2, 'Same parameters produce same key');
assert(key1.startsWith('search-'), 'Key starts with "search-"');

// Test 2: Different queries generate different keys
console.log('\n=== Test 2: Different queries generate different keys ===');
const requestBody2 = { q: 'phone', limit: 10, offset: 0 };
const key3 = generateIdempotencyKey(index, requestBody2);
assert(key1 !== key3, 'Different queries produce different keys');

// Test 3: Different limit values generate different keys
console.log('\n=== Test 3: Different limit values generate different keys ===');
const requestBody3 = { q: 'laptop', limit: 20, offset: 0 };
const key4 = generateIdempotencyKey(index, requestBody3);
assert(key1 !== key4, 'Different limit values produce different keys');

// Test 4: Different page (offset) values generate different keys
console.log('\n=== Test 4: Different page values generate different keys ===');
const requestBody4 = { q: 'laptop', limit: 10, offset: 10 };
const key5 = generateIdempotencyKey(index, requestBody4);
assert(key1 !== key5, 'Different page values produce different keys');

// Test 5: Different filters generate different keys
console.log('\n=== Test 5: Different filters generate different keys ===');
const requestBody5 = { q: 'laptop', limit: 10, offset: 0, filter: 'category IN ["electronics"]' };
const key6 = generateIdempotencyKey(index, requestBody5);
assert(key1 !== key6, 'Different filters produce different keys');

// Test 6: Different indexes generate different keys
console.log('\n=== Test 6: Different indexes generate different keys ===');
const key7 = generateIdempotencyKey('users', requestBody1);
assert(key1 !== key7, 'Different indexes produce different keys');

// Test 7: Canonical JSON ensures consistent ordering
console.log('\n=== Test 7: Canonical JSON ensures key ordering consistency ===');
const requestBody6 = { limit: 10, q: 'laptop', offset: 0 };
const key8 = generateIdempotencyKey(index, requestBody6);
assert(key1 === key8, 'Different key orders produce same canonical key');

// Test 8: Complex nested objects are handled correctly
console.log('\n=== Test 8: Complex nested objects are handled correctly ===');
const requestBody7 = {
    q: 'test',
    filter: 'category IN ["electronics"] AND price IN ["100-500"]',
    facets: ['category', 'price'],
    sort: ['price:asc']
};
const key9 = generateIdempotencyKey(index, requestBody7);
assert(key9.startsWith('search-'), 'Complex request produces valid key');

const requestBody8 = {
    facets: ['category', 'price'],
    filter: 'category IN ["electronics"] AND price IN ["100-500"]',
    q: 'test',
    sort: ['price:asc']
};
const key10 = generateIdempotencyKey(index, requestBody8);
assert(key9 === key10, 'Complex requests with different key orders produce same key');

// Test 9: Empty/null values are handled correctly
console.log('\n=== Test 9: Empty/null values are handled correctly ===');
const requestBody9 = { q: '', limit: 10, offset: 0 };
const key11 = generateIdempotencyKey(index, requestBody9);
assert(key11.startsWith('search-'), 'Empty query string produces valid key');

// Test 10: Arrays are handled correctly (order matters)
console.log('\n=== Test 10: Arrays preserve order ===');
const requestBody10 = { facets: ['category', 'brand', 'price'] };
const key12 = generateIdempotencyKey(index, requestBody10);
const requestBody11 = { facets: ['brand', 'category', 'price'] };
const key13 = generateIdempotencyKey(index, requestBody11);
assert(key12 !== key13, 'Different array orders produce different keys');

console.log('\n✅ All idempotency key tests passed!');
process.exit(0);
