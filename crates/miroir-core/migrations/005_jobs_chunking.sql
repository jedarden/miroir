-- Migration 005: Mode C chunked job support
-- Adds fields for chunked background jobs (plan §14.5 Mode C)
-- Large jobs are split into chunks by the first pod that picks them up
-- Each chunk is an independent job with a parent reference

-- Add chunking fields to jobs table
ALTER TABLE jobs ADD COLUMN parent_job_id TEXT;
ALTER TABLE jobs ADD COLUMN chunk_index INTEGER;
ALTER TABLE jobs ADD COLUMN total_chunks INTEGER;

-- Index for listing all chunks of a parent job
CREATE INDEX IF NOT EXISTS jobs_parent ON jobs(parent_job_id);

-- Index for expired claims (used by job reclamation)
CREATE INDEX IF NOT EXISTS jobs_claim_expires ON jobs(claim_expires_at);

-- Add created_at column for job cleanup
ALTER TABLE jobs ADD COLUMN created_at INTEGER;

-- Index for job cleanup (by created timestamp)
CREATE INDEX IF NOT EXISTS jobs_created_at ON jobs(created_at);
