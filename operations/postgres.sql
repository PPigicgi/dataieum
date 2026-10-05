CREATE TABLE IF NOT EXISTS publications (
  id text PRIMARY KEY,
  state text NOT NULL CHECK (state IN ('copying','verifying','verified','active','retired')),
  details jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS one_active_publication ON publications(state) WHERE state='active';
CREATE TABLE IF NOT EXISTS sources (
  id text PRIMARY KEY, info jsonb NOT NULL, last_sync text, last_error text NOT NULL
);
CREATE TABLE IF NOT EXISTS topics (
  id text PRIMARY KEY, main_id text NOT NULL, main_name text NOT NULL, name text NOT NULL, definition text NOT NULL
);
CREATE TABLE IF NOT EXISTS datasets (
  ordinal bigint NOT NULL UNIQUE,
  id text PRIMARY KEY,
  source_id text NOT NULL REFERENCES sources(id),
  title text NOT NULL,
  description text NOT NULL,
  metadata jsonb NOT NULL,
  original_mappings jsonb NOT NULL,
  fingerprint text NOT NULL,
  checked_at text,
  reference_years integer[] NOT NULL,
  classification jsonb NOT NULL
);
-- Current inferred relationships and their provenance stay with their dataset.
-- This avoids splitting millions of tiny properties into separate tables.
CREATE INDEX IF NOT EXISTS datasets_source_page ON datasets(source_id,ordinal);
CREATE INDEX IF NOT EXISTS datasets_band_page ON datasets((classification->>'band'),ordinal)
  WHERE classification->>'band' IS NOT NULL;
CREATE INDEX IF NOT EXISTS datasets_topic_page ON datasets((classification->>'primary_topic'),ordinal)
  WHERE classification->>'primary_topic' IS NOT NULL;
CREATE INDEX IF NOT EXISTS datasets_main_page ON datasets((split_part(classification->>'primary_topic','-',1)),ordinal)
  WHERE classification->>'primary_topic' IS NOT NULL;
CREATE INDEX IF NOT EXISTS datasets_years ON datasets USING gin(reference_years);
GRANT USAGE ON SCHEMA public TO wanted_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO wanted_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO wanted_reader;
