-- ==============================================================================
-- Warehouse Initialization Script
-- Executed on container creation to establish clean layered schemas
-- ==============================================================================

-- 1. Create layered pipeline schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- 2. Schema comments describing architectural boundaries
COMMENT ON SCHEMA raw IS 'Raw landing layer for unmodified source extracts';
COMMENT ON SCHEMA staging IS 'Staging layer for typed, cleaned, and normalized models via dbt';
COMMENT ON SCHEMA marts IS 'Serving layer with star schema dimensional models (facts and dimensions)';

-- 3. Grant schema permissions to pipeline user
GRANT ALL ON SCHEMA raw TO CURRENT_USER;
GRANT ALL ON SCHEMA staging TO CURRENT_USER;
GRANT ALL ON SCHEMA marts TO CURRENT_USER;
