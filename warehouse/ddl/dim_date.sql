-- ==============================================================================
-- Table: marts.dim_date
-- Description: Standardized calendar dimension for role-playing date joins.
-- Grain: Exactly one row per calendar day.
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.dim_date CASCADE;

CREATE TABLE marts.dim_date (
    date_key            INTEGER NOT NULL,                     -- Surrogate integer key: YYYYMMDD (e.g. 20260926)
    full_date           DATE NOT NULL,                        -- Actual calendar date
    day_of_week         INTEGER NOT NULL,                     -- 1 (Monday) to 7 (Sunday)
    day_name            VARCHAR(16) NOT NULL,                 -- Monday, Tuesday, etc.
    day_of_month        INTEGER NOT NULL,                     -- 1 to 31
    day_of_year         INTEGER NOT NULL,                     -- 1 to 366
    week_of_year        INTEGER NOT NULL,                     -- ISO week (1 to 53)
    month_number        INTEGER NOT NULL,                     -- 1 to 12
    month_name          VARCHAR(16) NOT NULL,                 -- January, February, etc.
    calendar_quarter    INTEGER NOT NULL,                     -- 1 to 4
    calendar_year       INTEGER NOT NULL,                     -- e.g. 2026
    is_weekend          BOOLEAN NOT NULL,                     -- TRUE for Saturday & Sunday
    fiscal_year         INTEGER NOT NULL,                     -- Company fiscal year
    fiscal_quarter      INTEGER NOT NULL,                     -- Company fiscal quarter (1 to 4)

    CONSTRAINT pk_dim_date PRIMARY KEY (date_key),
    CONSTRAINT uq_dim_date_full_date UNIQUE (full_date),
    CONSTRAINT chk_dim_date_quarter CHECK (calendar_quarter BETWEEN 1 AND 4),
    CONSTRAINT chk_dim_date_month CHECK (month_number BETWEEN 1 AND 12)
);

-- Comments on table and attributes
COMMENT ON TABLE marts.dim_date IS 'Role-playing calendar dimension table providing chronological hierarchy for analytical filtering and rollups.';
COMMENT ON COLUMN marts.dim_date.date_key IS 'Surrogate integer key formatted as YYYYMMDD.';
COMMENT ON COLUMN marts.dim_date.is_weekend IS 'Boolean flag indicating Saturday or Sunday for cohort filtering.';

-- Index on calendar attributes frequently used in WHERE / GROUP BY clauses
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON marts.dim_date (calendar_year, month_number);
CREATE INDEX IF NOT EXISTS idx_dim_date_quarter ON marts.dim_date (calendar_year, calendar_quarter);
