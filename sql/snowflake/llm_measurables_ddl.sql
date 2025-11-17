-- ============================================================================
-- LLM Measurables System - Snowflake DDL (CORRECTED)
-- ============================================================================
-- Version: 1.0.1
-- Date: 2025-11-06
-- Description: Production schema for facility measurables queried via LLM
-- Database: MIKHAIL.LLM
-- Changes: Removed unsupported indexes, added CLUSTER BY, fixed MAINTAIN_IS_LATEST,
--          added ACCEPTANCE_RULE table, added REQUERY_MIN_DAYS

USE DATABASE MIKHAIL;
CREATE SCHEMA IF NOT EXISTS LLM;
USE SCHEMA MIKHAIL.LLM;

-- ============================================================================
-- Table: ACCEPTANCE_RULE
-- Description: Reusable acceptance criteria for measurables
-- ============================================================================
CREATE OR REPLACE TABLE ACCEPTANCE_RULE (
    RULE_ID STRING PRIMARY KEY,
    MIN_CONFIDENCE NUMBER(3,0) DEFAULT 60 COMMENT 'Minimum confidence score (0-100)',
    MAX_FRESHNESS_DAYS NUMBER(6,0) DEFAULT 180 COMMENT 'Maximum evidence age in days',
    REQUIRE_DATED_SOURCE BOOLEAN DEFAULT TRUE COMMENT 'Require at least one dated evidence source',
    STATUS_CHANGE_OVERRIDE BOOLEAN DEFAULT FALSE COMMENT 'Accept stale data if status changed',
    VALIDATION_FUNCTION STRING COMMENT 'Name of validation function (e.g., enum_status, production_numeric)',
    NOTES STRING COMMENT 'Description of this rule',
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Reusable acceptance criteria templates for measurables';

-- Seed core acceptance rules
INSERT INTO ACCEPTANCE_RULE (RULE_ID, MIN_CONFIDENCE, MAX_FRESHNESS_DAYS, REQUIRE_DATED_SOURCE, STATUS_CHANGE_OVERRIDE, VALIDATION_FUNCTION, NOTES)
VALUES
    ('core_status', 60, 365, FALSE, TRUE, 'enum_status', 'Operational status - allows stale if status changed'),
    ('production_reported', 70, 180, TRUE, FALSE, 'production_numeric', 'Last reported production - strict freshness'),
    ('production_current', 70, 90, TRUE, FALSE, 'production_numeric', 'Current run-rate - very strict freshness'),
    ('production_forecast', 60, 365, TRUE, FALSE, 'forecast_range', 'Production forecast - requires range'),
    ('production_ytd', 70, 180, TRUE, FALSE, 'production_numeric', 'Year-to-date production'),
    ('capacity', 70, 365, FALSE, FALSE, 'capacity_numeric', 'Nameplate capacity - can be historical'),
    ('maintenance_planned', 70, 60, TRUE, FALSE, 'maintenance_window', 'Planned maintenance - forward-looking'),
    ('maintenance_unplanned', 65, 90, TRUE, FALSE, 'incident_window', 'Unplanned downtime - recent only'),
    ('inputs_constraint', 65, 60, TRUE, FALSE, 'constraint_boolean', 'Input constraints - current state'),
    ('logistics_constraint', 65, 30, TRUE, FALSE, 'constraint_boolean', 'Logistics constraints - very current'),
    ('logistics_inventory', 65, 60, TRUE, FALSE, 'numeric_positive', 'Inventory days on hand'),
    ('incident_safety', 70, 90, TRUE, FALSE, 'incident_window', 'Safety incidents'),
    ('incident_environmental', 70, 180, TRUE, FALSE, 'incident_window', 'Environmental incidents'),
    ('regulatory_action', 70, 180, TRUE, FALSE, 'regulatory_event', 'Adverse regulatory actions'),
    ('regulatory_permit', 65, 180, TRUE, FALSE, 'permit_status', 'Permit renewal status'),
    ('project_expansion', 65, 180, TRUE, FALSE, 'project_status', 'Expansion project status'),
    ('status_restart', 65, 120, TRUE, FALSE, 'restart_timeline', 'Restart timeline for suspended facilities'),
    ('workforce_labor', 70, 30, TRUE, FALSE, 'labor_action', 'Labor actions and strikes'),
    ('power_tariff', 70, 180, TRUE, FALSE, 'tariff_change', 'Power tariff changes'),
    ('power_loadshedding', 65, 30, TRUE, FALSE, 'loadshedding_risk', 'Load-shedding risk'),
    ('power_outage', 65, 30, TRUE, FALSE, 'outage_event', 'Grid outages'),
    ('power_captive', 70, 365, FALSE, FALSE, 'numeric_positive', 'Captive generation capacity'),
    ('power_fuel', 65, 60, TRUE, FALSE, 'constraint_boolean', 'Fuel supply constraints');

-- ============================================================================
-- Table: MEASURABLE
-- Description: Master catalog of facility measurable questions
-- ============================================================================
CREATE OR REPLACE TABLE MEASURABLE (
    JSON_ID STRING PRIMARY KEY,
    PROMPT_TEMPLATE STRING NOT NULL COMMENT 'Prompt template with {VARIABLE} placeholders',
    UNIT_CANONICAL STRING COMMENT 'Canonical SI unit (t, tpm, tpa, MW, m3/d, etc.) NULL for non-numeric',
    PACK STRING NOT NULL COMMENT 'Pack name: core, power, feedstock, acid, climate, security, etc.',
    PRIORITY NUMBER(3,0) NOT NULL COMMENT 'Priority within pack (1=highest, 999=lowest)',
    APPLICABILITY_TAGS ARRAY COMMENT 'Array of namespace:value tags (facility:mine, process:sxew, metal:Cu, risk:power_intensive)',
    ACCEPTANCE_RULE_ID STRING NOT NULL REFERENCES ACCEPTANCE_RULE(RULE_ID) COMMENT 'FK to acceptance criteria',
    REQUERY_MIN_DAYS NUMBER(4,0) DEFAULT 7 COMMENT 'Minimum days between re-queries (cooldown)',
    VERSION STRING DEFAULT '1.0' COMMENT 'Schema version',
    ACTIVE BOOLEAN DEFAULT TRUE COMMENT 'If FALSE, exclude from routing',
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Master catalog of LLM-queryable facility measurables';

-- ============================================================================
-- Table: MEASURABLE_RESULT
-- Description: Time-series results from measurable queries
-- ============================================================================
CREATE OR REPLACE TABLE MEASURABLE_RESULT (
    RESULT_ID STRING DEFAULT UUID_STRING(),
    FACILITY_KEY STRING NOT NULL COMMENT 'FK to MIKHAIL.ENTITY.FACILITY(FACILITY_KEY) - doc only, enforced via audit',
    JSON_ID STRING NOT NULL REFERENCES MEASURABLE(JSON_ID),

    -- Value storage (canonical + raw)
    VALUE_CANONICAL VARIANT COMMENT 'Value in canonical units',
    UNIT_CANONICAL STRING COMMENT 'Canonical SI unit',
    VALUE_RAW STRING COMMENT 'Raw value string from LLM',
    RAW_UNIT STRING COMMENT 'Raw unit from LLM',

    -- Temporal metadata
    AS_OF_DATE DATE NOT NULL COMMENT 'Date as of which this value is valid',
    QUERY_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    -- Quality metadata
    CONFIDENCE NUMBER(3,0) CHECK (CONFIDENCE BETWEEN 0 AND 100),
    FRESHNESS_DAYS NUMBER(6,0),
    METHOD STRING COMMENT 'explicit|inferred',
    PROVISIONAL BOOLEAN DEFAULT FALSE,
    IS_LATEST BOOLEAN DEFAULT FALSE COMMENT 'Maintained by MAINTAIN_IS_LATEST()',

    -- Evidence & provenance (hash required per evidence item)
    EVIDENCE ARRAY COMMENT 'Array of {title, url, date, hash:SHA256(title|url|date)}',
    NOTES STRING,
    PROMPT_HASH STRING COMMENT 'SHA-256 of prompt',

    -- Execution metadata
    RUN_ID STRING NOT NULL,

    -- Acceptance
    ACCEPTED BOOLEAN NOT NULL DEFAULT FALSE,
    ACCEPTANCE_REASON STRING,
    SUPERSEDED_BY STRING,

    -- Validation
    VALIDATION_ERRORS ARRAY,

    CONSTRAINT PK_MEASURABLE_RESULT PRIMARY KEY (RESULT_ID),
    CONSTRAINT UQ_MEASURABLE_RESULT UNIQUE (FACILITY_KEY, JSON_ID, AS_OF_DATE)
) COMMENT = 'Time-series results from LLM queries'
CLUSTER BY (FACILITY_KEY, JSON_ID, AS_OF_DATE);

-- ============================================================================
-- Table: FACILITY_TIER_PLAN
-- Description: Per-facility tier assignment and assembled measurable lists
-- ============================================================================
CREATE OR REPLACE TABLE FACILITY_TIER_PLAN (
    RUN_ID STRING NOT NULL,
    FACILITY_KEY STRING NOT NULL,
    TIER NUMBER(1,0) CHECK (TIER IN (1, 2, 3, 4)),
    TARGET_COUNT NUMBER(6,0) NOT NULL COMMENT '5/50/100/500',
    PICKED_JSON_IDS ARRAY NOT NULL COMMENT 'Assembled JSON_IDs for this facility+run',
    PICKED_PACKS OBJECT COMMENT 'Pack breakdown e.g., {"core":20,"power":25,"acid":10}',
    SCHEDULED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_FACILITY_TIER_PLAN PRIMARY KEY (FACILITY_KEY, RUN_ID)
) COMMENT = 'Per-facility tier plans with pack breakdown';

-- ============================================================================
-- Table: MEASURABLE_RUN
-- Description: Batch run metadata and aggregate statistics
-- ============================================================================
CREATE OR REPLACE TABLE MEASURABLE_RUN (
    RUN_ID STRING PRIMARY KEY,
    STARTED_AT TIMESTAMP_TZ NOT NULL,
    ENDED_AT TIMESTAMP_TZ,
    FACILITIES_COUNT NUMBER,
    QUERIES_ATTEMPTED NUMBER,
    QUERIES_ACCEPTED NUMBER,
    QUERIES_PROVISIONAL NUMBER,
    QUERIES_REJECTED_SCHEMA NUMBER,
    AVG_FRESHNESS_DAYS NUMBER,
    COST_USD_EST NUMBER(12,4),
    PROVIDER STRING COMMENT 'perplexity|openai|anthropic',
    NOTES STRING,
    STATUS STRING COMMENT 'pending|running|completed|failed|cancelled',
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
) COMMENT = 'Batch run metadata';

-- ============================================================================
-- Table: MEASURABLE_QUERY_LOG
-- Description: Per-query execution logs
-- ============================================================================
CREATE OR REPLACE TABLE MEASURABLE_QUERY_LOG (
    LOG_ID STRING DEFAULT UUID_STRING(),
    RUN_ID STRING NOT NULL,
    FACILITY_KEY STRING NOT NULL,
    JSON_ID STRING NOT NULL,
    ATTEMPT NUMBER DEFAULT 1,
    PROVIDER STRING NOT NULL,
    PROMPT_HASH STRING,
    TOKENS_PROMPT NUMBER,
    TOKENS_COMPLETION NUMBER,
    COST_USD_EST NUMBER(10,4),
    LATENCY_MS NUMBER,
    STATUS STRING COMMENT 'accepted|provisional|rejected_schema|rejected_validation|error',
    ERROR_CODE STRING,
    ERROR_MESSAGE STRING,
    CREATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_MEASURABLE_QUERY_LOG PRIMARY KEY (LOG_ID)
) COMMENT = 'Per-query execution logs';

-- ============================================================================
-- Stored Procedure: MAINTAIN_IS_LATEST (CORRECTED with full tie-break)
-- Description: Updates IS_LATEST flags with proper tie-break logic
-- ============================================================================
CREATE OR REPLACE PROCEDURE MAINTAIN_IS_LATEST()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step 1: Clear all IS_LATEST flags
    UPDATE MEASURABLE_RESULT SET IS_LATEST = FALSE;

    -- Step 2: Set IS_LATEST for newest accepted result per (facility_key, json_id)
    -- Tie-break: AS_OF_DATE DESC, QUERY_TIMESTAMP DESC, CONFIDENCE DESC, RESULT_ID DESC
    UPDATE MEASURABLE_RESULT t
    SET IS_LATEST = TRUE
    WHERE RESULT_ID IN (
        SELECT RESULT_ID
        FROM (
            SELECT
                RESULT_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY FACILITY_KEY, JSON_ID
                    ORDER BY AS_OF_DATE DESC, QUERY_TIMESTAMP DESC, CONFIDENCE DESC, RESULT_ID DESC
                ) AS rn
            FROM MEASURABLE_RESULT
            WHERE ACCEPTED = TRUE
        )
        WHERE rn = 1
    );

    RETURN 'IS_LATEST flags updated successfully';
END;
$$;

-- ============================================================================
-- Views
-- ============================================================================
CREATE OR REPLACE VIEW MEASURABLE_RESULT_LATEST AS
SELECT
    FACILITY_KEY,
    JSON_ID,
    VALUE_CANONICAL,
    UNIT_CANONICAL,
    AS_OF_DATE,
    CONFIDENCE,
    FRESHNESS_DAYS,
    EVIDENCE,
    QUERY_TIMESTAMP,
    RESULT_ID
FROM MEASURABLE_RESULT
WHERE IS_LATEST = TRUE
  AND ACCEPTED = TRUE
COMMENT = 'Latest accepted result per facility + measurable';

-- ============================================================================
-- Audit Task (Nightly FK check - doc only, implement separately)
-- ============================================================================
-- Task: Check MEASURABLE_RESULT.FACILITY_KEY exists in MIKHAIL.ENTITY.FACILITY
-- SELECT DISTINCT r.FACILITY_KEY
-- FROM MIKHAIL.LLM.MEASURABLE_RESULT r
-- LEFT JOIN MIKHAIL.ENTITY.FACILITY f ON r.FACILITY_KEY = f.FACILITY_KEY
-- WHERE f.FACILITY_KEY IS NULL;
-- Flag/delete orphaned rows

COMMENT ON SCHEMA MIKHAIL.LLM IS 'LLM Measurables System v1.0.1 - Production schema for facility operational metrics';
