-- ============================================================================
-- LLM Measurables DDL Patches
-- ============================================================================
-- Apply these after main DDL to add constraints and make seeding idempotent

USE DATABASE MIKHAIL;
USE SCHEMA LLM;

-- ============================================================================
-- Patch 1: Add CHECK constraint on REQUERY_MIN_DAYS
-- ============================================================================
ALTER TABLE MEASURABLE ADD CONSTRAINT CHK_REQUERY_MIN_DAYS CHECK (REQUERY_MIN_DAYS >= 0);

-- ============================================================================
-- Patch 2: Make ACCEPTANCE_RULE seeding idempotent (MERGE instead of INSERT)
-- ============================================================================
MERGE INTO ACCEPTANCE_RULE AS target
USING (
    SELECT 'core_status' AS RULE_ID, 60 AS MIN_CONFIDENCE, 365 AS MAX_FRESHNESS_DAYS, FALSE AS REQUIRE_DATED_SOURCE, TRUE AS STATUS_CHANGE_OVERRIDE, 'enum_status' AS VALIDATION_FUNCTION, 'Operational status - allows stale if status changed' AS NOTES UNION ALL
    SELECT 'production_reported', 70, 180, TRUE, FALSE, 'production_numeric', 'Last reported production - strict freshness' UNION ALL
    SELECT 'production_current', 70, 90, TRUE, FALSE, 'production_numeric', 'Current run-rate - very strict freshness' UNION ALL
    SELECT 'production_forecast', 60, 365, TRUE, FALSE, 'forecast_range', 'Production forecast - requires range' UNION ALL
    SELECT 'production_ytd', 70, 180, TRUE, FALSE, 'production_numeric', 'Year-to-date production' UNION ALL
    SELECT 'capacity', 70, 365, FALSE, FALSE, 'capacity_numeric', 'Nameplate capacity - can be historical' UNION ALL
    SELECT 'maintenance_planned', 70, 60, TRUE, FALSE, 'maintenance_window', 'Planned maintenance - forward-looking' UNION ALL
    SELECT 'maintenance_unplanned', 65, 90, TRUE, FALSE, 'incident_window', 'Unplanned downtime - recent only' UNION ALL
    SELECT 'inputs_constraint', 65, 60, TRUE, FALSE, 'constraint_boolean', 'Input constraints - current state' UNION ALL
    SELECT 'logistics_constraint', 65, 30, TRUE, FALSE, 'constraint_boolean', 'Logistics constraints - very current' UNION ALL
    SELECT 'logistics_inventory', 65, 60, TRUE, FALSE, 'numeric_positive', 'Inventory days on hand' UNION ALL
    SELECT 'incident_safety', 70, 90, TRUE, FALSE, 'incident_window', 'Safety incidents' UNION ALL
    SELECT 'incident_environmental', 70, 180, TRUE, FALSE, 'incident_window', 'Environmental incidents' UNION ALL
    SELECT 'regulatory_action', 70, 180, TRUE, FALSE, 'regulatory_event', 'Adverse regulatory actions' UNION ALL
    SELECT 'regulatory_permit', 65, 180, TRUE, FALSE, 'permit_status', 'Permit renewal status' UNION ALL
    SELECT 'project_expansion', 65, 180, TRUE, FALSE, 'project_status', 'Expansion project status' UNION ALL
    SELECT 'status_restart', 65, 120, TRUE, FALSE, 'restart_timeline', 'Restart timeline for suspended facilities' UNION ALL
    SELECT 'workforce_labor', 70, 30, TRUE, FALSE, 'labor_action', 'Labor actions and strikes' UNION ALL
    SELECT 'power_tariff', 70, 180, TRUE, FALSE, 'tariff_change', 'Power tariff changes' UNION ALL
    SELECT 'power_loadshedding', 65, 30, TRUE, FALSE, 'loadshedding_risk', 'Load-shedding risk' UNION ALL
    SELECT 'power_outage', 65, 30, TRUE, FALSE, 'outage_event', 'Grid outages' UNION ALL
    SELECT 'power_captive', 70, 365, FALSE, FALSE, 'numeric_positive', 'Captive generation capacity' UNION ALL
    SELECT 'power_fuel', 65, 60, TRUE, FALSE, 'constraint_boolean', 'Fuel supply constraints'
) AS source
ON target.RULE_ID = source.RULE_ID
WHEN MATCHED THEN UPDATE SET
    MIN_CONFIDENCE = source.MIN_CONFIDENCE,
    MAX_FRESHNESS_DAYS = source.MAX_FRESHNESS_DAYS,
    REQUIRE_DATED_SOURCE = source.REQUIRE_DATED_SOURCE,
    STATUS_CHANGE_OVERRIDE = source.STATUS_CHANGE_OVERRIDE,
    VALIDATION_FUNCTION = source.VALIDATION_FUNCTION,
    NOTES = source.NOTES
WHEN NOT MATCHED THEN INSERT (
    RULE_ID, MIN_CONFIDENCE, MAX_FRESHNESS_DAYS, REQUIRE_DATED_SOURCE,
    STATUS_CHANGE_OVERRIDE, VALIDATION_FUNCTION, NOTES
) VALUES (
    source.RULE_ID, source.MIN_CONFIDENCE, source.MAX_FRESHNESS_DAYS, source.REQUIRE_DATED_SOURCE,
    source.STATUS_CHANGE_OVERRIDE, source.VALIDATION_FUNCTION, source.NOTES
);

-- ============================================================================
-- Patch 3: Add MEASURABLE_ACTIVE view (filters ACTIVE = TRUE)
-- ============================================================================
CREATE OR REPLACE VIEW MEASURABLE_ACTIVE AS
SELECT *
FROM MEASURABLE
WHERE ACTIVE = TRUE
COMMENT = 'Active measurables only - use for routing';
