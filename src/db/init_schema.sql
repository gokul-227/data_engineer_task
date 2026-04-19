-- ============================================================================
-- Corporate Credit Rating Data Warehouse - Schema Initialization
-- Dimensional model with temporal tracking (star schema)
-- ============================================================================



-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- dim_company: SCD Type 2 - tracks company name/identity changes over time
CREATE TABLE IF NOT EXISTS dim_company (
    company_key     SERIAL PRIMARY KEY,
    company_id      VARCHAR(100) NOT NULL,      -- Natural key (stable identifier)
    company_name    VARCHAR(255) NOT NULL,
    valid_from      TIMESTAMP NOT NULL DEFAULT NOW(),
    valid_to        TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current      BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_company_id ON dim_company(company_id);
CREATE INDEX IF NOT EXISTS idx_dim_company_current ON dim_company(company_id, is_current) WHERE is_current = TRUE;

-- dim_sector: Corporate sector classification
CREATE TABLE IF NOT EXISTS dim_sector (
    sector_key      SERIAL PRIMARY KEY,
    sector_name     VARCHAR(255) NOT NULL UNIQUE,
    created_at      TIMESTAMP DEFAULT NOW()
);

-- dim_country: Country of origin
CREATE TABLE IF NOT EXISTS dim_country (
    country_key     SERIAL PRIMARY KEY,
    country_name    VARCHAR(255) NOT NULL UNIQUE,
    country_code    VARCHAR(10),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- dim_currency: Reporting currency
CREATE TABLE IF NOT EXISTS dim_currency (
    currency_key    SERIAL PRIMARY KEY,
    currency_code   VARCHAR(10) NOT NULL UNIQUE,
    currency_name   VARCHAR(100),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- dim_rating_methodology: Rating methodology applied
CREATE TABLE IF NOT EXISTS dim_rating_methodology (
    methodology_key     SERIAL PRIMARY KEY,
    methodology_name    VARCHAR(500) NOT NULL UNIQUE,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- FILE UPLOAD / AUDIT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS file_upload (
    upload_id           SERIAL PRIMARY KEY,
    filename            VARCHAR(255) NOT NULL,
    file_path           VARCHAR(500),
    file_hash           VARCHAR(64) NOT NULL,
    file_size_bytes     BIGINT,
    upload_timestamp    TIMESTAMP DEFAULT NOW(),
    processing_status   VARCHAR(20) DEFAULT 'pending'
                        CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    records_extracted   INTEGER DEFAULT 0,
    error_message       TEXT,
    processing_duration_ms  INTEGER,
    created_at          TIMESTAMP DEFAULT NOW(),
    UNIQUE(file_hash)       -- Idempotency: prevent duplicate file processing
);

CREATE INDEX IF NOT EXISTS idx_file_upload_status ON file_upload(processing_status);
CREATE INDEX IF NOT EXISTS idx_file_upload_filename ON file_upload(filename);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- fact_company_snapshot: Immutable record of each file extraction
-- Every upload creates a new snapshot row — no overwrites, full audit trail
CREATE TABLE IF NOT EXISTS fact_company_snapshot (
    snapshot_id         SERIAL PRIMARY KEY,
    
    -- Dimension foreign keys
    company_key         INTEGER NOT NULL REFERENCES dim_company(company_key),
    sector_key          INTEGER REFERENCES dim_sector(sector_key),
    country_key         INTEGER REFERENCES dim_country(country_key),
    currency_key        INTEGER REFERENCES dim_currency(currency_key),
    
    -- Upload lineage
    upload_id           INTEGER NOT NULL REFERENCES file_upload(upload_id),
    
    -- Version tracking: version_number is per company_id, incremented on each upload
    version_number      INTEGER NOT NULL,
    
    -- Rating methodologies (can be multiple per file)
    rating_methodologies    TEXT[],              -- Array of methodology names
    
    -- Industry risk data (supports multi-industry weighting)
    industry_risks          JSONB,              -- [{industry, score, weight}, ...]
    segmentation_criteria   VARCHAR(255),
    
    -- Business info
    accounting_principles   VARCHAR(50),
    year_end_month          VARCHAR(20),
    
    -- Business risk profile ratings
    business_risk_profile           VARCHAR(20),
    blended_industry_risk_profile   VARCHAR(20),
    competitive_positioning         VARCHAR(20),
    market_share                    VARCHAR(20),
    diversification                 VARCHAR(20),
    operating_profitability         VARCHAR(20),
    sector_specific_factor_1        VARCHAR(20),
    sector_specific_factor_2        VARCHAR(20),
    
    -- Financial risk profile ratings
    financial_risk_profile  VARCHAR(20),
    leverage                VARCHAR(20),
    interest_cover          VARCHAR(20),
    cash_flow_cover         VARCHAR(20),
    liquidity               VARCHAR(30),
    
    -- Credit metrics time-series (JSONB for flexible year columns)
    -- Format: {metric_name: {year: value, ...}, ...}
    credit_metrics          JSONB,
    
    -- Temporal tracking
    snapshot_date           TIMESTAMP NOT NULL DEFAULT NOW(),
    effective_from          TIMESTAMP NOT NULL DEFAULT NOW(),
    
    -- Data quality
    quality_score           FLOAT,              -- Overall quality score (0-1)
    quality_issues          JSONB,              -- Array of issues found
    
    created_at              TIMESTAMP DEFAULT NOW(),
    
    -- Prevent duplicate snapshots from same file
    UNIQUE(upload_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_company ON fact_company_snapshot(company_key);
CREATE INDEX IF NOT EXISTS idx_snapshot_date ON fact_company_snapshot(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshot_version ON fact_company_snapshot(company_key, version_number);
CREATE INDEX IF NOT EXISTS idx_snapshot_sector ON fact_company_snapshot(sector_key);
CREATE INDEX IF NOT EXISTS idx_snapshot_country ON fact_company_snapshot(country_key);
CREATE INDEX IF NOT EXISTS idx_snapshot_currency ON fact_company_snapshot(currency_key);

-- ============================================================================
-- PIPELINE STATE MANAGEMENT
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_run (
    run_id              VARCHAR(36) PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
    started_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMP,
    status              VARCHAR(20) NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed', 'partial')),
    files_found         INTEGER DEFAULT 0,
    files_processed     INTEGER DEFAULT 0,
    files_skipped       INTEGER DEFAULT 0,
    files_failed        INTEGER DEFAULT 0,
    records_inserted    INTEGER DEFAULT 0,
    execution_time_ms   INTEGER,
    quality_summary     JSONB,
    error_details       JSONB,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- DATA QUALITY REPORT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_report (
    report_id           SERIAL PRIMARY KEY,
    run_id              VARCHAR(36) REFERENCES pipeline_run(run_id),
    upload_id           INTEGER REFERENCES file_upload(upload_id),
    filename            VARCHAR(255) NOT NULL,
    completeness_pct    FLOAT,          -- % of required fields present
    validity_pct        FLOAT,          -- % of values passing validation
    total_fields        INTEGER,
    missing_fields      INTEGER,
    invalid_fields      INTEGER,
    warnings            JSONB,          -- Array of warning messages
    errors              JSONB,          -- Array of error messages
    field_details       JSONB,          -- Per-field quality info
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================================================
-- VIEWS FOR BI-FRIENDLY ACCESS
-- ============================================================================

-- Flattened view joining all dimensions for easy BI consumption
CREATE OR REPLACE VIEW vw_company_snapshot_detail AS
SELECT
    s.snapshot_id,
    s.version_number,
    c.company_id,
    c.company_name,
    sec.sector_name,
    co.country_name,
    cur.currency_code,
    s.rating_methodologies,
    s.industry_risks,
    s.segmentation_criteria,
    s.accounting_principles,
    s.year_end_month,
    s.business_risk_profile,
    s.blended_industry_risk_profile,
    s.competitive_positioning,
    s.market_share,
    s.diversification,
    s.operating_profitability,
    s.sector_specific_factor_1,
    s.sector_specific_factor_2,
    s.financial_risk_profile,
    s.leverage,
    s.interest_cover,
    s.cash_flow_cover,
    s.liquidity,
    s.credit_metrics,
    s.quality_score,
    s.snapshot_date,
    s.effective_from,
    f.filename AS source_file,
    f.upload_timestamp,
    f.file_hash
FROM fact_company_snapshot s
JOIN dim_company c ON s.company_key = c.company_key AND c.is_current = TRUE
LEFT JOIN dim_sector sec ON s.sector_key = sec.sector_key
LEFT JOIN dim_country co ON s.country_key = co.country_key
LEFT JOIN dim_currency cur ON s.currency_key = cur.currency_key
LEFT JOIN file_upload f ON s.upload_id = f.upload_id;

-- Latest snapshot per company
CREATE OR REPLACE VIEW vw_latest_company_snapshot AS
SELECT DISTINCT ON (c.company_id)
    s.snapshot_id,
    s.version_number,
    c.company_id,
    c.company_name,
    sec.sector_name,
    co.country_name,
    cur.currency_code,
    s.rating_methodologies,
    s.industry_risks,
    s.business_risk_profile,
    s.financial_risk_profile,
    s.snapshot_date,
    f.filename AS source_file
FROM fact_company_snapshot s
JOIN dim_company c ON s.company_key = c.company_key AND c.is_current = TRUE
LEFT JOIN dim_sector sec ON s.sector_key = sec.sector_key
LEFT JOIN dim_country co ON s.country_key = co.country_key
LEFT JOIN dim_currency cur ON s.currency_key = cur.currency_key
LEFT JOIN file_upload f ON s.upload_id = f.upload_id
ORDER BY c.company_id, s.version_number DESC;
