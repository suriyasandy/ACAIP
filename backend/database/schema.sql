-- ── Break Ledger ─────────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS breaks_id_seq;

CREATE TABLE IF NOT EXISTS breaks (
    id                BIGINT DEFAULT nextval('breaks_id_seq') PRIMARY KEY,
    trade_id          VARCHAR NOT NULL,
    product           VARCHAR,           -- CEQ / OTC / LnD / MTN
    source_system     VARCHAR,           -- D3S (scalable to others)
    rec_id            VARCHAR,           -- e.g. D3S-REC-001
    as_at_date        DATE,
    break_date        DATE,
    break_amount      DOUBLE,
    s_ccy             VARCHAR,           -- source currency
    m_ccy             VARCHAR,           -- mirror currency
    -- Computed columns
    age_days          INTEGER,           -- AS_AT_DATE - BREAK_DATE
    age_bucket        VARCHAR,           -- 0-2d | 3-7d | 8-30d | 30d+
    ccy_mismatch_flag BOOLEAN,           -- S_CCY != M_CCY
    recurring_flag    BOOLEAN DEFAULT FALSE,
    stale_flag        BOOLEAN,           -- age_days > 7
    -- Phase 2: KDB FX enrichment (populated later)
    gbp_amount        DOUBLE,
    fx_rate           DOUBLE,
    -- Provenance
    source_file       VARCHAR,
    file_date         DATE,
    load_ts           TIMESTAMP DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_breaks_product   ON breaks(product);
CREATE INDEX IF NOT EXISTS idx_breaks_file_date ON breaks(file_date);
CREATE INDEX IF NOT EXISTS idx_breaks_rec       ON breaks(rec_id);
CREATE INDEX IF NOT EXISTS idx_breaks_trade     ON breaks(trade_id);
CREATE INDEX IF NOT EXISTS idx_breaks_stale     ON breaks(stale_flag);

-- ── Validation Errors ─────────────────────────────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS val_id_seq;

CREATE TABLE IF NOT EXISTS validation_errors (
    id            BIGINT DEFAULT nextval('val_id_seq') PRIMARY KEY,
    upload_id     VARCHAR,
    source_file   VARCHAR,
    source_system VARCHAR,
    rec_id        VARCHAR,
    file_date     DATE,
    row_number    INTEGER,
    trade_id      VARCHAR,
    error_type    VARCHAR,   -- e.g. CCY_MISMATCH, MISSING_TRADE_ID
    error_detail  VARCHAR,
    severity      VARCHAR,   -- ERROR | WARNING
    load_ts       TIMESTAMP DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_val_upload    ON validation_errors(upload_id);
CREATE INDEX IF NOT EXISTS idx_val_severity  ON validation_errors(severity);
CREATE INDEX IF NOT EXISTS idx_val_type      ON validation_errors(error_type);

-- ── Upload Audit Log ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS upload_log (
    upload_id     VARCHAR PRIMARY KEY,
    filename      VARCHAR,
    source_system VARCHAR,
    rec_id        VARCHAR,
    product       VARCHAR,
    file_date     DATE,
    rows_received INTEGER,
    rows_loaded   INTEGER,
    error_count   INTEGER DEFAULT 0,
    warning_count INTEGER DEFAULT 0,
    upload_ts     TIMESTAMP DEFAULT current_timestamp,
    status        VARCHAR,
    file_hash     VARCHAR    -- SHA-256 for idempotency guard
);
