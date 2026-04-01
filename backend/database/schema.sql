-- ── Break Ledger (41 unified columns + audit) ──────────────────────────────────
CREATE SEQUENCE IF NOT EXISTS breaks_id_seq;

CREATE TABLE IF NOT EXISTS breaks (
    id                          BIGINT DEFAULT nextval('breaks_id_seq') PRIMARY KEY,
    source_system               VARCHAR,
    asset_class                 VARCHAR,
    rec_name                    VARCHAR,
    rec_id                      VARCHAR,
    trade_ref                   VARCHAR,
    break_type                  VARCHAR,
    break_value                 DOUBLE,
    break_ccy                   VARCHAR,
    abs_gbp                     DOUBLE,
    fx_rate                     DOUBLE,
    age_days                    INTEGER,
    age_bucket                  VARCHAR,
    day_of_month                INTEGER,
    period                      VARCHAR,
    report_date                 DATE,
    jira_ref                    VARCHAR,
    jira_desc                   VARCHAR,
    issue_category              VARCHAR,
    issue_category_2            VARCHAR,
    jira_priority               VARCHAR,
    epic                        VARCHAR,
    system_to_be_fixed          VARCHAR,
    fix_required                BOOLEAN,
    ml_risk_score               DOUBLE,
    thematic                    VARCHAR,
    type_of_issue               VARCHAR,
    recurring_break_flag        BOOLEAN,
    cross_platform_match        BOOLEAN,
    historical_match_confidence DOUBLE,
    action                      VARCHAR,
    bs_cert_ready               BOOLEAN,
    threshold_breach            BOOLEAN,
    material_flag               BOOLEAN,
    escalation_flag             VARCHAR,
    sla_breach                  BOOLEAN,
    days_to_sla                 INTEGER,
    emir_flag                   BOOLEAN,
    d3s_asset_class             VARCHAR,
    first_seen_date             DATE,
    last_seen_date              DATE,
    status                      VARCHAR,
    load_ts                     TIMESTAMP DEFAULT current_timestamp
);

CREATE INDEX IF NOT EXISTS idx_breaks_source   ON breaks(source_system);
CREATE INDEX IF NOT EXISTS idx_breaks_rec      ON breaks(rec_id);
CREATE INDEX IF NOT EXISTS idx_breaks_asset    ON breaks(asset_class);
CREATE INDEX IF NOT EXISTS idx_breaks_material ON breaks(material_flag);
CREATE INDEX IF NOT EXISTS idx_breaks_escalation ON breaks(escalation_flag);
CREATE INDEX IF NOT EXISTS idx_breaks_trade_ref ON breaks(trade_ref);
CREATE INDEX IF NOT EXISTS idx_breaks_report_date ON breaks(report_date);

-- ── Jira Tickets ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS jira_tickets (
    jira_ref          VARCHAR PRIMARY KEY,
    trade_ref         VARCHAR NOT NULL,
    source_system     VARCHAR,
    rec_id            VARCHAR,
    epic              VARCHAR,
    status            VARCHAR,
    summary           VARCHAR,
    assignee_team     VARCHAR,
    created_date      DATE,
    resolved_date     DATE,
    tags              VARCHAR,
    break_value_gbp   DOUBLE,
    asset_class       VARCHAR,
    is_draft          BOOLEAN DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_jira_trade ON jira_tickets(trade_ref, source_system);
CREATE INDEX IF NOT EXISTS idx_jira_status ON jira_tickets(status);
CREATE INDEX IF NOT EXISTS idx_jira_epic   ON jira_tickets(epic);

-- ── Rec Configuration Cache ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS rec_configs (
    rec_id              VARCHAR PRIMARY KEY,
    rec_name            VARCHAR,
    source_platform     VARCHAR,
    asset_class         VARCHAR,
    d3s_asset_class     VARCHAR,
    threshold_type      VARCHAR,
    threshold_pct       DOUBLE,
    threshold_abs_gbp   DOUBLE,
    escalation_sla_days INTEGER,
    jira_epic           VARCHAR,
    emir_flag           BOOLEAN,
    self_correct_days   INTEGER,
    ml_model_id         VARCHAR,
    active              BOOLEAN DEFAULT TRUE
);

-- ── FX Rates ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fx_rates (
    ccy_pair  VARCHAR,
    rate_date DATE,
    rate      DOUBLE,
    PRIMARY KEY (ccy_pair, rate_date)
);

-- ── Upload Audit Log ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS upload_log (
    upload_id       VARCHAR PRIMARY KEY,
    filename        VARCHAR,
    file_type       VARCHAR,
    source_detected VARCHAR,
    rows_received   INTEGER,
    rows_loaded     INTEGER,
    errors          INTEGER DEFAULT 0,
    upload_ts       TIMESTAMP DEFAULT current_timestamp,
    status          VARCHAR
);

-- ── ML Model Training Log ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS model_training_log (
    run_id          VARCHAR PRIMARY KEY,
    run_ts          TIMESTAMP DEFAULT current_timestamp,
    asset_class     VARCHAR,
    n_train         INTEGER,
    accuracy        DOUBLE,
    precision_score DOUBLE,
    recall_score    DOUBLE,
    model_path      VARCHAR,
    status          VARCHAR
);
