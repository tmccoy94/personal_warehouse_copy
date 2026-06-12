CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INT PRIMARY KEY,
    full_date     DATE NOT NULL,
    year          SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_id      INT NOT NULL,
    day           SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_holiday    BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_branch (
    branch_key           SERIAL PRIMARY KEY,
    source_system        TEXT DEFAULT 'keystone',
    source_branch_serial SMALLINT,
    branch_name          TEXT NOT NULL UNIQUE,
    abbreviation         VARCHAR(10) NOT NULL UNIQUE,
    keystone_code        TEXT,
    street               TEXT,
    city                 TEXT,
    state                CHAR(2),
    zip                  CHAR(5),
    time_zone            TEXT,
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (source_system, source_branch_serial)
);

CREATE TABLE IF NOT EXISTS person_identity (
    person_key  BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS person_source_identifier (
    person_key          BIGINT NOT NULL REFERENCES person_identity(person_key),
    source_system       TEXT NOT NULL,
    source_entity_type  TEXT NOT NULL,
    source_identifier   TEXT NOT NULL,
    is_primary          BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from          TIMESTAMPTZ,
    valid_to            TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_system, source_entity_type, source_identifier),
    UNIQUE (person_key, source_system, source_entity_type)
);

CREATE TABLE IF NOT EXISTS staging.person_snapshot_stage (
    person_serial        BIGINT PRIMARY KEY,
    first_name           TEXT,
    middle_name          TEXT,
    last_name            TEXT,
    birth_date           DATE,
    age                  SMALLINT,
    gender               TEXT,
    status               TEXT,
    cred_score           SMALLINT,
    score_date           DATE,
    branch_serial        SMALLINT,
    home_branch_serial   SMALLINT,
    account_eligibility  TEXT,
    account_type         TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ── Raw CORE mirrors — current-state, truncate/reload daily ──────────────────
-- These live in the raw schema (shared across dev/test/prod environments).
-- No snapshot_date column — historical data is embedded in the source date
-- fields (close_date, charge_off_date, etc.) on each record.

CREATE TABLE IF NOT EXISTS staging.ks_person (
    person_serial   BIGINT PRIMARY KEY,
    first_name      TEXT,
    middle_name     TEXT,
    last_name       TEXT,
    birth_date      DATE,
    gender          TEXT,
    person_type     TEXT,    -- CORE.PERSON_TYPE.DESCRIPTION — used to exclude Vendors from member counts
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.ks_ac_note (
    ac_note_serial  BIGINT PRIMARY KEY,
    person_serial   BIGINT NOT NULL,
    note_type       TEXT,    -- CORE.NOTE_TYPE.DESCRIPTION — e.g. 'Deceased Member'
    expiration_date DATE,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_ac_note_person ON staging.ks_ac_note (person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_share (
    share_serial      BIGINT PRIMARY KEY,
    person_serial     BIGINT NOT NULL,
    account_serial    BIGINT NOT NULL,
    share_id          CHAR(4),
    type_serial       INT,
    share_type        TEXT,
    branch_serial     SMALLINT,
    open_date                 DATE,
    close_date                DATE,
    charge_off_date           DATE,
    ach_deposit_last_amount   NUMERIC(14,2),
    ach_deposit_last_date     DATE,
    loaded_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_share_person    ON staging.ks_share (person_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_share_open_date ON staging.ks_share (open_date);

CREATE TABLE IF NOT EXISTS staging.ks_account (
    account_serial              BIGINT PRIMARY KEY,
    primary_person_serial       BIGINT,
    open_date                   DATE,
    close_date                  DATE,
    dormancy_restriction        TEXT,
    relationship_serial         INT,
    eligibility_serial          INT,
    type_serial                 INT,
    eligibility_description     TEXT,
    type_description            TEXT,
    relationship_description    TEXT,
    close_reason_description    TEXT,    -- CORE.ACCOUNT_CLOSE_REASON.DESCRIPTION — used in members_lost reporting
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_account_person ON staging.ks_account (primary_person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_home_branch (
    person_serial   BIGINT PRIMARY KEY,
    branch_serial   SMALLINT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.ks_person_score (
    person_serial   BIGINT PRIMARY KEY,
    credit_score    SMALLINT,
    score_date      DATE,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.ks_login (
    login_serial    BIGINT PRIMARY KEY,
    person_serial   BIGINT,
    channel         TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_login_person ON staging.ks_login (person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_transaction (
    transaction_serial   BIGINT PRIMARY KEY,
    person_serial        BIGINT,
    posting_date         DATE,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_transaction_person ON staging.ks_transaction (person_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_transaction_date   ON staging.ks_transaction (posting_date);

CREATE TABLE IF NOT EXISTS staging.ks_monetary (
    monetary_serial      BIGINT PRIMARY KEY,
    transaction_serial   BIGINT,
    posting_date         DATE,
    category             CHAR(1),        -- D=Deposit W=Withdrawal P=Payment A=Advance etc.
    source               TEXT,           -- B=Bill payment a=ACH P=Payroll H=Online etc.
    target_category      CHAR(1),        -- S=Share L=Loan
    target_serial        BIGINT,
    principal            NUMERIC(14,2),
    interest             NUMERIC(14,2),
    new_balance          NUMERIC(14,2),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_monetary_transaction ON staging.ks_monetary (transaction_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_monetary_transaction ON staging.ks_monetary (transaction_serial);

-- ENV posting date mirror (single row — the DB2 system's current business date)
-- Used in date-window queries (direct deposit last 30 days, bill pay last 30 days,
-- deceased note expiration) so our warehouse queries match DB2 exactly.
CREATE TABLE IF NOT EXISTS staging.ks_env (
    posting_date   DATE NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Loan raw mirrors

CREATE TABLE IF NOT EXISTS staging.ks_loan (
    loan_serial                     BIGINT PRIMARY KEY,
    account_serial                  BIGINT NOT NULL,
    person_serial                   BIGINT NOT NULL,
    open_date                       DATE,
    close_date                      DATE,
    charge_off_date                 DATE,
    maturity_date                   DATE,
    branch_serial                   SMALLINT,    -- CORE.LOAN.BRANCH_SERIAL — the disbursing branch (fully populated, unlike LOAN_REQUEST)
    loan_type_description           TEXT,
    balance                         NUMERIC(14,2),
    interest_rate                   NUMERIC(8,5),
    payment_count_dq_under_30       SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_30_to_59       SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_60_to_89       SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_90_to_119      SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_120_and_up     SMALLINT NOT NULL DEFAULT 0,
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_person    ON staging.ks_loan (person_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_open_date ON staging.ks_loan (open_date);

CREATE TABLE IF NOT EXISTS staging.ks_loan_request (
    loan_request_serial     BIGINT PRIMARY KEY,
    app_serial              BIGINT,
    type_serial             INT,
    loan_type               TEXT,
    branch_serial           SMALLINT,
    credit_limit            NUMERIC(14,2),
    credit_score            SMALLINT,
    processor_user_serial   INT,
    approved_loan_serial    BIGINT,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_request_app       ON staging.ks_loan_request (app_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_request_approved  ON staging.ks_loan_request (approved_loan_serial);

CREATE TABLE IF NOT EXISTS staging.ks_app (
    app_serial              BIGINT PRIMARY KEY,
    application_number      TEXT,
    application_date        DATE,
    app_type                TEXT,
    withdrawn_indicator     CHAR(1),
    processor_user_serial   INT,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_app_date ON staging.ks_app (application_date);

CREATE TABLE IF NOT EXISTS staging.ks_applicant (
    applicant_serial    BIGINT PRIMARY KEY,
    app_serial          BIGINT NOT NULL,
    person_serial       BIGINT,
    ordinal             INT,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_applicant_app    ON staging.ks_applicant (app_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_applicant_person ON staging.ks_applicant (person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_applicant_score (
    applicant_score_serial  BIGINT PRIMARY KEY,
    applicant_serial        BIGINT NOT NULL,
    score                   SMALLINT,
    score_date              DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_applicant_score_applicant ON staging.ks_applicant_score (applicant_serial);

CREATE TABLE IF NOT EXISTS staging.ks_decision (
    decision_serial             BIGINT PRIMARY KEY,
    loan_request_serial         BIGINT NOT NULL,
    type_serial                 INT,
    decision_date               DATE,
    decision_maker_user_serial  INT,
    decision_category_code      CHAR(1),
    decision_category           TEXT,
    decision_description        TEXT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_decision_loan_request ON staging.ks_decision (loan_request_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_decision_date         ON staging.ks_decision (decision_date);

-- ── Monthly CORE mirrors — point-in-time, truncate/reload per monthly partition ────
-- Loaded from the frozen MONTH_END_x snapshot DB by ks_monthly_loaders assets.
-- Schemas mirror the daily staging tables. Indexes use _monthly suffix to avoid name
-- collisions. These exist because daily staging is sourced from LIVE — for monthly
-- partitions to be accurate against the prior-month-end snapshot, they need their own
-- copy of the source data that hasn't been refreshed past the partition's EOM.

CREATE TABLE IF NOT EXISTS staging.ks_person_monthly (
    person_serial   BIGINT PRIMARY KEY,
    first_name      TEXT,
    middle_name     TEXT,
    last_name       TEXT,
    birth_date      DATE,
    gender          TEXT,
    person_type     TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.ks_ac_note_monthly (
    ac_note_serial  BIGINT PRIMARY KEY,
    person_serial   BIGINT NOT NULL,
    note_type       TEXT,
    expiration_date DATE,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_ac_note_monthly_person ON staging.ks_ac_note_monthly (person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_share_monthly (
    share_serial      BIGINT PRIMARY KEY,
    person_serial     BIGINT NOT NULL,
    account_serial    BIGINT NOT NULL,
    share_id          CHAR(4),
    type_serial       INT,
    share_type        TEXT,
    branch_serial     SMALLINT,
    open_date                 DATE,
    close_date                DATE,
    charge_off_date           DATE,
    ach_deposit_last_amount   NUMERIC(14,2),
    ach_deposit_last_date     DATE,
    loaded_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_share_monthly_person    ON staging.ks_share_monthly (person_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_share_monthly_open_date ON staging.ks_share_monthly (open_date);

CREATE TABLE IF NOT EXISTS staging.ks_account_monthly (
    account_serial              BIGINT PRIMARY KEY,
    primary_person_serial       BIGINT,
    open_date                   DATE,
    close_date                  DATE,
    dormancy_restriction        TEXT,
    relationship_serial         INT,
    eligibility_serial          INT,
    type_serial                 INT,
    eligibility_description     TEXT,
    type_description            TEXT,
    relationship_description    TEXT,
    close_reason_description    TEXT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_account_monthly_person ON staging.ks_account_monthly (primary_person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_login_monthly (
    login_serial    BIGINT PRIMARY KEY,
    person_serial   BIGINT,
    channel         TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_login_monthly_person ON staging.ks_login_monthly (person_serial);

-- ks_transaction_monthly / ks_monetary_monthly are loaded by COPYING from the daily
-- 90-day rolling tables (filtered to the partition window) instead of pulling the full
-- ~20-min CORE.MONETARY tables from DB2 again. Loader fails loudly if the daily window
-- doesn't cover the required range.

CREATE TABLE IF NOT EXISTS staging.ks_transaction_monthly (
    transaction_serial   BIGINT PRIMARY KEY,
    person_serial        BIGINT,
    posting_date         DATE,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_transaction_monthly_person ON staging.ks_transaction_monthly (person_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_transaction_monthly_date   ON staging.ks_transaction_monthly (posting_date);

CREATE TABLE IF NOT EXISTS staging.ks_monetary_monthly (
    monetary_serial      BIGINT PRIMARY KEY,
    transaction_serial   BIGINT,
    posting_date         DATE,
    category             CHAR(1),
    source               TEXT,
    target_category      CHAR(1),
    target_serial        BIGINT,
    principal            NUMERIC(14,2),
    interest             NUMERIC(14,2),
    new_balance          NUMERIC(14,2),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_monetary_monthly_transaction ON staging.ks_monetary_monthly (transaction_serial);

CREATE TABLE IF NOT EXISTS staging.ks_env_monthly (
    posting_date   DATE NOT NULL,
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.ks_loan_monthly (
    loan_serial                     BIGINT PRIMARY KEY,
    account_serial                  BIGINT NOT NULL,
    person_serial                   BIGINT NOT NULL,
    open_date                       DATE,
    close_date                      DATE,
    charge_off_date                 DATE,
    maturity_date                   DATE,
    branch_serial                   SMALLINT,
    loan_type_description           TEXT,
    balance                         NUMERIC(14,2),
    interest_rate                   NUMERIC(8,5),
    payment_count_dq_under_30       SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_30_to_59       SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_60_to_89       SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_90_to_119      SMALLINT NOT NULL DEFAULT 0,
    payment_count_dq_120_and_up     SMALLINT NOT NULL DEFAULT 0,
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_monthly_person    ON staging.ks_loan_monthly (person_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_monthly_open_date ON staging.ks_loan_monthly (open_date);

CREATE TABLE IF NOT EXISTS staging.ks_loan_request_monthly (
    loan_request_serial     BIGINT PRIMARY KEY,
    app_serial              BIGINT,
    type_serial             INT,
    loan_type               TEXT,
    branch_serial           SMALLINT,
    credit_limit            NUMERIC(14,2),
    credit_score            SMALLINT,
    processor_user_serial   INT,
    approved_loan_serial    BIGINT,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_request_monthly_app      ON staging.ks_loan_request_monthly (app_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_loan_request_monthly_approved ON staging.ks_loan_request_monthly (approved_loan_serial);

CREATE TABLE IF NOT EXISTS staging.ks_app_monthly (
    app_serial              BIGINT PRIMARY KEY,
    application_number      TEXT,
    application_date        DATE,
    app_type                TEXT,
    withdrawn_indicator     CHAR(1),
    processor_user_serial   INT,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_app_monthly_date ON staging.ks_app_monthly (application_date);

CREATE TABLE IF NOT EXISTS staging.ks_applicant_monthly (
    applicant_serial    BIGINT PRIMARY KEY,
    app_serial          BIGINT NOT NULL,
    person_serial       BIGINT,
    ordinal             INT,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_applicant_monthly_app    ON staging.ks_applicant_monthly (app_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_applicant_monthly_person ON staging.ks_applicant_monthly (person_serial);

CREATE TABLE IF NOT EXISTS staging.ks_applicant_score_monthly (
    applicant_score_serial  BIGINT PRIMARY KEY,
    applicant_serial        BIGINT NOT NULL,
    score                   SMALLINT,
    score_date              DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_applicant_score_monthly_applicant ON staging.ks_applicant_score_monthly (applicant_serial);

CREATE TABLE IF NOT EXISTS staging.ks_decision_monthly (
    decision_serial             BIGINT PRIMARY KEY,
    loan_request_serial         BIGINT NOT NULL,
    type_serial                 INT,
    decision_date               DATE,
    decision_maker_user_serial  INT,
    decision_category_code      CHAR(1),
    decision_category           TEXT,
    decision_description        TEXT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_staging_ks_decision_monthly_loan_request ON staging.ks_decision_monthly (loan_request_serial);
CREATE INDEX IF NOT EXISTS idx_staging_ks_decision_monthly_date         ON staging.ks_decision_monthly (decision_date);

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS person_snapshot_daily (
    person_key           BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    first_name           TEXT,
    middle_name          TEXT,
    last_name            TEXT,
    birth_date           DATE,
    gender               TEXT,
    batch_id             TEXT,
    record_hash          TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS person_snapshot_monthly (
    person_key           BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    first_name           TEXT,
    middle_name          TEXT,
    last_name            TEXT,
    birth_date           DATE,
    gender               TEXT,
    batch_id             TEXT,
    record_hash          TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS person_contact (
    person_contact_key  BIGSERIAL,
    person_key          BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date       DATE NOT NULL,
    source_system       TEXT NOT NULL,
    source_identifier   TEXT NOT NULL,
    contact_type        TEXT NOT NULL,
    contact_value       TEXT,
    is_primary          BOOLEAN,
    is_active           BOOLEAN,
    last_verified_date  DATE,
    valid_to            DATE,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_contact_key)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS ks_products_snapshot (
    person_key           BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    product_name         TEXT NOT NULL,
    product_count        SMALLINT NOT NULL DEFAULT 1,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key, product_name),
    UNIQUE (snapshot_date, source_system, source_person_serial, product_name)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS ks_relationships_snapshot (
    person_key           BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    status               TEXT,
    account_eligibility  TEXT,
    account_type         TEXT,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS ks_credit_score_snapshot (
    person_key           BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    credit_score         SMALLINT,
    credit_score_date    DATE,
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS ks_member_branch_snapshot (
    person_key           BIGINT NOT NULL REFERENCES person_identity(person_key),
    snapshot_date        DATE NOT NULL,
    source_system        TEXT NOT NULL DEFAULT 'keystone',
    source_person_serial BIGINT NOT NULL,
    branch_key           INT REFERENCES dim_branch(branch_key),
    home_branch_key      INT REFERENCES dim_branch(branch_key),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, person_key),
    UNIQUE (snapshot_date, source_system, source_person_serial)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS dim_goals (
    goal_key        SERIAL PRIMARY KEY,
    metric_name     TEXT NOT NULL,
    fiscal_year     SMALLINT NOT NULL,
    threshold_type  VARCHAR(20) NOT NULL,
    value           NUMERIC(14,4) NOT NULL,
    display_label   TEXT,
    UNIQUE (metric_name, fiscal_year, threshold_type)
);

CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key            SERIAL PRIMARY KEY,
    source_system           TEXT NOT NULL DEFAULT 'keystone',
    source_employee_serial  INT NOT NULL,
    teller_user             VARCHAR(40) NOT NULL,
    full_name               TEXT NOT NULL,
    role_descriptions       TEXT,
    termination_date        DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_employee_serial),
    UNIQUE (source_system, teller_user)
);

-- CU-total summary — one row per month, no branch dimension.
-- ks_members  = OP Branch count (OP is the only KS branch; CORE calls it "Overland Park Branch").
-- mo_members  = total_members − ks_members.
-- hb_ks/mo    = home-branch ZIP-territory counts (slightly different population than opening-branch).
-- net_new_*   = current − prior month; NULL when no prior row exists.
CREATE TABLE IF NOT EXISTS fct_members_summary (
    snapshot_date               DATE PRIMARY KEY,
    total_members               INT,
    ks_members                  INT,
    mo_members                  INT,
    active_members              INT,
    members_18_50               INT,
    hb_ks_members               INT,
    hb_mo_members               INT,
    members_lost                INT,
    members_added               INT,
    non_kcpd_members            INT,
    net_new_members             INT,
    net_new_ks_members          INT,
    net_new_mo_members          INT,
    net_new_active_members      INT,
    net_new_members_18_50       INT,
    net_new_hb_mo               INT,
    net_new_hb_ks               INT,
    net_new_non_kcpd_members    INT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Home-branch breakdown — one row per real branch per month (no CU Total, no Unassigned).
-- CU-level home branch totals (hb_ks_members, hb_mo_members) live in fct_members_summary.
CREATE TABLE IF NOT EXISTS fct_home_branch_members_summary (
    snapshot_date   DATE        NOT NULL,
    branch_key      INT         NOT NULL REFERENCES dim_branch(branch_key),
    total_members   INT,
    members_added   INT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, branch_key)
);

-- Members added in a month. event_date = first account/share open date.
CREATE TABLE IF NOT EXISTS fct_members_added (
    event_id                SERIAL PRIMARY KEY,
    source_system           TEXT NOT NULL DEFAULT 'keystone',
    month_id                INT NOT NULL,
    source_person_serial    BIGINT NOT NULL,
    person_key              BIGINT REFERENCES person_identity(person_key),
    event_date              DATE NOT NULL,
    agency                  TEXT,
    account_category        TEXT,
    returning_member        BOOLEAN,
    first_gensh_opened      DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_person_serial, month_id)
);

-- Members lost in a month. event_date = last General Share close date.
CREATE TABLE IF NOT EXISTS fct_members_lost (
    event_id                SERIAL PRIMARY KEY,
    source_system           TEXT NOT NULL DEFAULT 'keystone',
    month_id                INT NOT NULL,
    source_person_serial    BIGINT NOT NULL,
    person_key              BIGINT REFERENCES person_identity(person_key),
    event_date              DATE NOT NULL,
    close_reason            TEXT,
    membership_duration_yrs SMALLINT,
    first_gensh_opened      DATE,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_person_serial, month_id)
);

CREATE TABLE IF NOT EXISTS fct_financial_ratios_summary (
    snapshot_date               DATE PRIMARY KEY,
    net_worth_pct               NUMERIC(8,4),
    roa_pct                     NUMERIC(8,4),
    op_expense_pct              NUMERIC(8,4),
    loan_to_share_pct           NUMERIC(8,4),
    loan_delinquency_pct        NUMERIC(8,4),
    loan_charge_off_ratio       NUMERIC(8,4),
    loan_growth_pct             NUMERIC(8,4),
    atm_member_uses             INT,
    atm_foreign_uses            INT,
    atm_network_ytd_net_income  NUMERIC(14,2),
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fct_nps_summary (
    snapshot_date   DATE PRIMARY KEY,
    nps_score       NUMERIC(8,2),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fct_loan_portfolio_summary (
    snapshot_date               DATE PRIMARY KEY,
    loan_total_balance          NUMERIC(14,2),
    loan_income_monthly         NUMERIC(14,2),
    loan_growth_ytd_pct         NUMERIC(8,4),
    share_total_balance         NUMERIC(14,2),
    share_income_monthly        NUMERIC(14,2),
    share_growth_ytd_pct        NUMERIC(8,4),
    cc_total_balance            NUMERIC(14,2),
    cc_income_monthly           NUMERIC(14,2),
    cc_growth_ytd_pct           NUMERIC(8,4),
    loan_charge_offs            NUMERIC(14,2),
    loan_charge_off_recoveries  NUMERIC(14,2),
    cp_charge_offs              NUMERIC(14,2),
    cp_charge_off_recoveries    NUMERIC(14,2),
    atm_fraud_mtd               NUMERIC(14,2),
    check_loss_fraud_mtd        NUMERIC(14,2),
    cc_fraud_mtd                NUMERIC(14,2),
    debit_fraud_mtd             NUMERIC(14,2),
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Loans opened per branch per month (by LOAN.OPEN_DATE), from 2020-01-01 forward.
-- Fully derived from staging.ks_loan by ks_fct_loans_by_branch_summary_table
-- (truncate/reload). The CSV backfill is superseded and no longer populates this table.
--   loan_count            — total = payday + non_payday
--   payday_loan_count     — loan_type = 'Payday Alternative Loan'
--   non_payday_loan_count — all other loan types
CREATE TABLE IF NOT EXISTS fct_loans_by_branch_summary (
    snapshot_date         DATE NOT NULL,
    branch_key            INT NOT NULL REFERENCES dim_branch(branch_key),
    loan_count            INT,
    payday_loan_count     INT,
    non_payday_loan_count INT,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, branch_key)
);

CREATE TABLE IF NOT EXISTS fct_loan_applications (
    application_key                 BIGSERIAL PRIMARY KEY,
    source_system                   TEXT NOT NULL DEFAULT 'keystone',
    source_app_serial               BIGINT NOT NULL,
    application_number              TEXT NOT NULL,
    application_date                DATE NOT NULL,
    app_type                        TEXT,
    withdrawn_status                VARCHAR(20),
    source_primary_person_serial    BIGINT,
    primary_person_key              BIGINT REFERENCES person_identity(person_key),
    primary_credit_score            SMALLINT,
    source_co_applicant_person_serial BIGINT,
    co_applicant_person_key         BIGINT REFERENCES person_identity(person_key),
    co_applicant_name               TEXT,
    co_applicant_credit_score       SMALLINT,
    app_processor_key               INT REFERENCES dim_employee(employee_key),
    branch_key                      INT REFERENCES dim_branch(branch_key),
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_app_serial),
    UNIQUE (source_system, application_number)
);

CREATE TABLE IF NOT EXISTS fct_loan_requests (
    loan_request_key                BIGSERIAL PRIMARY KEY,
    source_system                   TEXT NOT NULL DEFAULT 'keystone',
    source_loan_request_serial      BIGINT NOT NULL,
    application_key                 BIGINT NOT NULL REFERENCES fct_loan_applications(application_key),
    application_number              TEXT NOT NULL,
    loan_type                       TEXT,
    requested_amount                NUMERIC(14,2),
    loan_request_credit_score       SMALLINT,
    branch_key                      INT REFERENCES dim_branch(branch_key),
    decision_category               VARCHAR(50),
    decision_description            TEXT,
    decision_date                   DATE,
    decision_maker_key              INT REFERENCES dim_employee(employee_key),
    processor_key                   INT REFERENCES dim_employee(employee_key),
    source_approved_loan_serial     BIGINT,
    loan_open_date                  DATE,
    is_payday_alt                   BOOLEAN,
    loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_system, source_loan_request_serial)
);

CREATE TABLE IF NOT EXISTS fct_loans (
    loan_key                    BIGSERIAL NOT NULL,
    snapshot_date               DATE NOT NULL,
    source_system               TEXT NOT NULL DEFAULT 'keystone',
    source_loan_serial          BIGINT NOT NULL,
    source_loan_request_serial  BIGINT,
    source_person_serial        BIGINT NOT NULL,
    person_key                  BIGINT REFERENCES person_identity(person_key),
    branch_key                  INT REFERENCES dim_branch(branch_key),
    loan_type                   TEXT,
    loan_status                 VARCHAR(40),
    original_amount             NUMERIC(14,2),
    current_balance             NUMERIC(14,2),
    open_date                   DATE,
    close_date                  DATE,
    maturity_date               DATE,
    interest_rate               NUMERIC(8,4),
    is_delinquent               BOOLEAN,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, loan_key),
    UNIQUE (snapshot_date, source_system, source_loan_serial)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS fct_digital_channels_transactions (
    snapshot_date               DATE PRIMARY KEY,
    bill_pay_transactions       INT,
    mobile_online_transactions  INT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fct_digital_enrollments (
    snapshot_date   DATE NOT NULL,
    service         TEXT NOT NULL,
    enrolled_count  INT NOT NULL,
    active_count    INT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, service)
);

CREATE TABLE IF NOT EXISTS fct_atm_by_machine_summary (
    snapshot_date   DATE NOT NULL,
    atm_name        TEXT NOT NULL,
    transactions    INT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, atm_name)
);

CREATE TABLE IF NOT EXISTS fct_branch_profitability_summary (
    snapshot_date   DATE NOT NULL,
    branch_key      INT NOT NULL REFERENCES dim_branch(branch_key),
    net_income      NUMERIC(14,2) NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, branch_key)
);

CREATE TABLE IF NOT EXISTS fct_teller_activity (
    activity_date       DATE NOT NULL,
    employee_key        INT NOT NULL REFERENCES dim_employee(employee_key),
    branch_key          INT REFERENCES dim_branch(branch_key),
    transaction_count   INT NOT NULL,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (activity_date, employee_key)
);

CREATE TABLE IF NOT EXISTS fct_teller_out_of_balance (
    event_id            SERIAL PRIMARY KEY,
    employee_key        INT NOT NULL REFERENCES dim_employee(employee_key),
    transaction_date    DATE NOT NULL,
    prev_cash           NUMERIC(12,2),
    new_cash            NUMERIC(12,2),
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fct_call_center_summary (
    snapshot_date               DATE PRIMARY KEY,
    calls_presented             INT,
    calls_answered              INT,
    calls_abandoned             INT,
    pct_answered                NUMERIC(6,4),
    pct_abandoned               NUMERIC(6,4),
    inbound_calls               INT,
    avg_call_duration_sec       NUMERIC(8,1),
    avg_speed_of_answer_sec     NUMERIC(8,1),
    answered_within_15_sec      INT,
    longest_wait_sec            INT,
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fct_call_center_agents (
    snapshot_date               DATE NOT NULL,
    employee_key                INT NOT NULL REFERENCES dim_employee(employee_key),
    call_count                  INT,
    avg_time_to_answer_sec      NUMERIC(8,1),
    avg_talking_duration_sec    NUMERIC(8,1),
    abandoned_calls             INT,
    missed_calls                INT,
    avg_abandoned_duration_sec  NUMERIC(8,1),
    abandoned_pct               NUMERIC(6,4),
    total_login_duration_sec    NUMERIC(12,1),
    loaded_at                   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, employee_key)
);

CREATE TABLE IF NOT EXISTS fct_survey_responses (
    engagement_id       TEXT PRIMARY KEY,
    response_date       DATE NOT NULL,
    queue_flow          TEXT,
    channel             TEXT,
    survey_format       TEXT,
    rating              TEXT,        -- text label as given in the survey: 'Excellent', 'Poor', etc.
    additional_feedback TEXT,
    member_name         TEXT,
    email               TEXT,
    phone               TEXT,
    loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS load_audit (
    batch_id             TEXT NOT NULL,
    source_system        TEXT,
    asset_name           TEXT NOT NULL,
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    source_extracted_at  TIMESTAMPTZ,
    row_count            INT,
    status               TEXT NOT NULL,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, asset_name)
);

CREATE TABLE IF NOT EXISTS hs_contacts (
    hs_contact_id   TEXT PRIMARY KEY,
    person_key      BIGINT REFERENCES person_identity(person_key),
    email           TEXT,
    firstname       TEXT,
    lastname        TEXT,
    phone           TEXT,
    company         TEXT,
    lifecyclestage  TEXT,
    hs_created_at   TIMESTAMPTZ,
    hs_updated_at   TIMESTAMPTZ,
    properties      JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hs_companies (
    hs_company_id   TEXT PRIMARY KEY,
    name            TEXT,
    domain          TEXT,
    industry        TEXT,
    city            TEXT,
    state           TEXT,
    hs_created_at   TIMESTAMPTZ,
    hs_updated_at   TIMESTAMPTZ,
    properties      JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hs_deals (
    hs_deal_id      TEXT PRIMARY KEY,
    dealname        TEXT,
    amount          NUMERIC(14,2),
    pipeline        TEXT,
    dealstage       TEXT,
    dealtype        TEXT,
    closedate       DATE,
    hs_created_at   TIMESTAMPTZ,
    hs_updated_at   TIMESTAMPTZ,
    properties      JSONB,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS hs_contact_company_association (
    hs_contact_id   TEXT NOT NULL REFERENCES hs_contacts(hs_contact_id),
    hs_company_id   TEXT NOT NULL REFERENCES hs_companies(hs_company_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hs_contact_id, hs_company_id)
);

CREATE TABLE IF NOT EXISTS hs_deal_contact_association (
    hs_deal_id      TEXT NOT NULL REFERENCES hs_deals(hs_deal_id),
    hs_contact_id   TEXT NOT NULL REFERENCES hs_contacts(hs_contact_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hs_deal_id, hs_contact_id)
);

CREATE TABLE IF NOT EXISTS hs_deal_company_association (
    hs_deal_id      TEXT NOT NULL REFERENCES hs_deals(hs_deal_id),
    hs_company_id   TEXT NOT NULL REFERENCES hs_companies(hs_company_id),
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (hs_deal_id, hs_company_id)
);

CREATE INDEX IF NOT EXISTS idx_hs_contacts_person_key
    ON hs_contacts (person_key);

CREATE INDEX IF NOT EXISTS idx_hs_contacts_email
    ON hs_contacts (email);

CREATE INDEX IF NOT EXISTS idx_hs_contacts_hs_updated_at
    ON hs_contacts (hs_updated_at);

CREATE INDEX IF NOT EXISTS idx_hs_companies_hs_updated_at
    ON hs_companies (hs_updated_at);

CREATE INDEX IF NOT EXISTS idx_hs_deals_hs_updated_at
    ON hs_deals (hs_updated_at);

CREATE INDEX IF NOT EXISTS idx_fct_loan_applications_primary_person_key
    ON fct_loan_applications (primary_person_key);

CREATE INDEX IF NOT EXISTS idx_fct_loan_applications_co_applicant_person_key
    ON fct_loan_applications (co_applicant_person_key);

CREATE INDEX IF NOT EXISTS idx_fct_loan_applications_source_primary_person
    ON fct_loan_applications (source_system, source_primary_person_serial);

CREATE INDEX IF NOT EXISTS idx_fct_loans_person_key
    ON fct_loans (person_key);

CREATE INDEX IF NOT EXISTS idx_fct_loans_snapshot_person
    ON fct_loans (snapshot_date, person_key);

CREATE INDEX IF NOT EXISTS idx_fct_loans_source_person
    ON fct_loans (source_system, source_person_serial);

CREATE INDEX IF NOT EXISTS idx_fct_loan_requests_application_number
    ON fct_loan_requests (application_number);

CREATE INDEX IF NOT EXISTS idx_fct_loan_requests_decision_category
    ON fct_loan_requests (decision_category);


-- ─────────────────────────────────────────────────────────────────────────────
-- BI presentation views (v_*) — no stored data
-- ─────────────────────────────────────────────────────────────────────────────

-- Member KPI summary — one row per month. All counts from fct_members_summary.
-- ks_members = OP Branch (only KS branch). hb_* = home-branch ZIP-territory counts.
-- net_new_* = current minus prior month; NULL when no prior row exists.
CREATE OR REPLACE VIEW v_members_monthly AS
SELECT
    snapshot_date,
    total_members,
    ks_members,
    mo_members,
    active_members,
    members_18_50,
    members_added,
    members_lost,
    non_kcpd_members,
    hb_ks_members,
    hb_mo_members,
    net_new_members,
    net_new_ks_members,
    net_new_mo_members,
    net_new_active_members,
    net_new_members_18_50,
    net_new_hb_mo,
    net_new_hb_ks,
    net_new_non_kcpd_members
FROM fct_members_summary;

-- Home-branch member counts by branch per month (ZIP-territory methodology).
-- One row per real branch per snapshot date. Unassigned members (no ZIP territory
-- match and no CU_HOME_BRANCH) are folded into Main Branch for BI presentation.
CREATE OR REPLACE VIEW v_home_branch_members_monthly AS
WITH rolled AS (
    SELECT
        hbms.snapshot_date,
        CASE
            WHEN branch.branch_name = 'Unassigned' THEN 'Main Branch'
            ELSE branch.branch_name
        END AS branch_name,
        hbms.total_members,
        hbms.members_added
    FROM fct_home_branch_members_summary AS hbms
    JOIN dim_branch AS branch ON branch.branch_key = hbms.branch_key
)
SELECT
    rolled.snapshot_date,
    rolled.branch_name,
    branch.abbreviation,
    SUM(rolled.total_members)::INTEGER AS total_members,
    SUM(rolled.members_added)::INTEGER AS members_added
FROM rolled
JOIN dim_branch AS branch ON branch.branch_name = rolled.branch_name
GROUP BY rolled.snapshot_date, rolled.branch_name, branch.abbreviation;

-- Membership termination events. Zoho aggregates by close_reason and date.
CREATE OR REPLACE VIEW v_members_lost AS
SELECT
    event_date,
    close_reason,
    membership_duration_yrs,
    first_gensh_opened
FROM fct_members_lost;

-- Credit Cards & Loans — YTD growth percentages + total balances. Feeds the
-- "Credit Cards & Loans" Zoho panel and balance tiles.
-- Growth % multiplied by 100 in the view (1.33 not 0.0133) because Zoho's
-- percentage data type appends "%" without multiplying; balances are dollar
-- amounts unchanged. All four columns come from fct_loan_portfolio_summary,
-- which is exclusively CSV-loaded by the manual-entries / backfill flow —
-- accounting does extra math on the balances that isn't visible in
-- staging.ks_loan, so we never derive these from the loan tables.
CREATE OR REPLACE VIEW v_cc_loans_growth_ytd_monthly AS
SELECT
    snapshot_date,
    (cc_growth_ytd_pct   * 100)::NUMERIC(8,4) AS credit_cards_growth_ytd_pct,
    (loan_growth_ytd_pct * 100)::NUMERIC(8,4) AS loans_growth_ytd_pct,
    cc_total_balance                          AS credit_cards_total_balance,
    loan_total_balance                        AS loans_total_balance
FROM fct_loan_portfolio_summary;

-- Loans by branch with LSI ("Online Applications") rolled into Main Branch.
-- The base table fct_loans_by_branch_summary keeps LSI as its own branch; this
-- view re-maps LSI's branch_key to Main and re-aggregates. Zoho pulls the table
-- for the LSI-separate view and this view for the LSI-in-Main view.
CREATE OR REPLACE VIEW v_loans_by_branch_lsi_in_main AS
WITH remapped AS (
    SELECT
        loans.snapshot_date,
        CASE
            WHEN src_branch.branch_name = 'LSI Branch - Online Applications'
            THEN (SELECT branch_key FROM dim_branch WHERE branch_name = 'Main Branch')
            ELSE loans.branch_key
        END                                AS branch_key,
        loans.loan_count,
        loans.payday_loan_count,
        loans.non_payday_loan_count
    FROM fct_loans_by_branch_summary AS loans
    JOIN dim_branch AS src_branch ON src_branch.branch_key = loans.branch_key
)
SELECT
    remapped.snapshot_date,
    branch.branch_name,
    SUM(remapped.loan_count)            AS loan_count,
    SUM(remapped.payday_loan_count)     AS payday_loan_count,
    SUM(remapped.non_payday_loan_count) AS non_payday_loan_count
FROM remapped
JOIN dim_branch AS branch ON branch.branch_key = remapped.branch_key
GROUP BY remapped.snapshot_date, branch.branch_name;

-- Financial ratios — CSV-loaded pct columns from fct_financial_ratios_summary.
-- Powers the Loan/Share, Delinquency, ROA, Operation Expense, Net Worth, Loan
-- Growth, and Loan Charge-Off panels + their KPI tiles. NPS lives in
-- v_nps_summary (separate base table); ATM-network metrics live in
-- v_atm_network_monthly.
-- All pct columns multiplied by 100 in the view (78.21 not 0.7821) because
-- Zoho's percentage data type appends "%" without multiplying.
CREATE OR REPLACE VIEW v_financial_ratios_monthly AS
SELECT
    snapshot_date,
    (net_worth_pct          * 100)::NUMERIC(8,4) AS net_worth_pct,
    (roa_pct                * 100)::NUMERIC(8,4) AS roa_pct,
    (op_expense_pct         * 100)::NUMERIC(8,4) AS op_expense_pct,
    (loan_to_share_pct      * 100)::NUMERIC(8,4) AS loan_to_share_pct,
    (loan_delinquency_pct   * 100)::NUMERIC(8,4) AS loan_delinquency_pct,
    (loan_charge_off_ratio  * 100)::NUMERIC(8,4) AS loan_charge_off_ratio,
    (loan_growth_pct        * 100)::NUMERIC(8,4) AS loan_growth_pct
FROM fct_financial_ratios_summary;

-- ATM network usage + YTD network income — split out from financial ratios
-- because these are counts and dollars, not percentages. ATM uses are integer
-- transaction counts; atm_network_ytd_net_income is a dollar amount.
CREATE OR REPLACE VIEW v_atm_network_monthly AS
SELECT
    snapshot_date,
    atm_member_uses,
    atm_foreign_uses,
    atm_network_ytd_net_income
FROM fct_financial_ratios_summary;

-- NPS score history — feeds the "Most Recent NPS Score" tile. Sparse: a row
-- exists only when a survey actually ran, so not a true monthly cadence.
CREATE OR REPLACE VIEW v_nps_summary AS
SELECT
    snapshot_date,
    nps_score
FROM fct_nps_summary;

