-- Source system schema (RDS PostgreSQL). Synthetic stand-in for the metering / CRM / registry estate.
-- Keep column names and families in sync with lakehouse/tables.py.
-- DMS needs a primary key on every captured table (all have one). REPLICA IDENTITY DEFAULT is enough:
-- updates carry the full new row, deletes carry the key only.

CREATE SCHEMA IF NOT EXISTS src;

CREATE TABLE src.supply_point (
    supply_point_id   text PRIMARY KEY,
    supply_ref        text NOT NULL,
    region            text,
    grid_zone         text,
    supply_type       text,
    status            text,
    source_updated_at timestamp NOT NULL
);

CREATE TABLE src.meter (
    meter_id          text PRIMARY KEY,
    supply_point_id   text,
    serial_no         text,
    meter_type        text,
    status            text,
    installed_at      date,
    source_updated_at timestamp NOT NULL
);

CREATE TABLE src.customer (
    customer_id       text PRIMARY KEY,
    customer_type     text,
    region            text,
    name              text,          -- PII
    address           text,          -- PII
    valid_from        date,
    source_updated_at timestamp NOT NULL
);

CREATE TABLE src.account (
    account_id        text PRIMARY KEY,
    customer_id       text NOT NULL,
    account_ref       text,
    status            text,
    opened_at         date,
    closed_at         date,
    source_updated_at timestamp NOT NULL
);

CREATE TABLE src.tariff (
    tariff_id         text PRIMARY KEY,
    name              text,
    tariff_type       text,
    currency          text,
    valid_from        date,
    valid_to          date,
    source_updated_at timestamp NOT NULL
);

-- Simplification of the design guide: the tariff is attached to the assignment, valid_to is exclusive.
CREATE TABLE src.meter_assignment (
    assignment_id     text PRIMARY KEY,
    meter_id          text NOT NULL,
    account_id        text NOT NULL,
    supply_point_id   text,
    tariff_id         text,
    valid_from        date NOT NULL,
    valid_to          date NOT NULL,   -- 9999-12-31 = open
    source_updated_at timestamp NOT NULL
);

-- Deliberately no foreign keys on the head-end tables: the legacy head-end can send unknown meters.
CREATE TABLE src.meter_reading (
    reading_id        text NOT NULL,
    meter_id          text,
    reading_ts        timestamp,       -- interval start, UTC
    interval_end      timestamp,
    version           integer NOT NULL,
    consumption_kwh   numeric(12,3),
    reading_type      text,
    quality_code      text,
    source_updated_at timestamp NOT NULL,
    PRIMARY KEY (reading_id, version)
);

CREATE TABLE src.reading_correction (
    correction_id     text PRIMARY KEY,
    reading_id        text NOT NULL,
    meter_id          text,
    reading_ts        timestamp,
    version           integer,
    replacement_kwh   numeric(12,3),
    reason            text,
    corrected_at      timestamp NOT NULL,
    source_updated_at timestamp NOT NULL
);

CREATE TABLE src.settlement_calendar (
    settlement_date   date NOT NULL,
    period            integer NOT NULL,
    start_utc         timestamp NOT NULL,
    end_utc           timestamp NOT NULL,
    local_start       timestamp,
    duration_minutes  integer,
    dst_flag          boolean,
    calendar_version  integer,
    PRIMARY KEY (settlement_date, period)
);

-- Billing control totals the Gold layer is reconciled against.
CREATE TABLE src.reconciliation_control (
    source_name       text NOT NULL,
    period            date NOT NULL,
    control           text NOT NULL,
    run_id            text NOT NULL,
    expected_count    integer,
    expected_total    numeric(18,3),
    unit              text,
    source_cutoff     timestamp,
    created_at        timestamp NOT NULL,
    control_version   integer NOT NULL,
    PRIMARY KEY (source_name, period, control, run_id)
);
