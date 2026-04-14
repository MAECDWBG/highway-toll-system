-- =============================================================================
-- sql/schema.sql
-- Redshift Star Schema for Highway Toll Collection System
--
-- Author : Mayukh Ghosh | Roll No : 23052334
-- Batch  : Data Engineering 2025-2026, KIIT University
-- =============================================================================

-- Create the mart schema
CREATE SCHEMA IF NOT EXISTS mart;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- DIM_DATE
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_id      INTEGER      NOT NULL ENCODE az64,
    date         DATE         NOT NULL,
    year         SMALLINT     NOT NULL,
    month        SMALLINT     NOT NULL,
    day          SMALLINT     NOT NULL,
    day_of_week  VARCHAR(10)  NOT NULL,
    is_weekend   SMALLINT     DEFAULT 0,
    PRIMARY KEY (date_id)
)
DISTSTYLE ALL
SORTKEY (date_id);


-- DIM_VEHICLE
CREATE TABLE IF NOT EXISTS mart.dim_vehicle (
    vehicle_id       BIGINT IDENTITY(1,1),
    tag_id           VARCHAR(20)  NOT NULL,
    vehicle_no       VARCHAR(15)  NOT NULL,
    owner_name       VARCHAR(100),
    registered_state VARCHAR(3),
    active           SMALLINT DEFAULT 1,
    created_at       TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (vehicle_id)
)
DISTSTYLE KEY
DISTKEY (vehicle_id)
SORTKEY (tag_id);


-- DIM_PLAZA
CREATE TABLE IF NOT EXISTS mart.dim_plaza (
    plaza_id    VARCHAR(10)   NOT NULL,
    name        VARCHAR(200)  NOT NULL,
    highway     VARCHAR(20),
    state       VARCHAR(50),
    latitude    DECIMAL(9,6),
    longitude   DECIMAL(9,6),
    operator    VARCHAR(100),
    active      SMALLINT DEFAULT 1,
    PRIMARY KEY (plaza_id)
)
DISTSTYLE ALL;


-- DIM_CLASS (vehicle classification)
CREATE TABLE IF NOT EXISTS mart.dim_class (
    class_id    VARCHAR(5)   NOT NULL,
    name        VARCHAR(50)  NOT NULL,
    axles       SMALLINT,
    base_rate   DECIMAL(8,2) NOT NULL,
    PRIMARY KEY (class_id)
)
DISTSTYLE ALL;


-- DIM_PAYMENT
CREATE TABLE IF NOT EXISTS mart.dim_payment (
    payment_id   SMALLINT    NOT NULL,
    method       VARCHAR(20) NOT NULL,   -- FASTAG, CASH, CARD
    status       VARCHAR(20) NOT NULL,   -- SUCCESS, FAILED, FLAGGED, LOW_BALANCE
    PRIMARY KEY (payment_id)
)
DISTSTYLE ALL;


-- =============================================================================
-- FACT TABLE
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.fact_transaction (
    transaction_id  VARCHAR(40)   NOT NULL ENCODE zstd,
    plaza_id        VARCHAR(10)   NOT NULL,
    vehicle_id      BIGINT        NOT NULL,
    date_id         INTEGER       NOT NULL,
    class_id        VARCHAR(5)    NOT NULL,
    payment_id      SMALLINT      NOT NULL,
    amount          DECIMAL(10,2) NOT NULL,
    entry_time      TIMESTAMP     NOT NULL,
    exit_time       TIMESTAMP,
    lane            SMALLINT,
    ingested_at     TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (transaction_id),
    FOREIGN KEY (plaza_id)   REFERENCES mart.dim_plaza(plaza_id),
    FOREIGN KEY (vehicle_id) REFERENCES mart.dim_vehicle(vehicle_id),
    FOREIGN KEY (date_id)    REFERENCES mart.dim_date(date_id),
    FOREIGN KEY (class_id)   REFERENCES mart.dim_class(class_id),
    FOREIGN KEY (payment_id) REFERENCES mart.dim_payment(payment_id)
)
DISTSTYLE KEY
DISTKEY (plaza_id)
COMPOUND SORTKEY (date_id, plaza_id);


-- =============================================================================
-- AGGREGATE TABLE (pre-computed for Grafana)
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.agg_hourly_revenue (
    plaza_id         VARCHAR(10)   NOT NULL,
    date_id          INTEGER       NOT NULL,
    hour             SMALLINT      NOT NULL,
    tx_count         INTEGER       DEFAULT 0,
    total_revenue    DECIMAL(14,2) DEFAULT 0,
    unique_vehicles  INTEGER       DEFAULT 0,
    avg_toll         DECIMAL(8,2),
    flagged_count    INTEGER       DEFAULT 0,
    loaded_at        TIMESTAMP     DEFAULT GETDATE(),
    PRIMARY KEY (plaza_id, date_id, hour)
)
DISTSTYLE KEY
DISTKEY (plaza_id)
COMPOUND SORTKEY (date_id, hour);


-- =============================================================================
-- SEED: DIM_PAYMENT
-- =============================================================================

INSERT INTO mart.dim_payment VALUES
    (1, 'FASTAG',  'SUCCESS'),
    (2, 'FASTAG',  'LOW_BALANCE'),
    (3, 'FASTAG',  'FLAGGED'),
    (4, 'CASH',    'SUCCESS'),
    (5, 'CARD',    'SUCCESS'),
    (6, 'CARD',    'FAILED');


-- =============================================================================
-- SEED: DIM_CLASS
-- =============================================================================

INSERT INTO mart.dim_class VALUES
    ('C1', '2-Wheeler',       2,  35.00),
    ('C2', 'Car/Jeep/Van',    4,  75.00),
    ('C3', 'Light Commercial',4, 120.00),
    ('C4', 'Bus/Truck',       6, 215.00),
    ('C5', 'Multi-Axle',      8, 330.00),
    ('C6', 'Oversized',      12, 455.00);


-- =============================================================================
-- SEED: DIM_PLAZA (sample)
-- =============================================================================

INSERT INTO mart.dim_plaza (plaza_id, name, highway, state, operator) VALUES
    ('PL001', 'Delhi-Meerut Expressway KM 12',      'NH-58',  'Uttar Pradesh', 'NHAI'),
    ('PL002', 'Mumbai-Pune Expressway KM 45',        'NH-48',  'Maharashtra',   'MSRDC'),
    ('PL003', 'Bengaluru-Mysuru Expressway KM 8',    'NH-275', 'Karnataka',     'NHAI'),
    ('PL004', 'Chennai-Vijayawada KM 60',            'NH-16',  'Andhra Pradesh','NHAI'),
    ('PL005', 'Kolkata-Dhanbad KM 30',               'NH-19',  'West Bengal',   'NHAI');
