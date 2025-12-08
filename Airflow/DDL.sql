-- STAGING TABLES

CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id      VARCHAR(50),
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    email            VARCHAR(200),
    signup_date      DATE,
    country          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS stg_products (
    product_id       VARCHAR(50),
    product_name     VARCHAR(255),
    category         VARCHAR(100),
    unit_price       NUMERIC(10,2)
);

CREATE TABLE IF NOT EXISTS stg_orders (
    order_id         VARCHAR(50),
    order_timestamp  TIMESTAMP,
    customer_id      VARCHAR(50),
    product_id       VARCHAR(50),
    quantity         INT,
    total_amount     NUMERIC(10,2),
    currency         VARCHAR(10),
    status           VARCHAR(50)
);
-- DIMENSION TABLES
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id      VARCHAR(50) PRIMARY KEY,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    email            VARCHAR(200),
    signup_date      DATE,
    country          VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id       VARCHAR(50) PRIMARY KEY,
    product_name     VARCHAR(255),
    category         VARCHAR(100),
    unit_price       NUMERIC(10,2)
);
-- FACT TABLE
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                 VARCHAR(50),
    order_timestamp_utc      TIMESTAMP,
    customer_id              VARCHAR(50),
    product_id               VARCHAR(50),
    quantity                 INT,
    total_amount             NUMERIC(10,2),
    currency_mismatch_flag   INT,
    status                   VARCHAR(50),
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);
