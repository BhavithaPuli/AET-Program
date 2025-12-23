1. End-to-End Pipeline Explanation

This project implements a complete end-to-end data engineering pipeline using the Bronze–Silver–Gold architecture on Databricks, with Power BI as the visualization layer.

Bronze Layer (Raw Ingestion)
  Raw sales, products, and stores data are ingested into the Bronze layer.
  Data is stored in Delta tables:
  bronze.sales_raw
  bronze.products_raw
  bronze.stores_raw
  Minimal transformations are applied.
  Ingestion metadata such as ingestion_timestamp and source_system is added.
  Each ingestion run is logged in bronze_logs for auditability.

Silver Layer (Cleaning & Validation)
 The Silver layer focuses on data quality and standardization.
 Reference data (stores, products) is:
 Validated for null primary keys
 Deduplicated
 Sales data is:
 Cleaned and standardized
 Calibrated
 Deduplicated by transaction_id
 Split into:
 silver.sales (valid records)
 silver.sales_quarantine (invalid records)
 All Silver processing is logged in silver_logs.

Gold Layer (Analytics & Reporting)
 The Gold layer contains business-ready, aggregated datasets.
 Joins and aggregations are intentionally performed here to keep Silver simple.
 Gold tables created:
 gold.daily_sales
 gold.product_performance
 gold.store_revenue
 gold.monthly_revenue
 Gold operations are logged in gold_logs.
 These tables are consumed directly by Power BI dashboards.
Visualization (Power BI)
 Power BI connects to Databricks and consumes Gold tables only.
 Dashboards provide insights into:
 Daily revenue trends
 Product performance
 Regional and store-level performance

2. Incremental Logic
Incremental processing is implemented in the Silver Sales layer to ensure scalability and efficiency.
Watermark-Based Processing
 A watermark table silver_watermark stores the last processed ingestion_timestamp.
 During each run:
 Only records with ingestion_timestamp greater than the watermark are processed.
 This ensures only new or late-arriving data is handled.
Idempotent Upserts
  Delta Lake MERGE operations are used on silver.sales:
  Existing records are updated
  New records are inserted
  This guarantees safe re-runs and prevents duplicate records.
Late-Arriving Data
  Late-arriving records are captured because the logic relies on ingestion_timestamp, not business timestamps.
  This ensures historical data arriving late is still processed correctly.

3. Calibration Logic
Calibration logic ensures data correctness and consistency before analytics.
Sales Amount Calibration
  The total sales amount is recalculated using the formula:
  total_amount_corrected = (quantity × unit_price) − discount
  This ensures inconsistencies in raw data do not propagate downstream.
Currency Normalization
  All sales are normalized to USD using predefined exchange rates.
  A derived column total_amount_usd is created to enable consistent revenue analysis.
Timestamp Standardization
  Transaction timestamps are converted to UTC (transaction_ts_utc) to avoid timezone inconsistencies.

5. Challenges & Assumptions

Challenges Faced
Handling schema conflicts during joins due to duplicate metadata columns.
Managing incremental updates while supporting late-arriving data.
Ensuring idempotent pipeline execution during multiple re-runs.
Avoiding complexity in the Silver layer while maintaining data quality.

Key Assumptions
transaction_id, product_id, and store_id are treated as primary identifiers.
Reference data (products and stores) changes infrequently.
Exchange rates are assumed to be static for this implementation.
Power BI is used strictly as a consumption layer and does not perform data transformations.

Conclusion
This project demonstrates a production-style data engineering pipeline with:
Layered architecture (Bronze–Silver–Gold)
Incremental processing
Data quality enforcement

End-to-end observability through logging

Business insights delivered via Power BI
