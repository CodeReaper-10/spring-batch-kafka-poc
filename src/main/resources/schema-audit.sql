-- ============================================================
-- schema-audit.sql
-- Run this manually once against MySQL (or add to a
-- src/main/resources/schema-audit.sql and reference it via
-- spring.sql.init.mode / spring.sql.init.schema-locations if you
-- want Boot to run it automatically on startup).
-- ============================================================

-- Shared across ALL FOUR jobs (text->csv, csv->text, item->csv, control-item->csv).
-- One row per job execution, written by the archive tasklet at the
-- very end of each run. This is the table you query live during the
-- demo to show "here's every job run, regardless of direction."
CREATE TABLE IF NOT EXISTS processed_files (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    -- Which Job ran: "textToCsvJob" | "csvToTextJob" | "itemToCsvJob" | "controlItemToCsvJob"
    job_name VARCHAR(100) NOT NULL,
    -- Redundant with job_name but query-friendlier for filtering/grouping
    -- during the demo: "TEXT_TO_CSV" | "CSV_TO_TEXT" | "ITEM_TO_CSV" | "CONTROL_ITEM_TO_CSV"
    direction VARCHAR(30)  NOT NULL,
    -- Name of the source file that was processed (not full path --
    -- keeps this table readable; full path is in Batch's own
    -- BATCH_JOB_EXECUTION_PARAMS table if you ever need it)
    file_name VARCHAR(255) NOT NULL,
    -- How many rows/lines were processed. Nullable because a failed
    -- validation step means this job never got as far as processing
    -- anything.
    rows_processed BIGINT,
    -- "SUCCESS" | "ARCHIVE_FAILED" | "FAILED"
    -- Deliberately a plain VARCHAR, not an ENUM -- keeps this table
    -- forgiving if you add a new status value mid-week without a
    -- migration.
    status VARCHAR(30) NOT NULL,
    run_at DATETIME NOT NULL,
    -- Helpful during the demo: "show me every job that touched this
    -- file" or "show me every run today"
    INDEX idx_job_name (job_name),
    INDEX idx_run_at (run_at)
);