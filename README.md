# Spring Batch + Kafka POC

Spring Batch 6 + Kafka POC demonstrating tasklet and chunk-oriented processing across
three interfaces: text→CSV, CSV→text, and text→MySQL, triggered via Kafka.

Each job follows the same three-step shape (`validate → transform → archive`). A shared
MySQL audit table tracks every job run across all three variants, alongside Spring
Batch's own `JobRepository` metadata for restart and fault-tolerance support.

## Stack

- Java 21
- Spring Boot 4.1.0
- Spring Batch 6.0.x
- Spring Kafka
- MySQL 8.4
- Docker Compose (for local Kafka + MySQL)

## Why three jobs, one shape

| Job | Reader source | Writer target | Demonstrates |
|---|---|---|---|
| `textToCsvJob` | Text file | CSV file | Chunk-oriented processing, high-volume line-by-line transform |
| `csvToTextJob` | CSV file | Text file | Mirror-image chunk step, different reader/writer types |
| `textToDbJob` | Text file | MySQL table | `JdbcBatchItemWriter` instead of a file writer — mirrors a real Kafka-consumer-into-database pattern |

All three share:
- **`FileValidationTasklet`** — confirms the source file exists/is readable before the chunk step runs
- **`ArchiveCleanupTasklet`** — moves the processed file and writes a row to the audit table
- **`processed_files`** audit table — one place to see every job run, any direction, any outcome

## Architecture

```
Kafka topic (batch-job-trigger)
        │
        ▼
BatchJobKafkaListener  ──▶  JobOperator.run(job, params)
        │
        ▼
Job = tasklet(validate) → chunk(transform) → tasklet(archive)
        │
        ▼
MySQL: processed_files (audit) + transformed_records (business data, textToDbJob only)
        + Spring Batch's own JobRepository tables (restart/fault-tolerance metadata)
```

Kafka messages carry a **file path and job type**, not the file's contents — the
message is a "go process this" signal; the data itself lives on disk (or, in a real
system, wherever the upstream actually drops it).

## Getting started

### 1. Start MySQL and Kafka

```bash
docker compose up -d
docker compose ps   # confirm both are healthy
```

### 2. Create the schema

Spring Batch's own tables (`BATCH_JOB_INSTANCE`, `BATCH_JOB_EXECUTION`, etc.) are
created automatically via `@EnableJdbcJobRepository` on first run. The audit and
business-data tables need to be created once manually:

```bash
docker exec -i batch-poc-mysql mysql -ubatchuser -pbatchpass batchpoc \
  < src/main/resources/schema-audit.sql
```

### 3. Add sample input files

```bash
mkdir -p input output
echo "Alice|Engineer|Remote" > input/sample.txt
```

### 4. Run the application

```bash
./mvnw spring-boot:run
```

Jobs do **not** run automatically on startup (`spring.batch.job.enabled=false`) —
they're launched explicitly via Kafka or the demo REST endpoint below.

### 5. Trigger a job

Via the demo REST endpoint (publishes to Kafka, proving the full path end to end):

```bash
curl -X POST http://localhost:8080/demo/trigger \
  -H "Content-Type: application/json" \
  -d '{"jobType":"TEXT_TO_CSV","inputFilePath":"input/sample.txt"}'
```

Valid `jobType` values: `TEXT_TO_CSV`, `CSV_TO_TEXT`, `TEXT_TO_DB`.

### 6. Check the results

- **Output file:** `output/output.csv` or `output/output.txt`
- **Archived source:** moved to `input/processed/`
- **Audit trail:** `SELECT * FROM processed_files;`
- **Batch execution history:** `SELECT * FROM BATCH_JOB_EXECUTION;`

## Project structure

```
src/main/java/com/adipta/batchkafka/
├── config/        — JDBC-backed JobRepository + JobOperator setup (defined once, app-wide)
├── tasklet/        — shared validate/archive tasklets, reused by all three jobs
├── audit/          — audit entity, repository, service
├── job/
│   ├── texttocsv/  — Job 1
│   ├── csvtotext/  — Job 2
│   └── texttodb/   — Job 3 (bonus variant)
└── kafka/          — listener + demo trigger controller
```

## Design notes

- **Batch 6 packaging:** `ItemReader`/`ItemWriter`/`ItemProcessor` and flat-file classes
  live under `org.springframework.batch.infrastructure.item.*` (moved from
  `org.springframework.batch.item.*` in earlier versions). Chunk steps use
  `ChunkOrientedStepBuilder` rather than `StepBuilder(...).chunk(...)`. `JobOperator`
  replaces the deprecated `JobLauncher`.
- **MySQL's three roles:** Spring Batch's own metadata tables (automatic), the shared
  `processed_files` audit table (written by every archive tasklet), and
  `transformed_records` (business data, used only by `textToDbJob`).
- **Single service, not microservices:** this POC intentionally runs as one Spring Boot
  application. The goal is to validate the batch/Kafka mechanics, not to stand up a
  distributed system — service decomposition is a production concern, not a POC one.

## Status

Proof-of-concept only — not production-hardened. `ddl-auto: update` and manual schema
scripts are POC shortcuts; a real deployment would use migrations (Flyway/Liquibase)
and externalized secrets rather than the plaintext credentials in `application.yml`.