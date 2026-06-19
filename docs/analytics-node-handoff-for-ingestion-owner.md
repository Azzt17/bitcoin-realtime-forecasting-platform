# Analytics Node Handoff Notes for Data Ingestion Owner

## Purpose

This document is a handoff note for the teammate who will continue work on the `analytics-node`.

It is designed to be readable by both humans and AI coding assistants. If you use an AI assistant to continue this work, provide this file as project context before asking it to generate code, commands, scripts, or pull requests.

## Project

Repository:

```text
bitcoin-realtime-forecasting-platform
```

Project goal:

```text
Build a cloud-based Bitcoin realtime forecasting platform using Kafka, Spark, ClickHouse, and dashboard tooling.
```

High-level data flow:

```text
Historical data / API data
        ↓
staging area on analytics-node
        ↓
ClickHouse raw tables
        ↓
Spark batch / streaming feature generation
        ↓
prediction tables
        ↓
Grafana / Streamlit dashboard
```

## Current Infrastructure Status

The cloud foundation has already been provisioned.

Provisioned nodes:

```text
kafka-1
kafka-2
kafka-3
spark-master
spark-worker-1
analytics-node
```

Completed milestones:

```text
Terraform DigitalOcean foundation: complete
Node runtime bootstrap: complete
Kafka cluster deployment: complete
Kafka topics creation: complete
Kafka produce/consume smoke test: complete
Analytics-node access for ingestion teammate: complete
```

Kafka topics currently available:

```text
btc.market.raw
btc.onchain.raw
btc.features.realtime
btc.predictions
btc.deadletter
btc.pipeline.metrics
```

Kafka smoke test result:

```text
Produce/consume test to btc.deadletter succeeded.
```

## Role Division

### Farid / Infrastructure Owner

Farid is responsible for:

```text
Terraform infrastructure
DigitalOcean resource lifecycle
VPC/firewall policy
Kafka cluster foundation
Spark cluster foundation
general infrastructure documentation
repository governance
reviewing infrastructure pull requests
```

Farid should approve changes that affect:

```text
Terraform
firewall
Droplet topology
Kafka deployment scripts
Spark deployment scripts
global service contracts
security-sensitive configuration
```

### Data Ingestion / Analytics Node Owner

The teammate receiving this document is responsible for:

```text
analytics-node operation
ClickHouse installation/deployment
ClickHouse database and schema setup
historical data staging
historical data import into ClickHouse
data validation after import
documenting ingestion commands and assumptions
opening pull requests for any repo changes
```

This role may work on:

```text
ClickHouse service files
ClickHouse schema files
data import scripts
data validation scripts
data documentation
ingestion runbooks
```

### Modeling Owner

The modeling teammate is responsible for:

```text
feature engineering logic
model training
model evaluation
prediction target definition
model artifact format
batch/realtime inference logic
```

The data ingestion owner should coordinate with the modeling owner before changing feature definitions or target table formats.

## Analytics Node Access

The ingestion owner has SSH access to:

```text
analytics-node
```

The Linux user is:

```text
ingestion
```

Farid has granted permanent sudo access to the ingestion teammate for continuing work on the analytics node.

Important: sudo access means the user can modify system services. Any service change should be documented and, when possible, represented as code or scripts in the repository.

## Working Directory on Analytics Node

Primary project directory:

```text
/opt/bitcoin-realtime-forecasting-platform
```

Recommended data staging directory:

```text
/opt/bitcoin-realtime-forecasting-platform/data/imports
```

Expected staging structure:

```text
/opt/bitcoin-realtime-forecasting-platform/data/imports/
├── incoming/
├── processed/
├── rejected/
└── metadata/
```

Meaning:

```text
incoming/   raw files waiting to be imported
processed/  files already imported successfully
rejected/   invalid or failed files
metadata/   README/metadata files describing datasets
```

## Immediate Next Mission

The next mission is to set up ClickHouse on `analytics-node`.

Recommended order:

```text
1. Verify analytics-node access
2. Inspect current runtime environment
3. Decide ClickHouse deployment method
4. Deploy ClickHouse
5. Secure ClickHouse configuration
6. Create database btc
7. Create raw/staging tables
8. Import a small test file first
9. Validate imported row counts and schema
10. Document all commands and assumptions
11. Open a pull request
```

Recommended deployment method:

```text
Docker Compose on analytics-node
```

Reason:

```text
The node already has Docker and Docker Compose installed.
Docker Compose keeps the deployment reproducible.
Service definitions can be committed to the repository.
```

## Expected ClickHouse Scope

ClickHouse should store:

```text
raw historical market data
raw historical block data
raw or aggregated transaction data
feature tables
prediction tables
model metrics
pipeline metrics
```

Initial database:

```sql
CREATE DATABASE IF NOT EXISTS btc;
```

Recommended initial tables:

```text
btc.raw_ohlcv
btc.raw_blocks
btc.raw_transactions_staging
btc.predictions
btc.prediction_errors
btc.model_metrics
btc.pipeline_metrics
```

Do not create final feature tables without coordinating with the modeling owner.

## Historical Data Ingestion Rules

Do not import the entire dataset first.

Use this order:

```text
1. Upload a tiny sample file
2. Import sample file
3. Validate schema
4. Validate row count
5. Validate timestamp/timezone interpretation
6. Validate numeric types
7. Import a larger sample
8. Import full dataset only after sample validation
```

For each dataset, provide metadata.

Minimum metadata:

```text
dataset name
source
time range
timezone
row count
column names
file format
compression
owner
upload date
notes about missing values or anomalies
```

Example metadata file:

```text
README_btc_ohlcv_1m_2012_2026.txt
```

Example metadata content:

```text
Dataset name: BTC OHLCV 1-minute
Source: historical market dataset
Time range: 2012-01-01 to 2026-xx-xx
Timezone: UTC
Rows: 7607549
Columns: Timestamp, Open, High, Low, Close, Volume
File format: CSV
Compression: none
Owner: data ingestion team
Upload date: 2026-06-19
Notes: Timestamp is Unix seconds.
```

## Repository Workflow

All changes must go through pull request.

Do not push directly to `main`.

Workflow:

```bash
git checkout main
git pull origin main
git checkout -b feat/clickhouse-analytics-node
```

After making changes:

```bash
git status --short
git diff
```

Stage exact files only.

Do not use:

```bash
git add .
```

Use exact staging:

```bash
git add path/to/file1 path/to/file2
```

Commit with a clear message:

```bash
git commit -m "feat: add ClickHouse analytics service"
```

Push branch:

```bash
git push -u origin feat/clickhouse-analytics-node
```

Open a pull request to `main`.

## Pull Request Requirements

Every PR should include:

```text
what changed
why it changed
how it was tested
commands used
risks or limitations
screenshots/log output if useful
rollback steps
```

For ClickHouse deployment PR, include:

```text
ClickHouse container status
ClickHouse version
database creation command
schema creation command
sample import command
sample validation query
where data is stored
how to stop/restart the service
```

## Documentation Requirements

If you change infrastructure or ingestion behavior, document it.

Recommended docs:

```text
docs/clickhouse-deployment.md
docs/historical-ingestion-runbook.md
docs/data-dictionary.md
docs/ingestion-validation.md
```

At minimum, document:

```text
commands used
file paths
service names
ports
database/table names
schema assumptions
known limitations
```

## Files That Must Not Be Committed

Do not commit:

```text
raw datasets
large CSV/XLSX/Parquet files
API keys
passwords
.env files with real secrets
SSH private keys
Terraform state files
database dump files
ClickHouse data directories
logs containing secrets
```

Examples to avoid:

```text
*.csv
*.xlsx
*.parquet
*.env
terraform.tfvars
terraform.tfstate
id_ed25519
id_rsa
```

If a sample dataset is needed for tests, keep it tiny and clearly mark it as sample data.

## Expected Repo Additions for ClickHouse

Suggested structure:

```text
services/
└── clickhouse/
    ├── docker-compose.yml
    ├── config/
    │   └── config.xml
    ├── users/
    │   └── users.xml
    └── README.md

clickhouse/
├── schema.sql
├── sample_queries.sql
└── README.md

scripts/
├── deploy-clickhouse.sh
├── verify-clickhouse.sh
└── import-ohlcv-sample.sh

docs/
├── clickhouse-deployment.md
├── historical-ingestion-runbook.md
└── data-dictionary.md
```

This structure may be adjusted, but changes should be explained in the PR.

## ClickHouse Validation Checklist

Before saying ClickHouse is complete:

```text
[ ] ClickHouse service starts successfully
[ ] ClickHouse restarts correctly
[ ] Data directory is persistent
[ ] btc database exists
[ ] raw table schema exists
[ ] sample CSV import succeeds
[ ] row count matches sample file
[ ] timestamp conversion is correct
[ ] numeric columns use safe types
[ ] query performance is acceptable for sample
[ ] service ports are not unnecessarily public
[ ] deployment is documented
[ ] PR is opened
```

## Security Notes

Do not expose ClickHouse publicly unless Farid approves the firewall and authentication model.

Recommended default:

```text
ClickHouse only accessible internally or through SSH tunnel
```

If local access from Farid's laptop is needed, use SSH tunnel:

```bash
ssh -L 8123:localhost:8123 root@<ANALYTICS_NODE_PUBLIC_IP>
```

Then access:

```text
http://localhost:8123
```

Do not publish database credentials in the repository.

## AI Assistant Instructions

If using an AI assistant, give it this instruction:

```text
You are assisting with the analytics-node and historical ingestion portion of the bitcoin-realtime-forecasting-platform repository.

Respect the role boundary:
- Do not modify Terraform, Kafka cluster scripts, firewall, or global infrastructure without explicit approval from Farid.
- Focus on ClickHouse deployment, schema setup, data staging, import scripts, validation scripts, and documentation.
- Do not commit raw datasets, secrets, API keys, SSH keys, Terraform state, or large files.
- All repository changes must be proposed through a branch and pull request.
- Use exact git staging commands, never `git add .`.
- Every change must include documentation and test/verification commands.
- Prefer reproducible scripts over manual-only instructions.
- Ask for clarification before changing schema contracts that affect modeling or dashboards.
```

## Suggested First AI Task

Use this prompt with an AI assistant:

```text
Analyze this repository and propose a ClickHouse deployment plan for analytics-node.

Constraints:
- Use Docker Compose on analytics-node.
- Do not modify Terraform or Kafka scripts.
- Do not commit secrets or raw data.
- Create reproducible deployment and verification scripts.
- Add documentation for deployment, schema, and sample ingestion.
- Use a feature branch and pull request workflow.
- Do not use `git add .`.
- Start with a small sample import, not full historical data.
```

## Suggested First Branch

```bash
git checkout main
git pull origin main
git checkout -b feat/clickhouse-analytics-node
```

## Suggested First Commit Series

Keep commits small:

```text
feat: add ClickHouse service definition
feat: add initial ClickHouse schema
chore: add ClickHouse deployment scripts
docs: add ClickHouse deployment guide
docs: add historical ingestion runbook
```

## Current Handoff Status

Farid has paused after completing:

```text
cloud foundation
node bootstrap
Kafka cluster
Kafka topics
Kafka smoke test
ingestion SSH access
```

The ingestion owner continues from:

```text
ClickHouse installation and historical data ingestion on analytics-node.
```

Any repository change should be made through pull request.
