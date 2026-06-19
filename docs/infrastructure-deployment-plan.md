# Infrastructure Deployment Plan

## Project

`bitcoin-realtime-forecasting-platform`

## Purpose

This document defines the infrastructure deployment plan for the Bitcoin realtime forecasting project.

The goal is to provide a clear, staged path from local infrastructure validation to cloud deployment, without taking ownership of historical data cleaning or model development.

## Deployment Strategy

The infrastructure will be built in stages.

```text
Stage 1: Local infrastructure baseline
Stage 2: Local integration with ingestion/modeling teams
Stage 3: Cloud infrastructure planning
Stage 4: Terraform-based DigitalOcean deployment
Stage 5: Service hardening and dashboard access
Stage 6: Teardown and cost-control workflow
```

The first priority is correctness and integration. Scalability and benchmark testing come after the basic platform is stable.

---

## Stage 1 — Local Infrastructure Baseline

### Goal

Run the core infrastructure locally so the team can validate service contracts before deploying to cloud.

### Services

```text
Kafka
Spark
ClickHouse
Grafana
Streamlit
```

### Expected Output

```text
Kafka can accept messages.
Spark can connect to Kafka.
Spark can write to ClickHouse.
Grafana can read from ClickHouse.
Streamlit can read from ClickHouse.
```

### Deliverables

```text
docker-compose.yml
clickhouse/schema.sql
scripts/create-topics.sh
scripts/init-clickhouse.sh
docs/local-deployment.md
```

### Success Criteria

```text
All services start successfully.
Kafka topics are created.
ClickHouse database `btc` is created.
A sample event can flow through Kafka.
A sample row can be inserted into ClickHouse.
Grafana datasource can connect to ClickHouse.
```

---

## Stage 2 — Local Team Integration

### Goal

Allow the ingestion and modeling teams to test against the local infrastructure contract.

### Integration Points

For the ingestion team:

```text
Kafka bootstrap server
Kafka topic names
ClickHouse host
ClickHouse database
Raw table schemas
```

For the modeling team:

```text
Spark master URL
Feature table schema
Prediction table schema
Prediction error table schema
Model artifact path convention
```

### Expected Output

```text
Ingestion team can send sample OHLCV/on-chain events.
Modeling team can run a Spark test job.
Prediction output can be written to ClickHouse.
Dashboard can read sample prediction output.
```

### Deliverables

```text
docs/service-contract.md
docs/local-integration-guide.md
sample payloads
sample ClickHouse insert query
sample Spark read/write test
```

---

## Stage 3 — Cloud Infrastructure Planning

### Goal

Define the DigitalOcean infrastructure before provisioning.

### Cloud Provider

```text
Provider: DigitalOcean
Region: Singapore
Target style: short-lived cloud lab
Provisioning: Terraform
```

### Initial Cloud Topology

```text
kafka-1
kafka-2
kafka-3
spark-master
spark-worker-1
analytics-node
```

### Service Placement

```text
kafka-1          Kafka broker 1
kafka-2          Kafka broker 2
kafka-3          Kafka broker 3
spark-master     Spark master + job submit node
spark-worker-1   Spark worker
analytics-node   ClickHouse + Grafana + Streamlit + reverse proxy
```

### Domain Plan

Domain managed in Cloudflare:

```text
faridwajdi.web.id
```

Suggested subdomains:

```text
grafana-btc.faridwajdi.web.id
stream-btc.faridwajdi.web.id
```

### Deliverables

```text
docs/cloud-architecture.md
docs/cost-guardrails.md
docs/security-plan.md
docs/teardown-guide.md
```

---

## Stage 4 — Terraform-Based DigitalOcean Deployment

### Goal

Provision cloud infrastructure reproducibly using Terraform.

### Terraform Scope

Terraform should manage:

```text
DigitalOcean project
VPC
SSH key reference
firewall rules
droplets
tags
outputs
```

Terraform should not manage:

```text
raw data
model artifacts
database contents
application secrets committed to Git
```

### Suggested Terraform Structure

```text
infra/terraform/
├── providers.tf
├── variables.tf
├── outputs.tf
├── project.tf
├── vpc.tf
├── firewall.tf
├── droplets.tf
├── cloud-init/
│   └── base.yaml
└── terraform.tfvars.example
```

### Expected Outputs

```text
kafka_private_ips
spark_master_private_ip
spark_worker_private_ip
analytics_private_ip
analytics_public_ip
grafana_url
streamlit_url
ssh_commands
```

### Success Criteria

```text
Terraform can create all nodes.
All nodes are reachable by SSH.
All nodes can communicate over private network.
Terraform can destroy all resources cleanly.
```

---

## Stage 5 — Service Deployment and Hardening

### Goal

Deploy services to the cloud nodes and apply basic hardening.

### Kafka

```text
3 brokers
replication factor 3
minimum in-sync replicas 2
topics created for market, on-chain, prediction, and deadletter events
```

### Spark

```text
Spark master
Spark worker
job submission path
Kafka connector available
ClickHouse write path available
```

### ClickHouse

```text
database: btc
raw tables
feature tables
prediction tables
error tables
model metrics table
pipeline metrics table
```

### Grafana / Streamlit

```text
Grafana datasource connected to ClickHouse
Streamlit reads from ClickHouse
public access through domain or restricted IP
```

### Security Baseline

```text
SSH key only
firewall enabled
Kafka private only
Spark private only
ClickHouse private only
Grafana/Streamlit protected
secrets stored in .env, not Git
```

---

## Stage 6 — Teardown and Cost Control

### Goal

Prevent cloud credit waste.

### Rule

The cloud environment is ephemeral.

```text
Create infrastructure.
Run experiment.
Capture metrics and screenshots.
Destroy infrastructure.
Verify billing/resource page.
```

### Required Teardown Steps

```text
terraform destroy
check droplets
check volumes
check snapshots
check reserved IPs
check load balancers
check billing/credits
```

### Cost Guardrail

Do not leave droplets running overnight unless explicitly required.

If the project is idle:

```text
destroy the infrastructure
```

---

## Infrastructure Roadmap

### MVP Infrastructure

```text
Local Docker Compose
Kafka single broker or local cluster
Spark local/standalone
ClickHouse
Grafana
Streamlit
Service contract validated
```

### Cloud MVP

```text
DigitalOcean droplets
Terraform provisioning
Kafka 3 brokers
Spark master + worker
ClickHouse analytics node
Grafana/Streamlit access
```

### Extended Infrastructure

```text
Prometheus
Node exporter
Kafka exporter
ClickHouse exporter
Grafana infrastructure dashboard
Cloudflare DNS
Reverse proxy with TLS
```

### Advanced Infrastructure

```text
multiple Spark workers
larger ClickHouse node
benchmark replay mode
automated deployment scripts
CI validation for Terraform
scheduled teardown reminders
```

---

## Current Next Step

The next implementation step is to create a minimal local infrastructure baseline.

Recommended first technical deliverables:

```text
docker-compose.yml
clickhouse/schema.sql
scripts/create-kafka-topics.sh
.env.example
README.md initial infrastructure usage
```

These files should be created with atomic commits.
