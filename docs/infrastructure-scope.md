# Infrastructure Scope

## Project

`bitcoin-realtime-forecasting-platform`

## Role

This repository focuses on infrastructure responsibilities for the Bitcoin realtime forecasting project.

The infrastructure layer provides the runtime platform for data ingestion, Spark processing, ClickHouse storage, and dashboard access.

## Infrastructure Responsibilities

The infrastructure scope includes:

- Provisioning cloud infrastructure
- Setting up Kafka
- Setting up Spark
- Setting up ClickHouse
- Setting up Grafana and Streamlit access
- Managing private networking and firewall rules
- Defining service endpoints
- Preparing deployment and teardown workflow
- Documenting integration contracts for ingestion and modeling teams

## Out of Scope

The infrastructure scope does not include:

- Owning raw historical datasets
- Cleaning OHLCV, block, or transaction data
- Training the final machine learning model
- Optimizing prediction accuracy
- Maintaining notebooks for model experimentation
- Storing large datasets inside the repository

## Integration Model

Historical data is handled by the data ingestion team.

Model training and evaluation are handled by the modeling team.

The infrastructure team provides:

- Kafka topics
- Spark runtime
- ClickHouse databases and tables
- Dashboard access
- Deployment documentation
- Service contract documentation

## Default Data Flow

Historical data:

```text
Data ingestion team
        ↓
ClickHouse / Parquet storage
        ↓
Spark batch processing
        ↓
Feature tables
```

Realtime or replay data:

```text
API / replay producer
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
ClickHouse
        ↓
Grafana / Streamlit
```

## Infrastructure Principle

The repository should grow only when the infrastructure requires it.

Do not create folders for data, notebooks, models, or application code unless they become part of the infrastructure workflow.

## Collaboration Boundary

The infrastructure team should provide stable interfaces, not own every part of the system.

Required infrastructure outputs:

```text
Kafka bootstrap servers
Kafka topic names
Spark master endpoint
ClickHouse host, database, and table names
Grafana URL
Streamlit URL
Deployment guide
Teardown guide
Service contract
```

Required inputs from other teams:

```text
Historical data format
Batch ingestion requirement
Model runtime dependency
Prediction output schema
Dashboard data requirements
```

## First Infrastructure Milestone

The first infrastructure milestone is not full production deployment.

The first milestone is:

```text
A minimal deployable platform where Kafka, Spark, ClickHouse, and dashboard services can run and communicate using documented endpoints.
```

After that, the infrastructure can evolve toward:

```text
multi-node deployment
Terraform provisioning
domain and reverse proxy
monitoring
benchmarking
cloud cost guardrails
```
