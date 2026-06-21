# MVP Demo Runbook

This runbook covers Track A for the MVP infrastructure phase.

Current scope:

- Kafka brokers on `kafka-1`, `kafka-2`, `kafka-3`
- Spark master and worker
- ClickHouse on `analytics-node`
- Grafana demo runtime on `analytics-node`
- Realtime market stream from Kafka to ClickHouse

Out of scope:

- Historical preprocessing
- Feature engineering
- Model training
- Model evaluation

## Endpoints

- Kafka bootstrap: private VPC addresses from `terraform output node_private_ips`
- Spark master UI: `http://<spark-master-private-ip>:8080`
- ClickHouse HTTP: `http://<analytics-node-private-ip>:8123`
- Grafana demo: `http://<analytics-node-public-ip>:3000`

## Pre-demo Checks

Run the consolidated health check:

```bash
scripts/check-infrastructure-health.sh
```

Confirm these services are running:

- Kafka container on all three brokers
- Spark master and worker containers
- ClickHouse container on analytics-node
- Grafana container on analytics-node

## Realtime Demo Flow

1. Verify Kafka topics exist.
2. Verify Spark worker is registered with Spark master.
3. Verify the market stream is running.
4. Produce a smoke event with the verification script.
5. Confirm the event appears once in `btc.realtime_market_events`.
6. Open the Grafana dashboard and confirm the realtime row count changes.

## Verification Commands

```bash
scripts/verify-kafka-cluster.sh
scripts/verify-spark-cluster.sh
scripts/verify-market-stream-pipeline.sh
```

## Rollback

If the demo runtime becomes unhealthy:

- restart the Grafana container on `analytics-node`
- restart the Spark market stream container on `spark-master`
- do not delete ClickHouse data
- do not destroy `analytics-node`

## Notes

The Grafana dashboard reads ClickHouse directly and is intended only as a lightweight MVP demo surface. It is not a substitute for the later historical preprocessing and modeling work.
