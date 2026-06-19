# Node Runtime Bootstrap Verification

## Project

`bitcoin-realtime-forecasting-platform`

## Verification Purpose

This document records the successful runtime bootstrap milestone for all DigitalOcean nodes.

The goal of this milestone is to verify that every provisioned node has the required base runtime to support later deployment of Kafka, Spark, ClickHouse, Grafana, and Streamlit.

## Scope

Runtime bootstrap installs and verifies:

```text
base Linux utilities
Java 17
Python 3
Docker Engine
Docker Compose plugin
standard runtime directory layout
```

## Bootstrap Scripts

The following scripts were added:

```text
scripts/bootstrap-node-runtime.sh
scripts/bootstrap-all-nodes.sh
scripts/verify-node-runtime.sh
```

### Script Responsibilities

`bootstrap-node-runtime.sh`

```text
Runs on one remote node.
Installs Docker, Docker Compose plugin, Java, Python, and base utilities.
Creates /opt/bitcoin-realtime-forecasting-platform.
Writes runtime version information.
Runs a Docker hello-world test.
```

`bootstrap-all-nodes.sh`

```text
Runs from the local machine.
Reads Terraform output.
Copies bootstrap-node-runtime.sh to every node.
Executes bootstrap on every node over SSH.
```

`verify-node-runtime.sh`

```text
Runs from the local machine.
Reads Terraform output.
SSHes into every node.
Prints node role, runtime versions, and Docker container status.
```

## Verified Nodes

The following nodes were bootstrapped:

```text
kafka-1
kafka-2
kafka-3
spark-master
spark-worker-1
analytics-node
```

## Runtime Directory

Each node should contain:

```text
/opt/bitcoin-realtime-forecasting-platform/
├── configs/
├── data/
├── logs/
├── scripts/
├── services/
└── runtime-versions.txt
```

## Runtime Verification

Each node should report:

```text
Java 17
Python 3
Docker Engine
Docker Compose plugin
```

Verification command:

```bash
scripts/verify-node-runtime.sh infra/terraform
```

Expected result:

```text
All nodes return runtime version information successfully.
Docker command is available.
Docker Compose plugin is available.
SSH connectivity is working.
```

## Docker Test

The bootstrap process runs:

```bash
docker run --rm hello-world
```

Success means:

```text
Docker daemon is active.
The node can pull Docker images.
The node can run containers.
```

## Git Commit

Bootstrap scripts were committed with:

```text
chore: add node runtime bootstrap scripts
```

## Current Milestone Status

```text
Terraform foundation: complete
Node SSH access: complete
Private network verification: complete
Runtime bootstrap: complete
Docker verification: complete
```

## Next Step

The next milestone is service deployment.

Recommended order:

```text
1. Deploy Kafka cluster
2. Create Kafka topics
3. Validate producer/consumer test
4. Deploy ClickHouse
5. Create btc database and initial schema
6. Deploy Spark master/worker service
7. Validate Spark connectivity
8. Deploy Grafana/Streamlit later
```

The immediate next technical step should be:

```text
Deploy Kafka across kafka-1, kafka-2, and kafka-3.
```

## Operational Reminder

The DigitalOcean environment is still running and consuming credit.

When stopping work for the day, either:

```text
Keep it running only if the next session is soon and intentional.
```

or destroy it:

```bash
cd infra/terraform
terraform destroy
```

After destroy, verify manually in DigitalOcean:

```text
Droplets
Volumes
Snapshots
Reserved IPs
Load balancers
Billing / Credits
```
