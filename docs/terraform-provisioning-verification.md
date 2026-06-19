# Terraform Provisioning Verification

## Project

`bitcoin-realtime-forecasting-platform`

## Verification Purpose

This document records the first successful cloud infrastructure milestone.

The goal of this milestone is to verify that Terraform can provision the DigitalOcean foundation and that all nodes are reachable and correctly initialized before deploying Kafka, Spark, ClickHouse, Grafana, or Streamlit.

## Provisioning Scope

Terraform foundation includes:

```text
DigitalOcean Project
DigitalOcean VPC
SSH key registration
Firewall rules
Droplets
Terraform outputs
Cloud-init base provisioning
```

## Region

```text
DigitalOcean region: sgp1
Datacenter: Singapore
```

## Node Topology

Provisioned nodes:

```text
kafka-1
kafka-2
kafka-3
spark-master
spark-worker-1
analytics-node
```

## Expected Node Roles

```text
kafka-1          Kafka broker 1
kafka-2          Kafka broker 2
kafka-3          Kafka broker 3
spark-master     Spark master and job submit node
spark-worker-1   Spark worker
analytics-node   ClickHouse, Grafana, Streamlit, and reverse proxy node
```

## Terraform Plan Result

Terraform plan result:

```text
Plan: 14 to add, 0 to change, 0 to destroy
```

This means Terraform planned to create new infrastructure only and did not attempt to modify or destroy existing resources.

## Verification Commands

### Terraform Outputs

Run from `infra/terraform`:

```bash
terraform output node_public_ips
terraform output node_private_ips
terraform output ssh_commands
terraform output analytics_public_ip
terraform output grafana_url
terraform output streamlit_url
```

### SSH Verification

Example command pattern:

```bash
ssh -i ~/.ssh/bitcoin_realtime_platform root@<NODE_PUBLIC_IP> "cat /etc/bitcoin-platform-node && java -version && python3 --version"
```

This should be tested for:

```text
kafka-1
kafka-2
kafka-3
spark-master
spark-worker-1
analytics-node
```

### Analytics Node Verification

Verified output:

```text
NODE_NAME=analytics-node
NODE_ROLE=analytics
analytics-node (analytics) provisioned at Fri Jun 19 15:31:37 UTC 2026
```

Runtime verification:

```text
Java: OpenJDK 17
Python: Python 3.12.3
```

## Private Networking Verification

From `analytics-node`, verify private network connectivity to other nodes:

```bash
ping -c 3 <KAFKA_1_PRIVATE_IP>
ping -c 3 <KAFKA_2_PRIVATE_IP>
ping -c 3 <KAFKA_3_PRIVATE_IP>
ping -c 3 <SPARK_MASTER_PRIVATE_IP>
ping -c 3 <SPARK_WORKER_1_PRIVATE_IP>
```

Expected result:

```text
All nodes respond through private IP addresses.
```

## Public Ping Note

Public ICMP ping is not required to work.

The firewall intentionally allows ICMP only from the private VPC range, not from the public internet. Public ping failure does not indicate that the Droplet is unhealthy.

SSH is the correct public connectivity test.

## Security Verification

Current firewall posture:

```text
SSH allowed only from trusted public IP/CIDR
HTTP/HTTPS public access prepared for future reverse proxy
Grafana development port restricted to trusted IP
Streamlit development port restricted to trusted IP
Internal service ports allowed only through VPC
```

Internal services should not be publicly exposed:

```text
Kafka
Spark
ClickHouse
Internal monitoring services
```

## Git Safety Verification

Do not commit these files:

```text
terraform.tfvars
terraform.tfstate
terraform.tfstate.backup
tfplan
*.tfplan
.terraform/
```

Commit-safe files:

```text
.terraform.lock.hcl
Terraform .tf configuration files
terraform.tfvars.example
cloud-init templates
documentation
```

## Milestone Status

Status:

```text
Terraform cloud foundation: verified
SSH access: verified
Cloud-init base provisioning: verified
Private networking: verified
Provider lock file: committed
Plan files ignored: committed
```

## Next Infrastructure Step

Next step:

```text
Prepare node bootstrap for service runtime.
```

This means installing and validating runtime dependencies required for Kafka, Spark, ClickHouse, Grafana, and Streamlit deployment.

The next implementation should include:

```text
Docker installation strategy
Docker Compose plugin installation
directory layout on each node
base environment files
service deployment approach
```

## Operational Reminder

The cloud environment is ephemeral.

When the environment is not needed:

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
