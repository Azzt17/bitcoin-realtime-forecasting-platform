# Terraform Infrastructure

This directory provisions the initial DigitalOcean foundation for `bitcoin-realtime-forecasting-platform`.

## Scope

This Terraform phase creates:

- DigitalOcean project
- VPC in Singapore
- SSH key registration
- Firewall rules
- Six Droplets:
  - kafka-1
  - kafka-2
  - kafka-3
  - spark-master
  - spark-worker-1
  - analytics-node

It does not deploy Kafka, Spark, ClickHouse, Grafana, or Streamlit yet. Service deployment comes after the cloud foundation is verified.

## Prerequisites

- Terraform installed
- DigitalOcean API token
- SSH key pair dedicated to this project
- Your current public IP address in CIDR format

## Prepare SSH Key

```bash
ssh-keygen -t ed25519 -f ~/.ssh/bitcoin_realtime_platform -C "bitcoin-realtime-platform"
```

## Configure Variables

```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars`:

```hcl
do_token = "dop_v1_xxx"
trusted_ssh_cidr = "YOUR_PUBLIC_IP/32"
ssh_public_key_path = "~/.ssh/bitcoin_realtime_platform.pub"
region = "sgp1"
```

## Run

```bash
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
terraform output
```

## Destroy

```bash
terraform destroy
```

After destroy, manually verify in DigitalOcean that Droplets, volumes, snapshots, reserved IPs, and load balancers are not left running.

## Security Notes

Do not commit:

- `terraform.tfvars`
- Terraform state files
- API tokens
- SSH private keys
