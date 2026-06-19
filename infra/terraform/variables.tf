variable "do_token" {
  description = "DigitalOcean API token. Pass via TF_VAR_do_token or terraform.tfvars. Do not commit secrets."
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "DigitalOcean project name."
  type        = string
  default     = "bitcoin-realtime-forecasting-platform"
}

variable "region" {
  description = "DigitalOcean region slug."
  type        = string
  default     = "sgp1"
}

variable "vpc_ip_range" {
  description = "Private VPC IP range for the project."
  type        = string
  default     = "10.10.0.0/16"
}

variable "ssh_public_key_path" {
  description = "Path to the public SSH key used for Droplet access."
  type        = string
  default     = "~/.ssh/bitcoin_realtime_platform.pub"
}

variable "trusted_ssh_cidr" {
  description = "Trusted public CIDR allowed to SSH into droplets. Example: 203.0.113.10/32"
  type        = string
}

variable "droplet_image" {
  description = "Base Droplet image."
  type        = string
  default     = "ubuntu-24-04-x64"
}

variable "droplets" {
  description = "Droplet definitions for the initial multi-node infrastructure."
  type = map(object({
    size = string
    role = string
  }))

  default = {
    kafka-1 = {
      size = "s-2vcpu-4gb"
      role = "kafka"
    }
    kafka-2 = {
      size = "s-2vcpu-4gb"
      role = "kafka"
    }
    kafka-3 = {
      size = "s-2vcpu-4gb"
      role = "kafka"
    }
    spark-master = {
      size = "s-2vcpu-4gb"
      role = "spark-master"
    }
    spark-worker-1 = {
      size = "s-4vcpu-8gb"
      role = "spark-worker"
    }
    analytics-node = {
      size = "s-4vcpu-8gb"
      role = "analytics"
    }
  }
}
