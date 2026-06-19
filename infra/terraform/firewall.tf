resource "digitalocean_firewall" "main" {
  name = "${var.project_name}-firewall"
  tags = [digitalocean_tag.project.name]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = [var.trusted_ssh_cidr]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "80"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "443"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  # Temporary dashboard access during development. Restrict to trusted IP.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "3000"
    source_addresses = [var.trusted_ssh_cidr]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8501"
    source_addresses = [var.trusted_ssh_cidr]
  }

  # Internal service communication over VPC.
  inbound_rule {
    protocol         = "tcp"
    port_range       = "1-65535"
    source_addresses = [var.vpc_ip_range]
  }

  inbound_rule {
    protocol         = "udp"
    port_range       = "1-65535"
    source_addresses = [var.vpc_ip_range]
  }

  inbound_rule {
    protocol         = "icmp"
    source_addresses = [var.vpc_ip_range]
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}
