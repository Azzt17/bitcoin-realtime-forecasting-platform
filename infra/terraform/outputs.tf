output "node_public_ips" {
  description = "Public IPv4 address for each node."
  value = {
    for name, droplet in digitalocean_droplet.nodes :
    name => droplet.ipv4_address
  }
}

output "node_private_ips" {
  description = "Private IPv4 address for each node."
  value = {
    for name, droplet in digitalocean_droplet.nodes :
    name => droplet.ipv4_address_private
  }
}

output "ssh_commands" {
  description = "SSH commands for all nodes."
  value = {
    for name, droplet in digitalocean_droplet.nodes :
    name => "ssh root@${droplet.ipv4_address}"
  }
}

output "analytics_public_ip" {
  description = "Public IP address for analytics-node."
  value       = digitalocean_droplet.nodes["analytics-node"].ipv4_address
}

output "grafana_url" {
  description = "Temporary Grafana access URL."
  value       = "http://${digitalocean_droplet.nodes["analytics-node"].ipv4_address}:3000"
}
