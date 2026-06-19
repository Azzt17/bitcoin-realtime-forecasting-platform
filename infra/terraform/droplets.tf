resource "digitalocean_droplet" "nodes" {
  for_each = var.droplets

  name     = "${var.project_name}-${each.key}"
  image    = var.droplet_image
  region   = var.region
  size     = each.value.size
  vpc_uuid = digitalocean_vpc.main.id

  ssh_keys = [
    digitalocean_ssh_key.main.fingerprint
  ]

  user_data = templatefile("${path.module}/cloud-init/base.yaml", {
    node_name = each.key
    node_role = each.value.role
  })

  tags = concat(local.common_tags, [
    "role-${each.value.role}"
  ])
}
