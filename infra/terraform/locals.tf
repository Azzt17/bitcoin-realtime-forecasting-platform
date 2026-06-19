locals {
  common_tags = [
    digitalocean_tag.project.name,
    digitalocean_tag.environment.name,
    digitalocean_tag.managed_by.name
  ]
}
