resource "digitalocean_project" "main" {
  name        = var.project_name
  description = "Infrastructure for the Bitcoin realtime forecasting platform."
  purpose     = "Educational purposes"
  environment = "Development"
}

resource "digitalocean_tag" "project" {
  name = "bitcoin-realtime-forecasting-platform"
}

resource "digitalocean_tag" "environment" {
  name = "lab"
}

resource "digitalocean_tag" "managed_by" {
  name = "managed-by-terraform"
}

resource "digitalocean_project_resources" "main" {
  project = digitalocean_project.main.id

  resources = [
    for droplet in digitalocean_droplet.nodes : droplet.urn
  ]
}
