resource "digitalocean_ssh_key" "main" {
  name       = "${var.project_name}-ssh-key"
  public_key = file(pathexpand(var.ssh_public_key_path))
}
