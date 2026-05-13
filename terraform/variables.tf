variable "admin_password" {
  description = "Clickhouse admin password"
  type        = string
  sensitive   = true
}

output "clickhouse_host_fqdn" {
  value = yandex_mdb_clickhouse_cluster.clickhouse_starschema.host[0].fqdn
}
