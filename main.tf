terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
}


resource "yandex_mdb_clickhouse_cluster" "clickhouse_starschema" {
  name                    = "clickhouse_starschema"
  environment             = "PRESTABLE"
  network_id              = yandex_vpc_network.default_network.id
  sql_database_management = true
  sql_user_management     = true
  admin_password          = var.clickhouse_password
  version                 = "25.8"

  clickhouse {
    resources {
      resource_preset_id = "s3-c4-m16"
      disk_type_id       = "network-ssd"
      disk_size          = 64
    }

    config {
      log_level              = "TRACE"
      max_connections        = 128
      max_concurrent_queries = 128
      keep_alive_timeout     = 3000
    }
  }

  host {
    type             = "CLICKHOUSE"
    zone             = "ru-central1-b"
    subnet_id        = yandex_vpc_subnet.foo.id
    assign_public_ip = true
  }

  cloud_storage {
    enabled = false
  }

  maintenance_window {
    type = "ANYTIME"
  }
}

resource "yandex_vpc_network" "default_network" {}

resource "yandex_vpc_subnet" "foo" {
  zone           = "ru-central1-b"
  network_id     = yandex_vpc_network.default_network.id
  v4_cidr_blocks = ["10.5.0.0/24"]
}