locals {
  sandbox_expiration_ms = 60 * 24 * 60 * 60 * 1000

  retail_datasets = {
    retail_bronze = {
      friendly_name = "Retail Bronze"
      description   = "Typed CDC events delivered from Kafka through Flink SQL."
      layer         = "bronze"
    }

    retail_silver = {
      friendly_name = "Retail Silver"
      description   = "Tested current-state and historical models transformed by dbt."
      layer         = "silver"
    }

    retail_gold = {
      friendly_name = "Retail Gold"
      description   = "Analytics-ready dimensions, facts, wide tables, and semantic models."
      layer         = "gold"
    }

    retail_realtime = {
      friendly_name = "Retail Realtime"
      description   = "Low-latency event-derived metrics produced by Flink SQL."
      layer         = "realtime"
    }
  }
}

resource "google_bigquery_dataset" "retail" {
  for_each = local.retail_datasets

  project       = var.project_id
  dataset_id    = each.key
  friendly_name = each.value.friendly_name
  description   = each.value.description
  location      = var.location

  default_table_expiration_ms     = local.sandbox_expiration_ms
  default_partition_expiration_ms = local.sandbox_expiration_ms
  max_time_travel_hours           = 168

  is_case_insensitive        = false
  delete_contents_on_destroy = false
  deletion_policy            = "PREVENT"

  labels = {
    data_layer  = each.value.layer
    environment = "development"
    managed_by  = "terraform"
  }

  lifecycle {
    prevent_destroy = true

    # Preserve access automatically created by BigQuery and existing users.
    # Dataset IAM will be managed separately using additive IAM resources.
    ignore_changes = [access]
  }
}