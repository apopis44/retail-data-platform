resource "google_service_account" "flink_writer" {
  project         = var.project_id
  account_id      = "retail-flink-writer"
  display_name    = "Retail Flink Writer"
  description     = "Writes low-latency CDC datasets and event-derived metrics to BigQuery."
  deletion_policy = "PREVENT"

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "dbt_transformer" {
  project         = var.project_id
  account_id      = "retail-dbt-transformer"
  display_name    = "Retail dbt Transformer"
  description     = "Runs dbt transformations and semantic validation through Dagster."
  deletion_policy = "PREVENT"


  lifecycle {
    prevent_destroy = true
  }
}