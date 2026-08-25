locals {
  flink_writer_datasets = toset([
    "retail_bronze",
    "retail_realtime",
  ])

  dbt_write_datasets = toset([
    "retail_silver",
    "retail_gold",
  ])
}

# Flink can create, update, and write tables only in Bronze and Realtime.
resource "google_bigquery_dataset_iam_member" "flink_data_editor" {
  for_each = local.flink_writer_datasets

  project    = var.project_id
  dataset_id = google_bigquery_dataset.retail[each.value].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.flink_writer.member
}

# dbt can read Bronze source tables but cannot modify them.
resource "google_bigquery_dataset_iam_member" "dbt_bronze_viewer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.retail["retail_bronze"].dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = google_service_account.dbt_transformer.member
}

# dbt can build, replace, test, and read models in Silver and Gold.
resource "google_bigquery_dataset_iam_member" "dbt_data_editor" {
  for_each = local.dbt_write_datasets

  project    = var.project_id
  dataset_id = google_bigquery_dataset.retail[each.value].dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = google_service_account.dbt_transformer.member
}

# BigQuery jobs are created at project scope.
resource "google_project_iam_member" "dbt_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = google_service_account.dbt_transformer.member
}