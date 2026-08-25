output "bigquery_datasets" {
  description = "BigQuery datasets managed by Terraform"

  value = {
    for name, dataset in google_bigquery_dataset.retail : name => {
      id            = dataset.id
      dataset_id    = dataset.dataset_id
      friendly_name = dataset.friendly_name
      location      = dataset.location
    }
  }
}