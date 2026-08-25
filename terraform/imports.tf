import {
  to = google_bigquery_dataset.retail["retail_bronze"]
  id = "projects/${var.project_id}/datasets/retail_bronze"
}

import {
  to = google_bigquery_dataset.retail["retail_silver"]
  id = "projects/${var.project_id}/datasets/retail_silver"
}

import {
  to = google_bigquery_dataset.retail["retail_gold"]
  id = "projects/${var.project_id}/datasets/retail_gold"
}

import {
  to = google_bigquery_dataset.retail["retail_realtime"]
  id = "projects/${var.project_id}/datasets/retail_realtime"
}