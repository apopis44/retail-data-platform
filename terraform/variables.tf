variable "project_id" {
  description = "Google Cloud project containing the retail data platform."
  type        = string
  nullable    = false

  validation {
    condition     = length(trimspace(var.project_id)) > 0
    error_message = "project_id must not be empty."
  }
}

variable "location" {
  description = "BigQuery location shared by all retail datasets."
  type        = string
  default     = "US"
  nullable    = false

  validation {
    condition     = length(trimspace(var.location)) > 0
    error_message = "location must not be empty."
  }
}