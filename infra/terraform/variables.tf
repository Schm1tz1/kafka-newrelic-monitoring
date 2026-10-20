variable "account_id" {
  type        = number
  description = "New Relic account ID the dashboards are created in."
}

variable "api_key" {
  type        = string
  sensitive   = true
  description = "New Relic user API key (NRAK-...)."
}

variable "region" {
  type        = string
  default     = "US"
  description = "New Relic region: US or EU."

  validation {
    condition     = contains(["US", "EU"], var.region)
    error_message = "The region must be US or EU."
  }
}
