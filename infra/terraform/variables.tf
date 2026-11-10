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

variable "alert_email" {
  type        = string
  default     = "alerts@example.com"
  description = "Email address notified by the example alert workflow. Replace the throwaway default."
}
