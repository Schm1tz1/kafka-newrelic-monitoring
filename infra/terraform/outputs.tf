output "dashboard_permalinks" {
  value = { for k, d in newrelic_one_dashboard_json.this : k => d.permalink }
}
