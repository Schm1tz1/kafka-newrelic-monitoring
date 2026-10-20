locals {
  # One entry per template in infra/dashboards/. Add a file there to add a dashboard.
  dashboards = toset(["cp", "ccloud", "ccloud-logs", "java", "python"])
}

# Templates only substitute ${account_id}. The {{variable}} placeholders in the
# NRQL queries are New Relic dashboard variables and pass through untouched.
# Each template sets "permissions": "PUBLIC_READ_ONLY" (everyone can view, only the creator can
# edit). To restrict further, set it to "PRIVATE" (only the creator sees it);
# "PUBLIC_READ_WRITE" lets anyone in the account edit.
resource "newrelic_one_dashboard_json" "this" {
  for_each = local.dashboards

  json = templatefile("${path.module}/../dashboards/${each.key}.json.tftpl", {
    account_id = var.account_id
  })
}

resource "newrelic_entity_tags" "this" {
  for_each = local.dashboards

  guid = newrelic_one_dashboard_json.this[each.key].guid

  tag {
    key    = "terraform"
    values = ["true"]
  }
}
