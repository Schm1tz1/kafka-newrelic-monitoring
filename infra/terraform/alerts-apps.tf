# Optional (var.app_alerts, default false). Application (client) alerts, kept apart from the Confluent Cloud infra policy.
# Metrics come from the Python/Java OTel instrumentation (kafka_app.*), see the java/python dashboards.
# Thresholds are starting points: tune to the application SLO.
locals {
  app_alerts = {
    producer_errors = {
      name        = "Producer delivery error ratio high"
      description = "Percent of produced records that failed delivery, per service."
      query       = "SELECT 100 * sum(kafka_app.producer_delivery_errors) / sum(kafka_app.produced_records) FROM Metric FACET service.name"
      critical    = { threshold = 5, duration = 300 }
      warning     = { threshold = 1, duration = 300 }
    }
    producer_latency = {
      name        = "Producer delivery latency high"
      description = "p95 delivery latency (ms) per service."
      query       = "SELECT percentile(kafka_app.producer_delivery_latency_ms, 95) FROM Metric FACET service.name"
      critical    = { threshold = 1000, duration = 300 } # placeholder, replace with SLO
    }
    consumer_lag = {
      name        = "Application consumer lag high"
      description = "Application-side lag estimate in records per service, topic and consumer group."
      query       = "SELECT max(kafka_app.consumer_lag_records) FROM Metric FACET service.name, topic, `consumer.group`"
      critical    = { threshold = 10000, duration = 600 } # placeholder, replace with SLO
    }
  }
}

resource "newrelic_alert_policy" "apps" {
  count               = var.app_alerts ? 1 : 0
  name                = "Kafka Clients"
  incident_preference = "PER_CONDITION_AND_TARGET"
}

resource "newrelic_nrql_alert_condition" "apps" {
  # for-expression, not a ternary: the 3 objects differ in shape (warning is optional), so `? :` has no common type.
  for_each = { for k, v in local.app_alerts : k => v if var.app_alerts }

  policy_id                    = newrelic_alert_policy.apps[0].id
  type                         = "static"
  name                         = each.value.name
  description                  = each.value.description
  aggregation_window           = 300
  aggregation_method           = "event_flow"
  aggregation_delay            = 120
  violation_time_limit_seconds = 86400

  nrql {
    query = each.value.query
  }

  critical {
    operator              = "above"
    threshold             = each.value.critical.threshold
    threshold_duration    = each.value.critical.duration
    threshold_occurrences = "all"
  }

  dynamic "warning" {
    for_each = try([each.value.warning], [])
    content {
      operator              = "above"
      threshold             = warning.value.threshold
      threshold_duration    = warning.value.duration
      threshold_occurrences = "all"
    }
  }
}

resource "newrelic_workflow" "apps" {
  count                 = var.app_alerts ? 1 : 0
  name                  = "Kafka Clients alerts"
  muting_rules_handling = "NOTIFY_ALL_ISSUES"

  issues_filter {
    name = "Kafka Clients policy"
    type = "FILTER"

    predicate {
      attribute = "labels.policyIds"
      operator  = "EXACTLY_MATCHES"
      values    = [newrelic_alert_policy.apps[0].id]
    }
  }

  destination {
    channel_id = newrelic_notification_channel.email.id
  }
}
