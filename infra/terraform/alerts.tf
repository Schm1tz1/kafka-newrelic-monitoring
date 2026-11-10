# Example alerts from the Confluent Cloud Metrics API baseline. Thresholds are
# starting points: tune them against the application SLO, cluster type and quotas.
# Metrics arrive under either Prometheus (confluent_kafka_server_*) or dotted
# (confluent.kafka.server.*) names, depending on the integration, so every query
# matches both, like the dashboards do.
locals {
  cluster = "(`kafka_id` OR `confluent.clusterId`)"

  alerts = {
    cluster_load = {
      name        = "Cluster load high"
      description = "Maximum broker utilization (0.0-1.0). Dedicated clusters only."
      query       = "SELECT max(confluent_kafka_server_cluster_load_percent_max OR confluent.kafka.server.cluster_load_percent_max) FROM Metric FACET ${local.cluster}"
      critical    = { threshold = 0.85, duration = 300 }
      warning     = { threshold = 0.75, duration = 900 }
    }
    consumer_lag = {
      name        = "Consumer lag high"
      description = "Max lag in offsets per consumer group and topic. Set the threshold from the application SLO."
      query       = "SELECT max(confluent_kafka_server_consumer_lag_offsets OR confluent.kafka.server.consumer_lag_offsets) FROM Metric FACET ${local.cluster}, (`consumer_group_id` OR `confluent.kafka.server.metric.consumer_group_id`), (`topic` OR `confluent.kafka.server.metric.topic`)"
      critical    = { threshold = 10000, duration = 600 } # placeholder, replace with SLO
    }
    rebalance = {
      name        = "Consumer group rebalance pending"
      description = "Maximum pending rebalance time (ms) per consumer group."
      query       = "SELECT max(confluent_kafka_server_max_pending_rebalance_time_milliseconds OR confluent.kafka.server.max_pending_rebalance_time_milliseconds) FROM Metric FACET ${local.cluster}, (`consumer_group_id` OR `confluent.kafka.server.metric.consumer_group_id`)"
      critical    = { threshold = 30000, duration = 300 }
      warning     = { threshold = 5000, duration = 300 }
    }
    throttling = {
      name        = "Client throttling"
      description = "Sustained non-zero throttle time (ms). The reason facet shows the cause, e.g. cluster_quota_violation or principal_quota_violation."
      query       = "SELECT max(confluent_kafka_server_client_limit_milliseconds OR confluent.kafka.server.client_limit_milliseconds) FROM Metric FACET ${local.cluster}, (`principal_id` OR `confluent.kafka.server.metric.principal_id`), (`reason` OR `confluent.kafka.server.metric.reason`)"
      critical    = { threshold = 0, duration = 900 }
    }
    hot_partition_ingress = {
      name        = "Hot partition (ingress)"
      description = "Partition ingress hotspot indicator stays at 1."
      query       = "SELECT max(confluent_kafka_server_hot_partition_ingress OR confluent.kafka.server.hot_partition_ingress) FROM Metric FACET ${local.cluster}, (`topic` OR `confluent.kafka.server.metric.topic`)"
      critical    = { threshold = 0, duration = 900 }
    }
    hot_partition_egress = {
      name        = "Hot partition (egress)"
      description = "Partition egress hotspot indicator stays at 1."
      query       = "SELECT max(confluent_kafka_server_hot_partition_egress OR confluent.kafka.server.hot_partition_egress) FROM Metric FACET ${local.cluster}, (`topic` OR `confluent.kafka.server.metric.topic`)"
      critical    = { threshold = 0, duration = 900 }
    }
    active_connections = {
      name        = "Active connections near limit"
      description = "Set the threshold to ~80% of the connection limit for the cluster type."
      query       = "SELECT max(confluent_kafka_server_active_connection_count OR confluent.kafka.server.active_connection_count) FROM Metric FACET ${local.cluster}"
      critical    = { threshold = 800, duration = 600 } # placeholder, 80% of cluster limit
    }
    partition_count = {
      name        = "Partition count near limit"
      description = "Set the threshold to ~80% of the partition limit for the cluster type."
      query       = "SELECT max(confluent_kafka_server_partition_count OR confluent.kafka.server.partition_count) FROM Metric FACET ${local.cluster}"
      critical    = { threshold = 1600, duration = 900 } # placeholder, 80% of cluster limit
    }
    # Audit-log based (needs the audit log pipeline, see ccloud-logs dashboard). Counts per 5m window.
    # Keep the backticks around dotted attribute names: without them these conditions never
    # emitted a signal and never fired, even though the same query worked in the query builder.
    authz_denied = {
      name        = "Authorization denials"
      description = "Authorization denials per principal in 5 minutes: Schema Registry DENY or RBAC granted=False."
      query       = "SELECT count(*) FROM Log WHERE `confluent.log.source` = 'confluent-audit' AND (`data.authorizationInfo.result` = 'DENY' OR `data.authorizationInfo.granted` = 'false') FACET `crn.cloud_cluster`, `data.authenticationInfo.principal`"
      timer       = 60                                 # sparse log signal, see event_timer note below
      critical    = { threshold = 20, duration = 300 } # placeholder, tune to baseline
      warning     = { threshold = 5, duration = 300 }
    }
    auth_failed = {
      name        = "Failed authentication"
      description = "Failed authentications in 5 minutes: `data.result.status` = 'UNAUTHENTICATED' (bad API key/secret, principal is None:UNKNOWN_USER) or an authenticationInfo.errorMessage (e.g. JWT_EXPIRED)."
      query       = "SELECT count(*) FROM Log WHERE `confluent.log.source` = 'confluent-audit' AND (`data.result.status` = 'UNAUTHENTICATED' OR `data.authenticationInfo.errorMessage` IS NOT NULL) FACET `crn.cloud_cluster`"
      timer       = 60                                 # sparse log signal, see event_timer note below
      critical    = { threshold = 20, duration = 300 } # placeholder, tune to baseline
      warning     = { threshold = 5, duration = 300 }
    }
    operation_failed = {
      name        = "Failed operations"
      description = "Authorized operations that failed at execution in 5 minutes (`data.result.status` = 'FAILURE'), e.g. quota exceeded, invalid config."
      query       = "SELECT count(*) FROM Log WHERE `confluent.log.source` = 'confluent-audit' AND `data.result.status` = 'FAILURE' FACET `crn.cloud_cluster`, `data.authenticationInfo.principal`"
      timer       = 60                                 # sparse log signal, see event_timer note below
      critical    = { threshold = 20, duration = 300 } # placeholder, tune to baseline
      warning     = { threshold = 5, duration = 300 }
    }
    # Ingest-loss (dead man's switch): no data = no signal, so these use signal-loss expiration.
    metric_ingest_loss = {
      name        = "Confluent metric ingest stopped"
      description = "No Confluent metric arrived for 10 minutes: broken Metrics API integration or Collector."
      query       = "SELECT count(*) FROM Metric WHERE metricName LIKE 'confluent%kafka%'"
      timer       = 60
      expiration  = 600
      critical    = { operator = "below", threshold = 1, duration = 300 }
    }
    audit_ingest_loss = {
      name        = "Confluent audit log ingest stopped"
      description = "No confluent-audit log arrived for 30 minutes: broken log pipeline (or an idle cluster, tune for quiet dev clusters)."
      query       = "SELECT count(*) FROM Log WHERE `confluent.log.source` = 'confluent-audit'"
      timer       = 60
      expiration  = 1800
      critical    = { operator = "below", threshold = 1, duration = 300 }
    }
    # Connect, ksqlDB, Flink: only fire if the service is used (no data = no violation).
    connector_failed = {
      name        = "Connector failed"
      description = "Connector in status FAILED."
      query       = "SELECT sum(confluent_kafka_connect_connector_status OR confluent.kafka.connect.connector_status) FROM Metric WHERE (`status` = 'FAILED' OR `confluent.kafka.connect.metric.status` = 'FAILED') FACET (`connector_id` OR `confluent.resourceId`)"
      critical    = { threshold = 0, duration = 300 }
    }
    connector_task_failed = {
      name        = "Connector task failed"
      description = "Connector task in status FAILED."
      query       = "SELECT sum(confluent_kafka_connect_connector_task_status OR confluent.kafka.connect.connector_task_status) FROM Metric WHERE (`status` = 'FAILED' OR `confluent.kafka.connect.metric.status` = 'FAILED') FACET (`connector_id` OR `confluent.resourceId`), (`task` OR `confluent.kafka.connect.metric.task`)"
      critical    = { threshold = 0, duration = 300 }
    }
    ksql_lag = {
      name        = "ksqlDB consumer lag high"
      description = "Committed offset lag per ksqlDB query."
      query       = "SELECT max(confluent_kafka_ksql_committed_offset_lag OR confluent.kafka.ksql.committed_offset_lag) FROM Metric FACET (`ksql_id` OR `confluent.resourceId`), (`query_id` OR `confluent.kafka.ksql.metric.query_id`)"
      critical    = { threshold = 10000, duration = 600 } # placeholder, replace with SLO
    }
    flink_pending = {
      name        = "Flink pending records high"
      description = "Records waiting to be processed per Flink compute pool."
      query       = "SELECT max(confluent_kafka_flink_pending_records OR confluent.kafka.flink.pending_records) FROM Metric FACET (`compute_pool_id` OR `confluent.clusterId`)"
      critical    = { threshold = 100000, duration = 600 } # placeholder, replace with SLO
    }
  }
}

resource "newrelic_alert_policy" "ccloud" {
  name                = "Confluent Cloud"
  incident_preference = "PER_CONDITION_AND_TARGET" # one incident per cluster/group/topic facet
}

resource "newrelic_nrql_alert_condition" "ccloud" {
  for_each = local.alerts

  policy_id   = newrelic_alert_policy.ccloud.id
  type        = "static"
  name        = each.value.name
  description = each.value.description

  # Metrics API data lands a few minutes late; 5m windows smooth single samples.
  # Log-count alerts are sparse (no matching event = no data), and event_flow only closes a
  # window when a later event arrives, so a one-off burst never fires. Those use event_timer.
  aggregation_window           = 300
  aggregation_method           = try(each.value.timer, null) == null ? "event_flow" : "event_timer"
  aggregation_delay            = try(each.value.timer, null) == null ? 300 : null
  aggregation_timer            = try(each.value.timer, null)
  violation_time_limit_seconds = 86400

  nrql {
    query = each.value.query
  }

  expiration_duration            = try(each.value.expiration, null)
  open_violation_on_expiration   = try(each.value.expiration, null) == null ? null : true
  close_violations_on_expiration = try(each.value.expiration, null) == null ? null : false

  critical {
    operator              = try(each.value.critical.operator, "above")
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

# Example notification path: email destination -> channel -> workflow on the policy.
resource "newrelic_notification_destination" "email" {
  name = "Confluent Cloud alerts email"
  type = "EMAIL"

  property {
    key   = "email"
    value = var.alert_email
  }
}

resource "newrelic_notification_channel" "email" {
  name           = "Confluent Cloud alerts email"
  type           = "EMAIL"
  product        = "IINT"
  destination_id = newrelic_notification_destination.email.id

  property {
    key   = "subject"
    value = "{{ issueTitle }}"
  }
}

resource "newrelic_workflow" "ccloud" {
  name                  = "Confluent Cloud alerts"
  muting_rules_handling = "NOTIFY_ALL_ISSUES"

  issues_filter {
    name = "Confluent Cloud policy"
    type = "FILTER"

    predicate {
      attribute = "labels.policyIds"
      operator  = "EXACTLY_MATCHES"
      values    = [newrelic_alert_policy.ccloud.id]
    }
  }

  destination {
    channel_id = newrelic_notification_channel.email.id
  }
}
