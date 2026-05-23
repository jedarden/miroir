{{/*
Expand the name of the chart.
*/}}
{{- define "miroir.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "miroir.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "miroir.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "miroir.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "miroir.selectorLabels" -}}
app.kubernetes.io/name: {{ include "miroir.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name
*/}}
{{- define "miroir.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "miroir.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Secret name
*/}}
{{- define "miroir.secretName" -}}
{{- if .Values.miroir.existingSecret }}
{{- .Values.miroir.existingSecret }}
{{- else }}
{{- include "miroir.fullname" . }}-secret
{{- end }}
{{- end }}

{{/*
Generate the full DNS address for a Meilisearch node.

Usage:
  {{ include "miroir.meilisearchNodeAddress" (dict "release" .Release "namespace" .Namespace "nodeIndex" 0) }}

Returns:
  http://release-name-meili-0.release-name-meili-headless.namespace.svc.cluster.local:7700
*/}}
{{- define "miroir.meilisearchNodeAddress" -}}
{{- $ns := .namespace | default "default" -}}
http://{{ .release.Name }}-meili-{{ .nodeIndex }}.{{ .release.Name }}-meili-headless.{{ $ns }}.svc.cluster.local:7700
{{- end -}}

{{/*
Generate the list of Meilisearch node addresses for the ConfigMap.

Usage:
  {{ include "miroir.meilisearchNodeList" $ }}

Returns a YAML-formatted list of node entries for the miroir config.
*/}}
{{- define "miroir.meilisearchNodeList" -}}
{{- $meiliReplicas := .Values.meilisearch.replicas | default 2 | int -}}
{{- $nodesPerGroup := .Values.meilisearch.nodesPerGroup | default 2 | int -}}
{{- $replicaGroups := .Values.miroir.replicaGroups | default 1 | int -}}
{{- range $group := until $replicaGroups -}}
{{- range $node := until $nodesPerGroup -}}
{{- $nodeIndex := add (mul $group $nodesPerGroup) $node }}
- id: "meili-{{ $nodeIndex }}"
  address: {{ include "miroir.meilisearchNodeAddress" (dict "release" $.Release "namespace" $.Namespace "nodeIndex" $nodeIndex) }}
  replica_group: {{ $group }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Generate the miroir YAML config for the ConfigMap.

Usage:
  {{ include "miroir.config" $ }}
*/}}
{{- define "miroir.config" -}}
shards: {{ .Values.miroir.shards | default 64 }}
replication_factor: {{ .Values.miroir.replicationFactor | default 1 }}
replica_groups: {{ .Values.miroir.replicaGroups | default 1 }}
nodes:
{{ include "miroir.meilisearchNodeList" . | indent 2 }}
task_store:
  backend: {{ .Values.taskStore.backend | default "sqlite" }}
  path: {{ .Values.taskStore.path | default "/data/miroir-tasks.db" }}
  {{- if eq (include "miroir.redisEnabled" .) "true" }}
  url: redis://{{ .Release.Name }}-redis.{{ .Release.Namespace | default "default" }}.svc.cluster.local:6379
  {{- end }}
health:
  interval_ms: 5000
  timeout_ms: 2000
  unhealthy_threshold: 3
  recovery_threshold: 2
scatter:
  node_timeout_ms: 5000
  retry_on_timeout: true
  unavailable_shard_policy: partial
rebalancer:
  auto_rebalance_on_recovery: true
  max_concurrent_migrations: 4
  migration_timeout_s: 3600
server:
  port: 7700
  bind: "0.0.0.0"
  max_body_bytes: {{ .Values.miroir.server.max_body_bytes | default 104857600 }}
  max_concurrent_requests: {{ .Values.miroir.server.max_concurrent_requests | default 500 }}
  request_timeout_ms: {{ .Values.miroir.server.request_timeout_ms | default 30000 }}
connection_pool_per_node:
  max_idle: {{ .Values.miroir.connection_pool_per_node.max_idle | default 32 }}
  max_total: {{ .Values.miroir.connection_pool_per_node.max_total | default 128 }}
  idle_timeout_s: {{ .Values.miroir.connection_pool_per_node.idle_timeout_s | default 60 }}
task_registry:
  cache_size: {{ .Values.miroir.task_registry.cache_size | default 10000 }}
  redis_pool_max: {{ .Values.miroir.task_registry.redis_pool_max | default 50 }}
  ttl_seconds: {{ .Values.miroir.task_registry.ttl_seconds | default 604800 }}
  prune_interval_s: {{ .Values.miroir.task_registry.prune_interval_s | default 300 }}
  prune_batch_size: {{ .Values.miroir.task_registry.prune_batch_size | default 10000 }}
idempotency:
  enabled: {{ .Values.miroir.idempotency.enabled | default true }}
  max_cached_keys: {{ .Values.miroir.idempotency.max_cached_keys | default 1000000 }}
  ttl_seconds: {{ .Values.miroir.idempotency.ttl_seconds | default 86400 }}
session_pinning:
  enabled: {{ .Values.miroir.session_pinning.enabled | default true }}
  ttl_seconds: {{ .Values.miroir.session_pinning.ttl_seconds | default 900 }}
  max_sessions: {{ .Values.miroir.session_pinning.max_sessions | default 100000 }}
  wait_strategy: {{ .Values.miroir.session_pinning.wait_strategy | default "block" }}
  max_wait_ms: {{ .Values.miroir.session_pinning.max_wait_ms | default 5000 }}
query_coalescing:
  enabled: {{ .Values.miroir.query_coalescing.enabled | default true }}
  window_ms: {{ .Values.miroir.query_coalescing.window_ms | default 50 }}
  max_subscribers: {{ .Values.miroir.query_coalescing.max_subscribers | default 1000 }}
  max_pending_queries: {{ .Values.miroir.query_coalescing.max_pending_queries | default 10000 }}
anti_entropy:
  enabled: {{ .Values.miroir.anti_entropy.enabled | default true }}
  schedule: {{ .Values.miroir.anti_entropy.schedule | default "every 6h" }}
  shards_per_pass: {{ .Values.miroir.anti_entropy.shards_per_pass | default 0 }}
  max_read_concurrency: {{ .Values.miroir.anti_entropy.max_read_concurrency | default 2 }}
  fingerprint_batch_size: {{ .Values.miroir.anti_entropy.fingerprint_batch_size | default 1000 }}
  auto_repair: {{ .Values.miroir.anti_entropy.auto_repair | default true }}
  updated_at_field: {{ .Values.miroir.anti_entropy.updated_at_field | default "_miroir_updated_at" }}
resharding:
  enabled: {{ .Values.miroir.resharding.enabled | default true }}
  backfill_concurrency: {{ .Values.miroir.resharding.backfill_concurrency | default 4 }}
  backfill_batch_size: {{ .Values.miroir.resharding.backfill_batch_size | default 1000 }}
  throttle_docs_per_sec: {{ .Values.miroir.resharding.throttle_docs_per_sec | default 0 }}
  verify_before_swap: {{ .Values.miroir.resharding.verify_before_swap | default true }}
  retain_old_index_hours: {{ .Values.miroir.resharding.retain_old_index_hours | default 48 }}
  allowed_windows: {{ .Values.miroir.resharding.allowed_windows | default list | toJson }}
peer_discovery:
  service_name: {{ .Values.miroir.peer_discovery.service_name | default (printf "%s-headless" (include "miroir.fullname" .)) }}
  refresh_interval_s: {{ .Values.miroir.peer_discovery.refresh_interval_s | default 15 }}
leader_election:
  enabled: {{ .Values.miroir.leader_election.enabled | default true }}
  lease_ttl_s: {{ .Values.miroir.leader_election.lease_ttl_s | default 10 }}
  renew_interval_s: {{ .Values.miroir.leader_election.renew_interval_s | default 3 }}
hpa:
  enabled: {{ .Values.hpa.enabled | default false }}
tracing:
  enabled: {{ .Values.tracing.enabled | default false }}
  endpoint: {{ .Values.tracing.endpoint | default "http://tempo.monitoring.svc:4317" }}
  service_name: {{ .Values.tracing.serviceName | default "miroir" }}
  sample_rate: {{ .Values.tracing.sampleRate | default 0.1 }}
search_ui:
  enabled: {{ .Values.search_ui.enabled | default true }}
  scoped_key_max_age_days: {{ .Values.search_ui.scoped_key_max_age_days | default 60 }}
  scoped_key_rotate_before_expiry_days: {{ .Values.search_ui.scoped_key_rotate_before_expiry_days | default 30 }}
  scoped_key_rotation_drain_s: {{ .Values.search_ui.scoped_key_rotation_drain_s | default 120 }}
admin_ui:
  enabled: {{ .Values.admin_ui.enabled | default true }}
  path: {{ .Values.admin_ui.path | default "/_miroir/admin" }}
  auth: {{ .Values.admin_ui.auth | default "key" }}
  session_ttl_s: {{ .Values.admin_ui.session_ttl_s | default 3600 }}
  read_only_mode: {{ .Values.admin_ui.read_only_mode | default false }}
  allowed_origins: {{ .Values.admin_ui.allowed_origins | default list "same-origin" | toJson }}
  cors_allowed_origins: {{ .Values.admin_ui.cors_allowed_origins | default list | toJson }}
  rate_limit:
    per_ip: {{ .Values.admin_ui.rate_limit.per_ip | default "10/minute" }}
    backend: {{ .Values.admin_ui.rate_limit.backend | default "redis" }}
    redis_key_prefix: {{ .Values.admin_ui.rate_limit.redis_key_prefix | default "miroir:ratelimit:adminlogin:" }}
    redis_ttl_s: {{ .Values.admin_ui.rate_limit.redis_ttl_s | default 60 }}
    failed_attempt_threshold: {{ .Values.admin_ui.rate_limit.failed_attempt_threshold | default 5 }}
    backoff_start_minutes: {{ .Values.admin_ui.rate_limit.backoff_start_minutes | default 10 }}
    backoff_max_hours: {{ .Values.admin_ui.rate_limit.backoff_max_hours | default 24 }}
{{- if .Values.miroir.cdc }}
cdc:
  enabled: {{ .Values.miroir.cdc.enabled | default true }}
  emit_ttl_deletes: {{ .Values.miroir.cdc.emit_ttl_deletes | default false }}
  emit_internal_writes: {{ .Values.miroir.cdc.emit_internal_writes | default false }}
  buffer:
    primary: {{ .Values.miroir.cdc.buffer.primary | default "memory" }}
    memory_bytes: {{ .Values.miroir.cdc.buffer.memory_bytes | default 67108864 }}
    overflow: {{ .Values.miroir.cdc.buffer.overflow | default "drop" }}
    {{- if eq (include "miroir.redisEnabled" .) "true" }}
    redis_bytes: {{ .Values.miroir.cdc.buffer.redis_bytes | default 1073741824 }}
    {{- end }}
    {{- if eq (include "miroir.cdcPvcEnabled" .) "true" }}
    pvc_path: /data/cdc
    {{- end }}
{{- end }}
{{- end -}}

{{/*
Return "true" if Redis is enabled, "false" otherwise.
*/}}
{{- define "miroir.redisEnabled" -}}
{{- if .Values.redis.enabled -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/*
Redis auth secret name.
*/}}
{{- define "miroir.redisSecretName" -}}
{{- if .Values.redis.auth.existingSecret -}}
{{- .Values.redis.auth.existingSecret -}}
{{- else -}}
{{- include "miroir.fullname" . }}-redis-secret
{{- end -}}
{{- end -}}

{{/*
Return "true" if CDC PVC should be created, "false" otherwise.
PVC is rendered when cdc.buffer.primary=="pvc" or cdc.buffer.overflow=="pvc".
*/}}
{{- define "miroir.cdcPvcEnabled" -}}
{{- if and .Values.miroir.cdc (or (eq .Values.miroir.cdc.buffer.primary "pvc") (eq .Values.miroir.cdc.buffer.overflow "pvc")) -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/*
Cross-field validations that JSON Schema draft-7 cannot express.
Rendered as an empty ConfigMap; fails template rendering on invalid config.
*/}}
{{- define "miroir.validate.values" -}}
{{- if .Values.search_ui -}}
{{- if and (hasKey .Values.search_ui "scoped_key_rotate_before_expiry_days") (hasKey .Values.search_ui "scoped_key_max_age_days") -}}
{{- if ge (int .Values.search_ui.scoped_key_rotate_before_expiry_days) (int .Values.search_ui.scoped_key_max_age_days) -}}
{{- fail (printf "search_ui.scoped_key_rotate_before_expiry_days (%d) must be strictly less than scoped_key_max_age_days (%d); otherwise rotation fires before/at key issuance, producing a continuous rotation loop" (int .Values.search_ui.scoped_key_rotate_before_expiry_days) (int .Values.search_ui.scoped_key_max_age_days)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if .Values.miroir.leader_election -}}
{{- if and (hasKey .Values.miroir.leader_election "lease_ttl_s") (hasKey .Values.miroir.leader_election "renew_interval_s") -}}
{{- if le (int .Values.miroir.leader_election.lease_ttl_s) (int .Values.miroir.leader_election.renew_interval_s) -}}
{{- fail (printf "leader_election.lease_ttl_s (%d) must be greater than leader_election.renew_interval_s (%d); otherwise the lease expires before it can be renewed" (int .Values.miroir.leader_election.lease_ttl_s) (int .Values.miroir.leader_election.renew_interval_s)) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
