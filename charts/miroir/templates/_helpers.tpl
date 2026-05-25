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
Create chart name and version as used by the chart label.
*/}}
{{- define "miroir.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "miroir.labels" -}}
helm.sh/chart: {{ include "miroir.chart" . }}
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
Redis enabled
*/}}
{{- define "miroir.redisEnabled" -}}
{{- eq .Values.miroir.taskStore.backend "redis" }}
{{- end }}

{{/*
Redis secret name
*/}}
{{- define "miroir.redisSecretName" -}}
{{- if .Values.redis.auth.existingSecret }}
{{- .Values.redis.auth.existingSecret }}
{{- else }}
{{- printf "%s-redis-secret" (include "miroir.fullname" .) }}
{{- end }}
{{- end }}

{{/*
CDC PVC enabled — only rendered when cdc.buffer.primary=="pvc" or cdc.buffer.overflow=="pvc" (plan §13.13)
*/}}
{{- define "miroir.cdcPvcEnabled" -}}
{{- or (eq .Values.miroir.cdc.buffer.primary "pvc") (eq .Values.miroir.cdc.buffer.overflow "pvc") }}
{{- end }}

{{/*
Service Account Name
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
{{- printf "%s-miroir-secret" (include "miroir.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Miroir config (miroir.yaml)
*/}}
{{- define "miroir.config" -}}
# Miroir configuration (plan §4)
shards: {{ .Values.miroir.shards }}
replication_factor: {{ .Values.miroir.replicationFactor }}
replica_groups: {{ .Values.miroir.replicaGroups }}

nodes: []
task_store:
  backend: {{ .Values.miroir.taskStore.backend | quote }}
  path: {{ .Values.miroir.taskStore.path | quote }}
  {{- if and (eq (include "miroir.redisEnabled" .) "true") .Values.redis.enabled }}
  url: {{ printf "redis://%s-redis:6379" (include "miroir.fullname" .) | quote }}
  {{- else if .Values.miroir.taskStore.url }}
  url: {{ .Values.miroir.taskStore.url | quote }}
  {{- end }}

admin:
  enabled: true

health:
  interval_ms: 5000
  timeout_ms: 2000
  unhealthy_threshold: 3
  recovery_threshold: 2

scatter:
  node_timeout_ms: 5000
  retry_on_timeout: true
  unavailable_shard_policy: {{ .Values.miroir.scatter.unavailableShardPolicy | quote }}

rebalancer:
  auto_rebalance_on_recovery: true
  max_concurrent_migrations: 4
  migration_timeout_s: 3600

server:
  port: 7700
  bind: "0.0.0.0"
  max_body_bytes: 104857600
  max_concurrent_requests: 500
  request_timeout_ms: 30000

connection_pool_per_node:
  max_idle: 32
  max_total: 128
  idle_timeout_s: 60

task_registry:
  cache_size: 10000
  redis_pool_max: 50
  ttl_seconds: 604800
  prune_interval_s: 300
  prune_batch_size: 10000

{{- if .Values.miroir.cdc.enabled }}
cdc:
  enabled: true
  emit_ttl_deletes: {{ .Values.miroir.cdc.emit_ttl_deletes }}
  emit_internal_writes: {{ .Values.miroir.cdc.emit_internal_writes }}
  sinks:
{{- if .Values.miroir.cdc.sinks }}
{{ toYaml .Values.miroir.cdc.sinks | indent 4 }}
{{- else }}
  []
{{- end }}
  buffer:
    primary: {{ .Values.miroir.cdc.buffer.primary | quote }}
    memory_bytes: {{ .Values.miroir.cdc.buffer.memory_bytes }}
    overflow: {{ .Values.miroir.cdc.buffer.overflow | quote }}
    {{- if eq .Values.miroir.cdc.buffer.primary "redis" }}
    redis_bytes: {{ .Values.miroir.cdc.buffer.redis_bytes }}
    {{- end }}
{{- end }}

peer_discovery:
  service_name: "miroir-headless"
  refresh_interval_s: 15

leader_election:
  enabled: true
  lease_ttl_s: 10
  renew_interval_s: 3
{{- end }}

{{/*
Validate values at render time (cross-field checks that JSON Schema cannot express).
*/}}
{{- define "miroir.validate.values" -}}
{{- if .Values.miroir.search_ui.scoped_key_rotate_before_expiry_days }}
{{- if ge (int .Values.miroir.search_ui.scoped_key_rotate_before_expiry_days) (int .Values.miroir.search_ui.scoped_key_max_age_days) }}
{{- fail "search_ui.scoped_key_rotate_before_expiry_days must be strictly less than search_ui.scoped_key_max_age_days (otherwise rotation would fire immediately or before key issuance, producing a continuous rotation loop)" }}
{{- end }}
{{- end }}
{{- end }}
