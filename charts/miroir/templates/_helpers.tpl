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
peer_discovery:
  service_name: {{ .Release.Name }}-headless
  refresh_interval_s: 15
leader_election:
  enabled: true
  lease_ttl_s: 10
  renew_interval_s: 3
hpa:
  enabled: {{ .Values.hpa.enabled | default false }}
tracing:
  enabled: {{ .Values.tracing.enabled | default false }}
  endpoint: {{ .Values.tracing.endpoint | default "http://tempo.monitoring.svc:4317" }}
  service_name: {{ .Values.tracing.serviceName | default "miroir" }}
  sample_rate: {{ .Values.tracing.sampleRate | default 0.1 }}
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
{{- end -}}
