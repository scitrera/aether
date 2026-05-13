{{/*
Expand the name of the chart.
*/}}
{{- define "aether-gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated to 63 characters to comply with DNS naming specs.
*/}}
{{- define "aether-gateway.fullname" -}}
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
Create chart label value (name-version).
*/}}
{{- define "aether-gateway.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "aether-gateway.labels" -}}
helm.sh/chart: {{ include "aether-gateway.chart" . }}
{{ include "aether-gateway.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels used by Deployment/Service selectors.
*/}}
{{- define "aether-gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "aether-gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "aether-gateway.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "aether-gateway.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Name of the Secret holding credentials.
*/}}
{{- define "aether-gateway.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- include "aether-gateway.fullname" . }}-credentials
{{- end }}
{{- end }}

{{/*
Name of the ConfigMap holding the gateway config file.
*/}}
{{- define "aether-gateway.configMapName" -}}
{{- include "aether-gateway.fullname" . }}-config
{{- end }}

{{/*
Validate that secrets.postgresPassword is set to a secure value when PostgreSQL is configured.
*/}}
{{- define "aether-gateway.validateSecrets" -}}
{{- if .Values.config.postgres.host }}
{{- if or (eq .Values.secrets.postgresPassword "changeme") (eq .Values.secrets.postgresPassword "") }}
{{- fail "SECURITY: You must set secrets.postgresPassword to a secure value when PostgreSQL is enabled" }}
{{- end }}
{{- end }}
{{- end }}
