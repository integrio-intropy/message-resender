{{/*
Expand the name of the chart.
*/}}
{{- define "message-resender.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars (DNS naming spec).
*/}}
{{- define "message-resender.fullname" -}}
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
{{- define "message-resender.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "message-resender.labels" -}}
helm.sh/chart: {{ include "message-resender.chart" . }}
{{ include "message-resender.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "message-resender.selectorLabels" -}}
app.kubernetes.io/name: {{ include "message-resender.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
control-plane: controller-manager
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "message-resender.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "message-resender.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Manager image reference.
*/}}
{{- define "message-resender.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Broker secret name.
Returns the name of the Secret to use for broker env vars,
or empty string when no secret is needed.
*/}}
{{- define "message-resender.brokerSecretName" -}}
{{- if eq .Values.broker.type "rabbitmq" }}
  {{- if .Values.broker.rabbitmq.existingSecret }}
    {{- .Values.broker.rabbitmq.existingSecret }}
  {{- else if .Values.broker.rabbitmq.dsn }}
    {{- printf "%s-broker" (include "message-resender.fullname" .) }}
  {{- end }}
{{- else if eq .Values.broker.type "azureservicebus" }}
  {{- if .Values.broker.azureServiceBus.existingSecret }}
    {{- .Values.broker.azureServiceBus.existingSecret }}
  {{- else if .Values.broker.azureServiceBus.connectionString }}
    {{- printf "%s-broker" (include "message-resender.fullname" .) }}
  {{- end }}
{{- end }}
{{- end }}
