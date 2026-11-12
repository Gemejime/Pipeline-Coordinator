{{/*
Expand the name of the chart.
*/}}
{{- define "rag-ingestion-coordinator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "rag-ingestion-coordinator.fullname" -}}
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
{{- define "rag-ingestion-coordinator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels for all chart resources.
*/}}
{{- define "rag-ingestion-coordinator.labels" -}}
helm.sh/chart: {{ include "rag-ingestion-coordinator.chart" . }}
{{ include "rag-ingestion-coordinator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for coordinator control-plane workload pods (excludes migration Jobs).
*/}}
{{- define "rag-ingestion-coordinator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-ingestion-coordinator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: coordinator
{{- end }}

{{/*
Selector labels for the isolated PreSync DB migration Job pod so live Services never route to it.
*/}}
{{- define "rag-ingestion-coordinator.migrationSelectorLabels" -}}
app.kubernetes.io/name: {{ include "rag-ingestion-coordinator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: migration
{{- end }}

{{/*
Create the name of the service account to use.
*/}}
{{- define "rag-ingestion-coordinator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rag-ingestion-coordinator.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Shared hardened PodTemplateSpec used by both Deployment and Argo Rollout.
*/}}
{{- define "rag-ingestion-coordinator.podTemplate" -}}
metadata:
  annotations:
    checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
    {{- with .Values.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  labels:
    {{- include "rag-ingestion-coordinator.selectorLabels" . | nindent 4 }}
    {{- with .Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  serviceAccountName: {{ include "rag-ingestion-coordinator.serviceAccountName" . }}
  terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
  securityContext:
    {{- toYaml .Values.podSecurityContext | nindent 4 }}
  containers:
    - name: coordinator
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      securityContext:
        {{- toYaml .Values.securityContext | nindent 8 }}
      ports:
        - name: http
          containerPort: 8080
          protocol: TCP
        - name: grpc
          containerPort: 50051
          protocol: TCP
      envFrom:
        - configMapRef:
            name: {{ include "rag-ingestion-coordinator.fullname" . }}-config
      env:
        {{- if .Values.secrets.keys.postgresDSN }}
        - name: POSTGRES_DSN
          valueFrom:
            secretKeyRef:
              name: {{ .Values.secrets.existingSecret }}
              key: {{ .Values.secrets.keys.postgresDSN }}
        {{- end }}
        {{- if .Values.secrets.keys.redisPassword }}
        - name: REDIS_PASSWORD
          valueFrom:
            secretKeyRef:
              name: {{ .Values.secrets.existingSecret }}
              key: {{ .Values.secrets.keys.redisPassword }}
              optional: true
        {{- end }}
        {{- if .Values.secrets.keys.authHMACSecret }}
        - name: AUTH_HMAC_SECRET
          valueFrom:
            secretKeyRef:
              name: {{ .Values.secrets.existingSecret }}
              key: {{ .Values.secrets.keys.authHMACSecret }}
        {{- end }}
        {{- if .Values.secrets.keys.authStaticTokensJSON }}
        - name: AUTH_STATIC_TOKENS_JSON
          valueFrom:
            secretKeyRef:
              name: {{ .Values.secrets.existingSecret }}
              key: {{ .Values.secrets.keys.authStaticTokensJSON }}
        {{- end }}
        {{- if .Values.secrets.keys.awsAccessKeyID }}
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: {{ .Values.secrets.existingSecret }}
              key: {{ .Values.secrets.keys.awsAccessKeyID }}
        {{- end }}
        {{- if .Values.secrets.keys.awsSecretAccessKey }}
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: {{ .Values.secrets.existingSecret }}
              key: {{ .Values.secrets.keys.awsSecretAccessKey }}
        {{- end }}
      startupProbe:
        httpGet:
          path: {{ .Values.probes.startup.path }}
          port: http
        initialDelaySeconds: {{ .Values.probes.startup.initialDelaySeconds }}
        periodSeconds: {{ .Values.probes.startup.periodSeconds }}
        timeoutSeconds: {{ .Values.probes.startup.timeoutSeconds }}
        failureThreshold: {{ .Values.probes.startup.failureThreshold }}
      livenessProbe:
        httpGet:
          path: {{ .Values.probes.liveness.path }}
          port: http
        periodSeconds: {{ .Values.probes.liveness.periodSeconds }}
        timeoutSeconds: {{ .Values.probes.liveness.timeoutSeconds }}
        failureThreshold: {{ .Values.probes.liveness.failureThreshold }}
      readinessProbe:
        httpGet:
          path: {{ .Values.probes.readiness.path }}
          port: http
        periodSeconds: {{ .Values.probes.readiness.periodSeconds }}
        timeoutSeconds: {{ .Values.probes.readiness.timeoutSeconds }}
        failureThreshold: {{ .Values.probes.readiness.failureThreshold }}
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
      volumeMounts:
        - name: tmp
          mountPath: /tmp
  volumes:
    # Ephemeral /tmp only; strictly zero /dev/shm mounts per control-plane architecture.
    - name: tmp
      emptyDir:
        sizeLimit: {{ .Values.tmpVolumeSizeLimit }}
  {{- with .Values.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- if .Values.topologySpreadConstraints }}
  topologySpreadConstraints:
    {{- range .Values.topologySpreadConstraints }}
    - maxSkew: {{ .maxSkew }}
      topologyKey: {{ .topologyKey }}
      whenUnsatisfiable: {{ .whenUnsatisfiable }}
      labelSelector:
        matchLabels:
          {{- if and .labelSelector .labelSelector.matchLabels }}
          {{- toYaml .labelSelector.matchLabels | nindent 10 }}
          {{- else }}
          {{- include "rag-ingestion-coordinator.selectorLabels" $ | nindent 10 }}
          {{- end }}
    {{- end }}
  {{- end }}
  {{- if .Values.affinity }}
  affinity:
    {{- if .Values.affinity.podAntiAffinity }}
    podAntiAffinity:
      {{- if .Values.affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution }}
      preferredDuringSchedulingIgnoredDuringExecution:
        {{- range .Values.affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution }}
        - weight: {{ .weight }}
          podAffinityTerm:
            topologyKey: {{ .podAffinityTerm.topologyKey }}
            labelSelector:
              matchLabels:
                {{- if and .podAffinityTerm.labelSelector .podAffinityTerm.labelSelector.matchLabels }}
                {{- toYaml .podAffinityTerm.labelSelector.matchLabels | nindent 16 }}
                {{- else }}
                {{- include "rag-ingestion-coordinator.selectorLabels" $ | nindent 16 }}
                {{- end }}
        {{- end }}
      {{- end }}
      {{- if .Values.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution }}
      requiredDuringSchedulingIgnoredDuringExecution:
        {{- range .Values.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution }}
        - topologyKey: {{ .topologyKey }}
          labelSelector:
            matchLabels:
              {{- if and .labelSelector .labelSelector.matchLabels }}
              {{- toYaml .labelSelector.matchLabels | nindent 14 }}
              {{- else }}
              {{- include "rag-ingestion-coordinator.selectorLabels" $ | nindent 14 }}
              {{- end }}
        {{- end }}
      {{- end }}
    {{- end }}
    {{- with .Values.affinity.nodeAffinity }}
    nodeAffinity:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- with .Values.affinity.podAffinity }}
    podAffinity:
      {{- toYaml . | nindent 6 }}
    {{- end }}
  {{- end }}
{{- end }}
