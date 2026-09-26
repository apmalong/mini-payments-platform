{{- define "fraud-scorer.labels" -}}
app.kubernetes.io/name: fraud-scorer
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
{{- define "fraud-scorer.selector" -}}
app.kubernetes.io/name: fraud-scorer
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
