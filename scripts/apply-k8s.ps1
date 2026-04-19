# Apply manifests in dependency order (default namespace).
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$k8s = Join-Path $root "k8s"
Set-Location $root

kubectl apply -f (Join-Path $k8s "zookeeper.yaml")
kubectl apply -f (Join-Path $k8s "kafka.yaml")
kubectl apply -f (Join-Path $k8s "mongo.yaml")
kubectl apply -f (Join-Path $k8s "00-pipeline-storage.yaml")
kubectl apply -f (Join-Path $k8s "producer.yaml")
kubectl apply -f (Join-Path $k8s "consumer.yaml")
kubectl apply -f (Join-Path $k8s "api-producer.yaml")
kubectl apply -f (Join-Path $k8s "spark.yaml")
kubectl apply -f (Join-Path $k8s "monitoring-config.yaml")
kubectl apply -f (Join-Path $k8s "prometheus.yaml")
kubectl apply -f (Join-Path $k8s "kafka-exporter.yaml")

Write-Host "Applied base stack. For Phase 7 run either:"
Write-Host "  kubectl apply -f k8s/phase7-baseline-hpa.yaml   (baseline)"
Write-Host "  kubectl apply -f k8s/phase7-autoscaler.yaml     (proposed)"
