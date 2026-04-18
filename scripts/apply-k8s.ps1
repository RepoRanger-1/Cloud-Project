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

Write-Host "Applied. Wait for pods Ready, then port-forward Prometheus (see project instructions)."
