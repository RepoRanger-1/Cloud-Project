# Build images into the active Docker daemon.
# If using minikube: minikube docker-env | Invoke-Expression  (PowerShell)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

docker build -f Dockerfile.spark -t spark:latest .
docker build -f Dockerfile.producer -t producer:latest .
docker build -f Dockerfile.consumer -t consumer:latest .
docker build -f Dockerfile.api-producer -t api-producer:latest .


Write-Host "Done. Images: producer, consumer, api-producer, spark (all :latest)"
