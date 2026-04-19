# Phase 7 Runbook: SLA-Aware Predictive Autoscaling

This runbook compares:

1. Baseline: CPU HPA (`k8s/phase7-baseline-hpa.yaml`)
2. Proposed: Risk-score autoscaler (`k8s/phase7-autoscaler.yaml`)

## 1) Build and deploy

```powershell
.\scripts\build-images.ps1
.\scripts\apply-k8s.ps1
kubectl rollout status deployment/producer
kubectl rollout status deployment/consumer
kubectl rollout status deployment/prometheus
kubectl rollout status deployment/kafka-exporter
```

## 2) Open Prometheus

```powershell
kubectl port-forward svc/prometheus 9090:9090
```

Use <http://localhost:9090>.

## 3) Experiment metrics (PromQL)

- Input rate:
  - `sum(rate(ecommerce_events_sent_total[1m]))`
- Processing throughput:
  - `sum(rate(ecommerce_events_processed_total[1m]))`
- P95 delay:
  - `histogram_quantile(0.95, sum(rate(ecommerce_processing_delay_ms_bucket[5m])) by (le))`
- Consumer group lag (danielqsj/kafka-exporter exposes `kafka_consumergroup_lag`):
  - `sum(kafka_consumergroup_lag{consumergroup="ecommerce-consumer-group",topic="ecommerce-events"})`
  - If empty, open **Status → Targets** and confirm `kafka-exporter` is UP; then run `kafka_consumergroup_lag` in Graph with no filters to see real label values.
- Cost proxy (replica-minutes):
  - Sample consumer replicas every 30s:
  - `kubectl get deploy consumer -w`
  - Compute `replica_minutes = (sum of sampled replicas * 30) / 60`
- Proposed scaler risk score (only after `phase7-autoscaler` is running **and** Prometheus scrapes it):
  - `phase7_risk_score`
  - Baseline HPA: apply `k8s/prometheus.yaml` (no autoscaler scrape). Proposed: `kubectl apply -f k8s/prometheus-with-phase7-autoscaler.yaml` then `kubectl rollout restart deployment/prometheus`.

## 4) Load scenarios

Change producer send interval:

```powershell
# steady load (~1 eps)
kubectl set env deployment/producer EVENT_INTERVAL_MS=1000

# burst (~5 eps)
kubectl set env deployment/producer EVENT_INTERVAL_MS=200

# burst (~10 eps)
kubectl set env deployment/producer EVENT_INTERVAL_MS=100

# back to normal
kubectl set env deployment/producer EVENT_INTERVAL_MS=1000
```

For a spiky pattern, alternate every 2 minutes between `100` and `1000`.

## 5) Baseline run (CPU HPA)

```powershell
kubectl delete -f k8s/phase7-autoscaler.yaml --ignore-not-found
kubectl apply -f k8s/phase7-baseline-hpa.yaml
kubectl get hpa -w
```

Record all metrics for:

- steady
- burst 5x
- burst 10x
- spiky alternating

Run each scenario 3 times.

## 6) Proposed run (SLA-aware autoscaler)

```powershell
kubectl delete -f k8s/phase7-baseline-hpa.yaml --ignore-not-found
kubectl scale deployment/consumer --replicas=1
kubectl apply -f k8s/phase7-autoscaler.yaml
kubectl apply -f k8s/prometheus-with-phase7-autoscaler.yaml
kubectl rollout restart deployment/prometheus
kubectl logs deployment/phase7-autoscaler -f
```

Repeat the same scenarios and 3 runs each.

## 7) Result table template

| Scenario | Method | Avg P95 delay (ms) | Avg lag | Avg throughput (eps) | Recovery time (s) | Replica-minutes | Notes |
|---|---|---:|---:|---:|---:|---:|---|
| steady | baseline |  |  |  |  |  |  |
| steady | proposed |  |  |  |  |  |  |
| burst 5x | baseline |  |  |  |  |  |  |
| burst 5x | proposed |  |  |  |  |  |  |
| burst 10x | baseline |  |  |  |  |  |  |
| burst 10x | proposed |  |  |  |  |  |  |
| spiky | baseline |  |  |  |  |  |  |
| spiky | proposed |  |  |  |  |  |  |

## 8) Hypothesis statement (for report)

Compared with CPU-based HPA, the SLA-aware predictive autoscaler reduces P95 processing delay under bursty workloads while maintaining comparable throughput at lower replica-minute cost, by combining lag, delay, trend, and cost signals with cooldown and hysteresis.
