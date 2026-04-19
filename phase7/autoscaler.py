import os
import time
from dataclasses import dataclass

import requests
from kubernetes import client, config
from prometheus_client import Gauge, start_http_server


def env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass
class Settings:
    prometheus_url: str = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
    namespace: str = os.environ.get("TARGET_NAMESPACE", "default")
    deployment: str = os.environ.get("TARGET_DEPLOYMENT", "consumer")
    lag_query: str = os.environ.get(
        "LAG_QUERY",
        'sum(kafka_consumergroup_lag{consumergroup="ecommerce-consumer-group",topic="ecommerce-events"})',
    )
    delay_query: str = os.environ.get(
        "DELAY_P95_QUERY",
        "histogram_quantile(0.95, sum(rate(ecommerce_processing_delay_ms_bucket[5m])) by (le))",
    )
    input_rate_query: str = os.environ.get(
        "INPUT_RATE_QUERY",
        "sum(rate(ecommerce_events_sent_total[1m]))",
    )
    process_rate_query: str = os.environ.get(
        "PROCESS_RATE_QUERY",
        "sum(rate(ecommerce_events_processed_total[1m]))",
    )
    min_replicas: int = env_int("MIN_REPLICAS", 1)
    max_replicas: int = env_int("MAX_REPLICAS", 6)
    lag_target: float = env_float("LAG_TARGET", 250.0)
    delay_sla_ms: float = env_float("DELAY_SLA_MS", 1200.0)
    w1: float = env_float("W_LAG", 0.45)
    w2: float = env_float("W_DELAY", 0.35)
    w3: float = env_float("W_TREND", 0.25)
    w4: float = env_float("W_COST", 0.20)
    up_threshold: float = env_float("UP_THRESHOLD", 1.0)
    down_threshold: float = env_float("DOWN_THRESHOLD", 0.35)
    step_up: int = env_int("STEP_UP", 1)
    step_down: int = env_int("STEP_DOWN", 1)
    up_confirm: int = env_int("UP_CONFIRM", 2)
    down_confirm: int = env_int("DOWN_CONFIRM", 3)
    cooldown_seconds: int = env_int("COOLDOWN_SECONDS", 60)
    loop_seconds: int = env_int("LOOP_SECONDS", 30)
    metrics_port: int = env_int("METRICS_PORT", 9102)


class PromClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.timeout = 10

    def query_float(self, promql: str, fallback: float = 0.0) -> float:
        try:
            response = self.session.get(
                f"{self.base_url}/api/v1/query", params={"query": promql}, timeout=10
            )
            response.raise_for_status()
            payload = response.json()
            result = payload.get("data", {}).get("result", [])
            if not result:
                return fallback
            return float(result[0]["value"][1])
        except Exception:
            return fallback


class K8sScaler:
    def __init__(self, namespace: str, deployment: str) -> None:
        config.load_incluster_config()
        self.namespace = namespace
        self.deployment = deployment
        self.api = client.AppsV1Api()

    def get_replicas(self) -> int:
        dep = self.api.read_namespaced_deployment(self.deployment, self.namespace)
        return int(dep.spec.replicas or 1)

    def set_replicas(self, replicas: int) -> None:
        body = {"spec": {"replicas": replicas}}
        self.api.patch_namespaced_deployment_scale(
            name=self.deployment,
            namespace=self.namespace,
            body=body,
        )


def main() -> None:
    s = Settings()
    prom = PromClient(s.prometheus_url)
    scaler = K8sScaler(s.namespace, s.deployment)

    # Exporter metrics for debugging and report screenshots.
    risk_gauge = Gauge("phase7_risk_score", "Current SLA-aware scaling risk score")
    desired_gauge = Gauge("phase7_desired_replicas", "Desired replicas after policy")
    lag_gauge = Gauge("phase7_lag_value", "Kafka consumer group lag")
    p95_gauge = Gauge("phase7_p95_delay_ms", "P95 processing delay in ms")
    in_rate_gauge = Gauge("phase7_input_rate_eps", "Incoming event rate")
    out_rate_gauge = Gauge("phase7_processed_rate_eps", "Processed event rate")
    start_http_server(s.metrics_port)

    up_hits = 0
    down_hits = 0
    prev_rate = 0.0
    cooldown_until = 0.0

    while True:
        now = time.time()

        lag = prom.query_float(s.lag_query, fallback=0.0)
        p95_delay = prom.query_float(s.delay_query, fallback=0.0)
        in_rate = prom.query_float(s.input_rate_query, fallback=0.0)
        out_rate = prom.query_float(s.process_rate_query, fallback=0.0)
        replicas = scaler.get_replicas()

        lag_norm = lag / max(s.lag_target, 1.0)
        delay_norm = p95_delay / max(s.delay_sla_ms, 1.0)
        rate_trend = (in_rate - prev_rate) / max(prev_rate, 0.1)
        cost_norm = replicas / max(s.max_replicas, 1)
        risk_score = (
            s.w1 * lag_norm
            + s.w2 * delay_norm
            + s.w3 * max(rate_trend, 0.0)
            - s.w4 * cost_norm
        )

        desired = replicas
        if now >= cooldown_until:
            if risk_score > s.up_threshold:
                up_hits += 1
                down_hits = 0
                if up_hits >= s.up_confirm:
                    desired = min(s.max_replicas, replicas + s.step_up)
            elif risk_score < s.down_threshold:
                down_hits += 1
                up_hits = 0
                if down_hits >= s.down_confirm:
                    desired = max(s.min_replicas, replicas - s.step_down)
            else:
                up_hits = 0
                down_hits = 0

            if desired != replicas:
                scaler.set_replicas(desired)
                print(
                    f"Scaled {s.deployment}: {replicas} -> {desired} | "
                    f"risk={risk_score:.3f} lag={lag:.1f} p95={p95_delay:.1f} "
                    f"in_rate={in_rate:.2f} out_rate={out_rate:.2f}"
                )
                cooldown_until = now + s.cooldown_seconds
                up_hits = 0
                down_hits = 0
        else:
            print(
                f"Cooldown active; hold {replicas}. risk={risk_score:.3f} lag={lag:.1f} "
                f"p95={p95_delay:.1f} in_rate={in_rate:.2f} out_rate={out_rate:.2f}"
            )

        risk_gauge.set(risk_score)
        desired_gauge.set(desired)
        lag_gauge.set(lag)
        p95_gauge.set(p95_delay)
        in_rate_gauge.set(in_rate)
        out_rate_gauge.set(out_rate)

        prev_rate = in_rate
        time.sleep(s.loop_seconds)


if __name__ == "__main__":
    main()
