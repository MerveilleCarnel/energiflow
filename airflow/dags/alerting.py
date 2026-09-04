"""
Callback d'alerte en cas d'echec d'une tache Airflow (C4.3.1). Pousse
l'horodatage du dernier echec, par tache, vers Prometheus Pushgateway.
Grafana peut alors declencher une alerte des que ce timestamp est recent.
"""
import os
import time
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091")


def alerte_echec_tache(context):
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id if context.get("dag") else "inconnu"
    task_id = task_instance.task_id if task_instance else "inconnu"

    try:
        registry = CollectorRegistry()
        gauge = Gauge(
            "energiflow_derniere_alerte_epoch",
            "Horodatage Unix du dernier echec pour cette tache",
            ["dag_id", "task_id"],
            registry=registry,
        )
        gauge.labels(dag_id=dag_id, task_id=task_id).set(time.time())
        push_to_gateway(PUSHGATEWAY_URL, job="energiflow_alertes",
                         grouping_key={"task_id": task_id}, registry=registry)
    except Exception as e:
        print(f"Echec de l'envoi de l'alerte vers Pushgateway : {e}")