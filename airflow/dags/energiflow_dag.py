"""
DAG EnergiFlow — orchestre les 6 collectes (RTE, Open-Meteo, RTE Wholesale Market, ODRE, Web Scraping, éCO2mix)
une fois par jour, construit les couches Silver et Gold, puis pousse les indicateurs de fraîcheur vers Prometheus.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from alerting import alerte_echec_tache

default_args = {
    "owner": "energiflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alerte_echec_tache,
}

with DAG(
    dag_id="energiflow_collecte_quotidienne",
    description="Collecte quotidienne des 6 sources EnergiFlow + Silver + Gold + supervision",
    default_args=default_args,
    schedule="0 6 * * *",  # tous les jours à 6h du matin
    start_date=datetime(2026, 7, 20),
    catchup=False,
    tags=["energiflow", "bloc1", "bloc4"],
) as dag:

    collecte_rte = BashOperator(
        task_id="collecte_consommation_rte",
        bash_command="python /opt/airflow/scripts/collect_rte.py",
    )

    collecte_meteo = BashOperator(
        task_id="collecte_meteo",
        bash_command="python /opt/airflow/scripts/collect_meteo.py",
    )

    collecte_prix = BashOperator(
        task_id="collecte_prix_marche",
        bash_command="python /opt/airflow/scripts/collect_prix.py",
    )

    collecte_odre = BashOperator(
        task_id="collecte_consommation_regionale_odre",
        bash_command="python /opt/airflow/scripts/collect_odre.py",
    )

    collecte_scraping = BashOperator(
        task_id="collecte_actualites_scraping",
        bash_command="python /opt/airflow/scripts/collect_scraping.py",
    )

    collecte_eco2mix = BashOperator(
        task_id="collecte_mix_carbone_eco2mix",
        bash_command="python /opt/airflow/scripts/collect_eco2mix.py",
    )

    construction_silver = BashOperator(
        task_id="construction_couche_silver",
        bash_command="python /opt/airflow/scripts/build_silver.py",
    )

    construction_gold = BashOperator(
        task_id="construction_couche_gold",
        bash_command="python /opt/airflow/scripts/build_gold.py",
    )

    push_metriques = BashOperator(
        task_id="push_metriques_prometheus",
        bash_command="python /opt/airflow/scripts/push_metrics.py",
        env={"PUSHGATEWAY_URL": "pushgateway:9091"},
    )

    # Les 6 collectes tournent en parallèle, puis Silver se construit une fois
    # qu'elles sont TOUTES terminées, puis Gold à partir de Silver, puis les
    # métriques de fraîcheur sont poussées vers Prometheus.
    [collecte_rte, collecte_meteo, collecte_prix, collecte_odre, collecte_scraping, collecte_eco2mix] >> construction_silver >> construction_gold >> push_metriques