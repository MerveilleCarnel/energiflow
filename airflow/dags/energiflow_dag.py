"""
DAG EnergiFlow — orchestre les 5 collectes (RTE, Open-Meteo, RTE Wholesale Market, ODRE, Web Scraping)
une fois par jour, puis construit les couches Silver et Gold à partir des données collectées.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "energiflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="energiflow_collecte_quotidienne",
    description="Collecte quotidienne des 5 sources EnergiFlow + Silver + Gold",
    default_args=default_args,
    schedule="0 6 * * *",  # tous les jours à 6h du matin
    start_date=datetime(2026, 7, 20),
    catchup=False,
    tags=["energiflow", "bloc1"],
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

    construction_silver = BashOperator(
        task_id="construction_couche_silver",
        bash_command="python /opt/airflow/scripts/build_silver.py",
    )

    construction_gold = BashOperator(
        task_id="construction_couche_gold",
        bash_command="python /opt/airflow/scripts/build_gold.py",
    )

    # Les 5 collectes tournent en parallèle, puis Silver se construit une fois
    # qu'elles sont TOUTES terminées, puis Gold se construit à partir de Silver.
    [collecte_rte, collecte_meteo, collecte_prix, collecte_odre, collecte_scraping] >> construction_silver >> construction_gold