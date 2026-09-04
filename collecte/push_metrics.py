"""
Pousse les métriques de fraîcheur par source vers Prometheus Pushgateway
(C4.3.1). Prometheus scrape le Pushgateway ; Grafana visualise et alerte
sur ces métriques.
"""
import os
from datetime import datetime, timezone
import psycopg2
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091")

SOURCES = [
    ("consommation_rte", "SELECT MAX(start_date) FROM public.fact_consommation"),
    ("meteo", "SELECT MAX(date_heure) FROM public.fact_meteoid"),
    ("prix_marche", "SELECT MAX(date_heure) FROM public.fact_prix_marche"),
    ("consommation_regionale_odre", "SELECT MAX(date_heure) FROM public.fact_consommation_regionale"),
    ("actualites", "SELECT MAX(date_publication) FROM public.fact_actualites"),
    ("eco2mix", "SELECT MAX(date_heure) FROM public.fact_eco2mix"),
]


"""
Pousse les métriques de fraîcheur par source vers Prometheus Pushgateway
(C4.3.1). Prometheus scrape le Pushgateway ; Grafana visualise et alerte
sur ces métriques.
"""
import os
from datetime import datetime, timezone
import psycopg2
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")
PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "pushgateway:9091")

SOURCES = [
    ("consommation_rte", "SELECT MAX(start_date) FROM public.fact_consommation"),
    ("meteo", "SELECT MAX(date_heure) FROM public.fact_meteoid"),
    ("prix_marche", "SELECT MAX(date_heure) FROM public.fact_prix_marche"),
    ("consommation_regionale_odre", "SELECT MAX(date_heure) FROM public.fact_consommation_regionale"),
    ("actualites", "SELECT MAX(date_publication) FROM public.fact_actualites"),
    ("eco2mix", "SELECT MAX(date_heure) FROM public.fact_eco2mix"),
]


def main():
    registry = CollectorRegistry()
    gauge_fraicheur = Gauge(
        "energiflow_source_retard_heures",
        "Retard en heures entre maintenant et la derniere donnee collectee, par source",
        ["source"],
        registry=registry,
    )

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    maintenant = datetime.now(timezone.utc)

    with conn, conn.cursor() as cur:
        for nom_source, requete in SOURCES:
            cur.execute(requete)
            derniere_donnee = cur.fetchone()[0]
            if derniere_donnee is None:
                retard_heures = -1  # convention : -1 = aucune donnee jamais recue
            else:
                # Certaines sources stockent une DATE simple (sans heure), ex :
                # fact_actualites.date_publication. On la convertit en datetime
                # (minuit UTC) pour rester comparable aux autres sources.
                if not isinstance(derniere_donnee, datetime):
                    derniere_donnee = datetime(
                        derniere_donnee.year, derniere_donnee.month, derniere_donnee.day,
                        tzinfo=timezone.utc,
                    )
                elif derniere_donnee.tzinfo is None:
                    derniere_donnee = derniere_donnee.replace(tzinfo=timezone.utc)
                retard_heures = round((maintenant - derniere_donnee).total_seconds() / 3600, 2)

            gauge_fraicheur.labels(source=nom_source).set(retard_heures)
            print(f"  {nom_source}: retard = {retard_heures}h")

    conn.close()

    push_to_gateway(PUSHGATEWAY_URL, job="energiflow_monitoring_fraicheur", registry=registry)
    print(f"Metriques poussees vers Pushgateway ({PUSHGATEWAY_URL}).")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()