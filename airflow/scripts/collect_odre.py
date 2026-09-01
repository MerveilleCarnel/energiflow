"""
Collecte de la consommation régionale (API ODRE - Opendatasoft Explore v2.1,
publique, sans authentification) et sauvegarde en base PostgreSQL.
"""
import os
import requests
import psycopg2
from datetime import datetime, timedelta

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

ODRE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/records"
PAGE_SIZE = 100


def fetch_odre_page(where_clause: str, limit: int, offset: int) -> dict:
    """Récupère une page de résultats (l'API plafonne à 100 lignes par appel)."""
    params = {
        "where": where_clause,
        "limit": limit,
        "offset": offset,
        "order_by": "date_heure",
    }
    response = requests.get(ODRE_URL, params=params)
    if not response.ok:
        print("Réponse brute d'ODRE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def fetch_odre_all(where_clause: str) -> list[dict]:
    """Boucle sur toutes les pages jusqu'à récupérer l'ensemble des lignes correspondantes."""
    all_records = []
    offset = 0
    while True:
        page = fetch_odre_page(where_clause, PAGE_SIZE, offset)
        records = page.get("results", [])
        if not records:
            break
        all_records.extend(records)
        total = page.get("total_count", len(all_records))
        print(f"  ... {len(all_records)} / {total} lignes récupérées")
        offset += PAGE_SIZE
        if offset >= total:
            break
    return all_records


def save_to_postgres(rows: list[dict]) -> None:
    """Crée la table si besoin et insère les lignes (upsert sur date_heure + region)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_consommation_regionale (
                    date_heure                     TIMESTAMPTZ NOT NULL,
                    code_insee_region               TEXT,
                    region                           TEXT NOT NULL,
                    consommation_brute_electricite_mw NUMERIC,
                    statut_rte                       TEXT,
                    PRIMARY KEY (date_heure, region)
                );
            """)
            for r in rows:
                cur.execute("""
                    INSERT INTO fact_consommation_regionale
                        (date_heure, code_insee_region, region, consommation_brute_electricite_mw, statut_rte)
                    VALUES (%(date_heure)s, %(code_insee_region)s, %(region)s, %(conso)s, %(statut_rte)s)
                    ON CONFLICT (date_heure, region) DO UPDATE
                    SET consommation_brute_electricite_mw = EXCLUDED.consommation_brute_electricite_mw,
                        statut_rte = EXCLUDED.statut_rte;
                """, {
                    "date_heure": r.get("date_heure"),
                    "code_insee_region": r.get("code_insee_region"),
                    "region": r.get("region"),
                    "conso": r.get("consommation_brute_electricite_rte"),
                    "statut_rte": r.get("statut_rte"),
                })
    finally:
        conn.close()


def fetch_latest_available_date() -> str:
    """Interroge la ligne la plus récente disponible pour caler la fenêtre de collecte dessus."""
    params = {"order_by": "date_heure desc", "limit": 1}
    response = requests.get(ODRE_URL, params=params)
    response.raise_for_status()
    results = response.json().get("results", [])
    if not results:
        raise RuntimeError("Impossible de déterminer la date la plus récente : aucune donnée retournée.")
    return results[0]["date"]  # format "YYYY-MM-DD"


if __name__ == "__main__":
    latest_str = fetch_latest_available_date()
    end_day = datetime.strptime(latest_str, "%Y-%m-%d")
    start_day = end_day - timedelta(days=6)

    start_str = start_day.strftime("%Y-%m-%d")
    end_str = end_day.strftime("%Y-%m-%d")
    where_clause = f"date >= date'{start_str}' and date <= date'{end_str}'"

    print(f"Donnée la plus récente disponible : {latest_str}")
    print(f"Collecte ODRE du {start_str} au {end_str}...")
    records = fetch_odre_all(where_clause)

    print(f"{len(records)} lignes extraites au total.")
    print("Exemple :", records[0] if records else "(aucune ligne)")

    save_to_postgres(records)
    print(f"{len(records)} lignes insérées/mises à jour dans PostgreSQL (table fact_consommation_regionale).")