"""
Backfill ponctuel (à exécuter une seule fois) : récupère la consommation
régionale (ODRE) depuis janvier 2025 jusqu'à la donnée la plus récente
disponible, pour aligner cette source sur la même période que fact_consommation.
Réutilise exactement la même logique que collect_odre.py.
"""
import os
import requests
import psycopg2

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

ODRE_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/consommation-quotidienne-brute-regionale/records"
PAGE_SIZE = 100

BACKFILL_START = "2025-01-01"


def fetch_odre_page(where_clause: str, limit: int, offset: int) -> dict:
    params = {"where": where_clause, "limit": limit, "offset": offset, "order_by": "date_heure"}
    response = requests.get(ODRE_URL, params=params, timeout=30)
    if not response.ok:
        print("Réponse brute d'ODRE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def fetch_odre_all(where_clause: str) -> list[dict]:
    all_records = []
    offset = 0
    while True:
        page = fetch_odre_page(where_clause, PAGE_SIZE, offset)
        records = page.get("results", [])
        if not records:
            break
        all_records.extend(records)
        total = page.get("total_count", len(all_records))
        if len(all_records) % 2000 < PAGE_SIZE:
            print(f"  ... {len(all_records)} / {total} lignes récupérées")
        offset += PAGE_SIZE
        if offset >= total:
            break
    return all_records


def fetch_latest_available_date() -> str:
    params = {"order_by": "date_heure desc", "limit": 1}
    response = requests.get(ODRE_URL, params=params, timeout=30)
    response.raise_for_status()
    results = response.json().get("results", [])
    if not results:
        raise RuntimeError("Impossible de déterminer la date la plus récente.")
    return results[0]["date"]


def save_to_postgres(rows: list[dict]) -> None:
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
            for i, r in enumerate(rows, start=1):
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
                if i % 5000 == 0:
                    print(f"  ... {i} / {len(rows)} lignes insérées en base")
    finally:
        conn.close()


if __name__ == "__main__":
    from datetime import datetime, timedelta

    latest_str = fetch_latest_available_date()
    latest_date = datetime.strptime(latest_str, "%Y-%m-%d")
    start_date = datetime.strptime(BACKFILL_START, "%Y-%m-%d")

    CHUNK_DAYS = 15  # 12 régions x 48 relevés/jour x 15 j ≈ 8 640 lignes, sous la limite de 10 000 de l'API
    total_inserted = 0
    current = start_date

    print(f"Backfill ODRE du {BACKFILL_START} au {latest_str}, par tranches de {CHUNK_DAYS} jours...")
    print("Ceci peut prendre 10 à 20 minutes au total.")

    while current <= latest_date:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), latest_date)
        where_clause = (
            f"date >= date'{current.strftime('%Y-%m-%d')}' "
            f"and date <= date'{chunk_end.strftime('%Y-%m-%d')}'"
        )
        print(f"\n  Tranche {current.date()} -> {chunk_end.date()} :")
        records = fetch_odre_all(where_clause)
        save_to_postgres(records)
        total_inserted += len(records)
        print(f"  {len(records)} lignes insérées pour cette tranche (total cumulé : {total_inserted}).")

        current = chunk_end + timedelta(days=1)

    print(f"\nBackfill ODRE terminé : {total_inserted} lignes insérées/mises à jour au total.")