"""
Backfill ponctuel (à exécuter une seule fois) : récupère la consommation
nationale depuis janvier 2025 jusqu'à hier, pour disposer d'un historique
complet (et donc d'un vrai test hiver/été) tout en se raccordant exactement
là où la collecte quotidienne (Airflow) prend le relais chaque jour.
Réutilise exactement la même logique que collect_rte.py (mêmes fonctions,
même table, même upsert) — seule la période demandée change.
"""
import os
import time
import base64
import requests
import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

RTE_CLIENT_ID = os.environ.get("RTE_CLIENT_ID", "")
RTE_CLIENT_SECRET = os.environ.get("RTE_CLIENT_SECRET", "")
RTE_BASIC_AUTH = os.environ.get("RTE_BASIC_AUTH", "")

TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
BASE_URL = "https://digital.iservices.rte-france.com/open_api/consumption/v1"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

# Période à récupérer : depuis janvier 2025 jusqu'à hier (raccord avec le
# collecteur quotidien, qui prend le relais à partir d'aujourd'hui).
BACKFILL_START = datetime(2025, 1, 1)
BACKFILL_END = datetime.now(ZoneInfo("Europe/Paris")).replace(tzinfo=None) - timedelta(days=1)
CHUNK_DAYS = 7  # l'API exige un minimum de 7 jours par requête (déjà vérifié)
PAUSE_ENTRE_APPELS = 0.5  # secondes ; ~85 tranches attendues sur cette période


def get_access_token() -> str:
    if RTE_BASIC_AUTH:
        encoded = RTE_BASIC_AUTH.strip()
    else:
        credentials = f"{RTE_CLIENT_ID.strip()}:{RTE_CLIENT_SECRET.strip()}"
        encoded = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    response = requests.post(TOKEN_URL, headers=headers, data={"grant_type": "client_credentials"})
    response.raise_for_status()
    return response.json()["access_token"]


def fetch_consommation_short_term(token: str, start_date: str, end_date: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start_date, "end_date": end_date}
    response = requests.get(f"{BASE_URL}/short_term", headers=headers, params=params)
    if not response.ok:
        print("  Réponse brute de RTE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def flatten_points(data: dict) -> list[dict]:
    points = []
    for block in data["short_term"]:
        block_type = block["type"]
        for v in block["values"]:
            points.append({
                "start_date": v["start_date"],
                "end_date": v["end_date"],
                "updated_date": v["updated_date"],
                "value_mw": v["value"],
                "type": block_type,
            })
    return points


def save_to_postgres(points: list[dict]) -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            for p in points:
                cur.execute("""
                    INSERT INTO fact_consommation (start_date, end_date, updated_date, value_mw, type)
                    VALUES (%(start_date)s, %(end_date)s, %(updated_date)s, %(value_mw)s, %(type)s)
                    ON CONFLICT (start_date, type) DO UPDATE
                    SET end_date = EXCLUDED.end_date,
                        updated_date = EXCLUDED.updated_date,
                        value_mw = EXCLUDED.value_mw;
                """, p)
    finally:
        conn.close()


if __name__ == "__main__":
    token = get_access_token()
    total_points = 0
    current = BACKFILL_START

    print(f"Backfill de {BACKFILL_START.date()} à {BACKFILL_END.date()}, par tranches de {CHUNK_DAYS} jours...")
    paris_tz = ZoneInfo("Europe/Paris")
    while current < BACKFILL_END:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), BACKFILL_END)
        start_str = current.replace(tzinfo=paris_tz, hour=0, minute=0, second=0).isoformat()
        end_str = chunk_end.replace(tzinfo=paris_tz, hour=23, minute=59, second=59).isoformat()

        print(f"  {current.date()} -> {chunk_end.date()} ...", end=" ")
        data = fetch_consommation_short_term(token, start_str, end_str)
        points = flatten_points(data)
        save_to_postgres(points)
        total_points += len(points)
        print(f"{len(points)} points insérés.")

        current = chunk_end + timedelta(days=1)
        time.sleep(PAUSE_ENTRE_APPELS)

    print(f"\nBackfill terminé : {total_points} points insérés au total ({BACKFILL_START.date()} -> {BACKFILL_END.date()}).")
    print("La collecte quotidienne (Airflow, 6h du matin) prend le relais à partir d'aujourd'hui : plus aucun trou.")
    print("Pense à relancer build_silver.py puis build_gold.py pour que les vues intègrent ces nouvelles données.")