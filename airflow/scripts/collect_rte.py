"""
Collecte de la consommation nationale (API RTE Consumption) et sauvegarde en base PostgreSQL.
Étape 1 du Bloc 1 : un seul script, pas encore d'orchestration Airflow.
"""
import os
import base64
import requests
import psycopg2
from datetime import datetime, timedelta

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


def get_access_token() -> str:
    """Récupère un token OAuth2 (valide 2h) via client_credentials."""
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
    if not response.ok:
        print("Réponse brute de RTE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()["access_token"]


def fetch_consommation_short_term(token: str, start_date: str, end_date: str) -> dict:
    """Récupère la consommation réalisée/prévue sur la période donnée."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start_date, "end_date": end_date}
    response = requests.get(f"{BASE_URL}/short_term", headers=headers, params=params)
    if not response.ok:
        print("Réponse brute de RTE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def flatten_points(data: dict) -> list[dict]:
    """
    L'API renvoie une liste de blocs (un par type : REALISED, ID_FORECAST...),
    chacun contenant sa propre liste "values" avec les relevés 15 minutes.
    On aplatit tout ça en une seule liste de points exploitables.
    """
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
    """Crée la table si besoin et insère les points (upsert sur start_date + type)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_consommation (
                    start_date   TIMESTAMPTZ NOT NULL,
                    end_date     TIMESTAMPTZ NOT NULL,
                    updated_date TIMESTAMPTZ NOT NULL,
                    value_mw     NUMERIC NOT NULL,
                    type         TEXT NOT NULL,
                    PRIMARY KEY (start_date, type)
                );
            """)
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
    from zoneinfo import ZoneInfo

    paris_tz = ZoneInfo("Europe/Paris")
    end_day = datetime.now(paris_tz) - timedelta(days=1)      # hier (dernier jour avec données définitives)
    start_day = end_day - timedelta(days=6)                    # 7 jours de plage au total

    start = start_day.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    end = end_day.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()

    token = get_access_token()
    data = fetch_consommation_short_term(token, start, end)
    points = flatten_points(data)

    print(f"{len(points)} points extraits du {start_day.strftime('%d/%m/%Y')} au {end_day.strftime('%d/%m/%Y')}")
    print("Exemple :", points[0] if points else "(aucun point)")

    save_to_postgres(points)
    print(f"{len(points)} points insérés/mis à jour dans PostgreSQL (table fact_consommation).")