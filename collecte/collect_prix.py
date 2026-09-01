"""
Collecte des prix de marché de gros de l'électricité (API RTE Wholesale Market)
et sauvegarde en base PostgreSQL.
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
BASE_URL = "https://digital.iservices.rte-france.com/open_api/wholesale_market/v3"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")


def get_access_token() -> str:
    """Récupère un token OAuth2 (même logique que pour Consumption)."""
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
        print("Réponse brute de RTE (token) :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()["access_token"]

def fetch_prix_marche(token: str, start_date: str, end_date: str) -> dict:
    """Récupère les prix spot du marché de gros sur la période donnée."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start_date, "end_date": end_date}
    response = requests.get(f"{BASE_URL}/france_power_exchanges", headers=headers, params=params)
    if not response.ok:
        print("Réponse brute de RTE (prix) :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()

def flatten_prix(data: dict) -> list[dict]:
    """
    Aplatit la réponse en une liste de points (date_heure, prix_eur_mwh).
    Gère deux formes possibles : une liste de blocs avec "values" imbriqué
    (comme pour la consommation), ou une liste de points déjà à plat.
    """
    root_key = next(iter(data))  # premier (et souvent unique) champ de la réponse
    blocks = data[root_key]
    rows = []
    for block in blocks:
        if isinstance(block, dict) and "values" in block:
            for v in block["values"]:
                rows.append({
                    "date_heure": v.get("start_date"),
                    "prix_eur_mwh": v.get("price", v.get("value")),
                })
        else:
            rows.append({
                "date_heure": block.get("start_date"),
                "prix_eur_mwh": block.get("price", block.get("value")),
            })
    return rows

def save_to_postgres(rows: list[dict]) -> None:
    """Crée la table si besoin et insère les points (upsert sur date_heure)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_prix_marche (
                    date_heure     TIMESTAMPTZ NOT NULL,
                    prix_eur_mwh   NUMERIC,
                    PRIMARY KEY (date_heure)
                );
            """)
            for r in rows:
                if r["date_heure"] is None:
                    continue
                cur.execute("""
                    INSERT INTO fact_prix_marche (date_heure, prix_eur_mwh)
                    VALUES (%(date_heure)s, %(prix_eur_mwh)s)
                    ON CONFLICT (date_heure) DO UPDATE
                    SET prix_eur_mwh = EXCLUDED.prix_eur_mwh;
                """, r)
    finally:
        conn.close()


if __name__ == "__main__":
    from zoneinfo import ZoneInfo

    paris_tz = ZoneInfo("Europe/Paris")
    end_day = datetime.now(paris_tz) - timedelta(days=1)
    start_day = end_day - timedelta(days=6)

    start = start_day.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
    end = end_day.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()

    token = get_access_token()
    data = fetch_prix_marche(token, start, end)

    print("Structure brute reçue (clés de premier niveau) :", list(data.keys()))

    rows = flatten_prix(data)
    print(f"{len(rows)} points de prix extraits.")
    print("Exemple :", rows[0] if rows else "(aucun point)")

    save_to_postgres(rows)
    print(f"{len(rows)} points insérés/mis à jour dans PostgreSQL (table fact_prix_marche).")