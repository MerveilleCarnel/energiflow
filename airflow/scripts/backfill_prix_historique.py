"""
Backfill ponctuel (à exécuter une seule fois, rejouable sans risque grâce à
l'upsert) : récupère les prix de marché depuis janvier 2025 jusqu'à hier.

Version corrigée : chaque tranche est protégée par un retry automatique
(3 tentatives, pause croissante), pour ne pas laisser un aléa réseau/API
ponctuel (500, 403 transitoire) interrompre tout le backfill. Les tranches
qui échouent malgré les retries sont journalisées à la fin plutôt que de
faire planter le script.
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
BASE_URL = "https://digital.iservices.rte-france.com/open_api/wholesale_market/v3"

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

BACKFILL_START = datetime(2025, 1, 1)
BACKFILL_END = datetime.now(ZoneInfo("Europe/Paris")).replace(tzinfo=None) - timedelta(days=1)
CHUNK_DAYS = 7
PAUSE_ENTRE_APPELS = 1.5          # légèrement augmentée par précaution
MAX_TENTATIVES = 3
PAUSE_RETRY_BASE = 5              # secondes ; doublée à chaque nouvelle tentative


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


def fetch_prix_marche(token: str, start_date: str, end_date: str) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start_date, "end_date": end_date}
    response = requests.get(f"{BASE_URL}/france_power_exchanges", headers=headers, params=params)
    if not response.ok:
        print("    Réponse brute de RTE :", response.status_code, response.text[:200])
    response.raise_for_status()
    return response.json()


def fetch_prix_marche_avec_retry(token: str, start_date: str, end_date: str) -> dict | None:
    """Retente jusqu'à MAX_TENTATIVES fois avant d'abandonner cette tranche."""
    for tentative in range(1, MAX_TENTATIVES + 1):
        try:
            return fetch_prix_marche(token, start_date, end_date)
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            if tentative < MAX_TENTATIVES:
                pause = PAUSE_RETRY_BASE * tentative
                print(f"    Échec (HTTP {code}), tentative {tentative}/{MAX_TENTATIVES}, "
                      f"nouvelle tentative dans {pause}s...")
                time.sleep(pause)
            else:
                print(f"    Échec définitif après {MAX_TENTATIVES} tentatives (HTTP {code}).")
                return None
        except requests.exceptions.RequestException as e:
            if tentative < MAX_TENTATIVES:
                pause = PAUSE_RETRY_BASE * tentative
                print(f"    Erreur réseau ({e}), tentative {tentative}/{MAX_TENTATIVES}, "
                      f"nouvelle tentative dans {pause}s...")
                time.sleep(pause)
            else:
                print(f"    Échec définitif après {MAX_TENTATIVES} tentatives (erreur réseau : {e}).")
                return None
    return None


def flatten_prix(data: dict) -> list[dict]:
    root_key = next(iter(data))
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
    token = get_access_token()
    total_points = 0
    tranches_echouees = []
    current = BACKFILL_START
    paris_tz = ZoneInfo("Europe/Paris")

    print(f"Backfill prix de {BACKFILL_START.date()} à {BACKFILL_END.date()}, par tranches de {CHUNK_DAYS} jours...")
    while current < BACKFILL_END:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS - 1), BACKFILL_END)
        duree_jours = (chunk_end - current).days + 1
        if duree_jours < CHUNK_DAYS:
            print(f"  {current.date()} -> {chunk_end.date()} : tranche trop courte ({duree_jours} j), "
                  f"ignorée — sera couverte par la collecte quotidienne.")
            break

        start_str = current.replace(tzinfo=paris_tz, hour=0, minute=0, second=0, microsecond=0).isoformat()
        end_str = chunk_end.replace(tzinfo=paris_tz, hour=23, minute=59, second=59, microsecond=0).isoformat()

        print(f"  {current.date()} -> {chunk_end.date()} ...", end=" ")
        data = fetch_prix_marche_avec_retry(token, start_str, end_str)

        if data is None:
            tranches_echouees.append((current.date(), chunk_end.date()))
        else:
            points = flatten_prix(data)
            save_to_postgres(points)
            total_points += len(points)
            print(f"{len(points)} points insérés.")

        current = chunk_end + timedelta(days=1)
        time.sleep(PAUSE_ENTRE_APPELS)

    print(f"\nBackfill prix terminé : {total_points} points insérés au total.")
    if tranches_echouees:
        print(f"\n{len(tranches_echouees)} tranche(s) définitivement en échec (à retenter manuellement si besoin) :")
        for debut, fin in tranches_echouees:
            print(f"  - {debut} -> {fin}")
    else:
        print("Aucune tranche en échec.")
    print("\nPense à relancer build_silver.py puis build_gold.py.")