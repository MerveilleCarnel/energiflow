"""
Collecte du mix énergétique et de l'intensité carbone 

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

ECO2MIX_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-tr/records"
PAGE_SIZE = 100


def fetch_eco2mix_page(where_clause: str, limit: int, offset: int) -> dict:
    """Récupère une page de résultats (l'API plafonne généralement à 100 lignes par appel)."""
    params = {
        "where": where_clause,
        "limit": limit,
        "offset": offset,
        "order_by": "date_heure",
    }
    response = requests.get(ECO2MIX_URL, params=params)
    if not response.ok:
        print("Réponse brute d'ODRE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def fetch_eco2mix_all(where_clause: str) -> list[dict]:
    """Boucle sur toutes les pages jusqu'à récupérer l'ensemble des lignes correspondantes."""
    all_records = []
    offset = 0
    while True:
        page = fetch_eco2mix_page(where_clause, PAGE_SIZE, offset)
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
    """Crée la table si besoin et insère les lignes (upsert sur date_heure)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_eco2mix (
                    date_heure          TIMESTAMPTZ NOT NULL PRIMARY KEY,
                    consommation        NUMERIC,
                    nucleaire           NUMERIC,
                    eolien              NUMERIC,
                    solaire             NUMERIC,
                    hydraulique         NUMERIC,
                    gaz                 NUMERIC,
                    charbon             NUMERIC,
                    fioul               NUMERIC,
                    bioenergies         NUMERIC,
                    pompage             NUMERIC,
                    ech_physiques       NUMERIC,
                    taux_co2            NUMERIC
                );
            """)
            for r in rows:
                cur.execute("""
                    INSERT INTO fact_eco2mix
                        (date_heure, consommation, nucleaire, eolien, solaire,
                         hydraulique, gaz, charbon, fioul, bioenergies, pompage,
                         ech_physiques, taux_co2)
                    VALUES (%(date_heure)s, %(consommation)s, %(nucleaire)s, %(eolien)s,
                            %(solaire)s, %(hydraulique)s, %(gaz)s, %(charbon)s, %(fioul)s,
                            %(bioenergies)s, %(pompage)s, %(ech_physiques)s, %(taux_co2)s)
                    ON CONFLICT (date_heure) DO UPDATE
                    SET consommation = EXCLUDED.consommation,
                        nucleaire = EXCLUDED.nucleaire,
                        eolien = EXCLUDED.eolien,
                        solaire = EXCLUDED.solaire,
                        hydraulique = EXCLUDED.hydraulique,
                        gaz = EXCLUDED.gaz,
                        charbon = EXCLUDED.charbon,
                        fioul = EXCLUDED.fioul,
                        bioenergies = EXCLUDED.bioenergies,
                        pompage = EXCLUDED.pompage,
                        ech_physiques = EXCLUDED.ech_physiques,
                        taux_co2 = EXCLUDED.taux_co2;
                """, {
                    "date_heure": r.get("date_heure"),
                    "consommation": r.get("consommation"),
                    "nucleaire": r.get("nucleaire"),
                    "eolien": r.get("eolien"),
                    "solaire": r.get("solaire"),
                    "hydraulique": r.get("hydraulique"),
                    "gaz": r.get("gaz"),
                    "charbon": r.get("charbon"),
                    "fioul": r.get("fioul"),
                    "bioenergies": r.get("bioenergies"),
                    "pompage": r.get("pompage"),
                    "ech_physiques": r.get("ech_physiques"),
                    "taux_co2": r.get("taux_co2"),
                })
    finally:
        conn.close()


if __name__ == "__main__":
    end_day = datetime.utcnow()
    start_day = end_day - timedelta(days=2)  # fenêtre glissante, dataset "temps réel"

    start_str = start_day.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_day.strftime("%Y-%m-%dT%H:%M:%S")
    where_clause = f"date_heure >= date'{start_str}' and date_heure <= date'{end_str}'"

    print(f"Collecte eCO2mix du {start_str} au {end_str}...")
    records = fetch_eco2mix_all(where_clause)

    print(f"{len(records)} lignes extraites au total.")
    print("Exemple (à vérifier - noms de champs) :", records[0] if records else "(aucune ligne)")

    save_to_postgres(records)
    print(f"{len(records)} lignes insérées/mises à jour dans PostgreSQL (table fact_eco2mix).")