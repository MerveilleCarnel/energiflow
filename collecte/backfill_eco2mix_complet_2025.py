"""
Backfill ponctuel (à exécuter une seule fois) : complète l'historique
éCO2mix pour couvrir la même période que les 3 autres sources numériques
(consommation nationale, météo, consommation régionale) : janvier 2025 ->
aujourd'hui.

fact_eco2mix couvre déjà décembre 2025 -> aujourd'hui (backfill hiver +
collecte quotidienne). Ce script ne comble que le trou restant :
janvier 2025 -> 30 novembre 2025.

Utilise l'API ODRE (Opendatasoft), dataset eco2mix-national-cons-def
(données consolidées/définitives, historique depuis 2012, pas demi-heure).
Découpage mensuel pour rester sous la limite offset+limit de l'API
(la même contrainte que backfill_odre_historique.py).
Réutilise le même schéma de table (fact_eco2mix) que collect_eco2mix.py.
"""
import os
import time
import requests
import psycopg2
from datetime import date
from calendar import monthrange

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

ECO2MIX_CONS_DEF_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
PAGE_SIZE = 100

BACKFILL_START = date(2025, 1, 1)
BACKFILL_END = date(2025, 11, 30)  # décembre 2025 -> aujourd'hui déjà en base


def generer_tranches_mensuelles(debut: date, fin: date):
    """Découpe la période en tranches mois par mois (bornes incluses)."""
    tranches = []
    courant = debut
    while courant <= fin:
        dernier_jour_mois = monthrange(courant.year, courant.month)[1]
        fin_mois = date(courant.year, courant.month, dernier_jour_mois)
        tranches.append((courant, min(fin_mois, fin)))
        if courant.month == 12:
            courant = date(courant.year + 1, 1, 1)
        else:
            courant = date(courant.year, courant.month + 1, 1)
    return tranches


def fetch_eco2mix_page(where_clause: str, limit: int, offset: int) -> dict:
    params = {
        "where": where_clause,
        "limit": limit,
        "offset": offset,
        "order_by": "date_heure",
    }
    response = requests.get(ECO2MIX_CONS_DEF_URL, params=params, timeout=30)
    if not response.ok:
        print("Réponse brute d'ODRE :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def fetch_eco2mix_all(where_clause: str) -> list[dict]:
    all_records = []
    offset = 0
    while True:
        page = fetch_eco2mix_page(where_clause, PAGE_SIZE, offset)
        records = page.get("results", [])
        if not records:
            break
        all_records.extend(records)
        total = page.get("total_count", len(all_records))
        print(f"    ... {len(all_records)} / {total} lignes récupérées")
        offset += PAGE_SIZE
        if offset >= total:
            break
    return all_records


def save_to_postgres(rows: list[dict]) -> None:
    """Insère les lignes dans fact_eco2mix (upsert sur date_heure)."""
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
    tranches = generer_tranches_mensuelles(BACKFILL_START, BACKFILL_END)
    total_insere = 0

    print(f"Backfill éCO2mix du {BACKFILL_START} au {BACKFILL_END}, en {len(tranches)} tranches mensuelles...")

    for i, (debut_tranche, fin_tranche) in enumerate(tranches, start=1):
        print(f"\nTranche {i}/{len(tranches)} : {debut_tranche} -> {fin_tranche}")
        where_clause = f"date_heure >= date'{debut_tranche}' and date_heure <= date'{fin_tranche}'"
        records = fetch_eco2mix_all(where_clause)
        print(f"  {len(records)} lignes extraites pour cette tranche.")
        if records:
            save_to_postgres(records)
            total_insere += len(records)
        time.sleep(1)  # politesse envers l'API

    print(f"\nBackfill terminé : {total_insere} lignes insérées/mises à jour au total.")
    print("Pense à relancer build_silver.py puis build_gold.py pour que les vues intègrent ce nouvel historique.")