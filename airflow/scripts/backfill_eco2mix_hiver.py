"""
Backfill ponctuel (à exécuter une seule fois) : récupère le mix énergétique
et l'intensité carbone depuis décembre 2025 jusqu'à la donnée la plus récente
disponible, via l'API ODRE (Opendatasoft), dataset eco2mix-national-cons-def
(données consolidées/définitives, historique depuis 2012, pas demi-heure).
Couvre notamment le pic historique du 7 janvier 2026 (vague de froid,
88 244 MW), pour permettre une comparaison hiver/été de la corrélation
carbone/consommation. Réutilise le même schéma de table (fact_eco2mix) que
collect_eco2mix.py.

Note 1 : ce dataset a un décalage de publication d'environ 1 mois (les
données du mois M sont livrées mi-M+1) — la couverture s'arrêtera donc
naturellement avant la date du jour. Le dataset temps réel
(eco2mix-national-tr, voir collect_eco2mix.py) couvre déjà les ~2 derniers
mois, donc les deux sources se complètent sans trou de données.

Note 2 : l'API Opendatasoft refuse toute requête où offset + limit > 10000.
On découpe donc la période en tranches mensuelles (chaque mois ~2000 lignes,
largement sous la limite) et on repagine à zéro pour chaque mois.
"""
import os
import requests
import psycopg2
from datetime import datetime
from calendar import monthrange

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

ECO2MIX_CONS_DEF_URL = "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/eco2mix-national-cons-def/records"
PAGE_SIZE = 100

BACKFILL_START = datetime(2025, 12, 1)
BACKFILL_END = datetime.now()


def generer_tranches_mensuelles(debut: datetime, fin: datetime):
    """Découpe [debut, fin] en tranches mensuelles (année, mois, premier_jour, dernier_jour)."""
    annee, mois = debut.year, debut.month
    while (annee, mois) <= (fin.year, fin.month):
        premier_jour = datetime(annee, mois, 1)
        dernier_jour_du_mois = monthrange(annee, mois)[1]
        dernier_jour = datetime(annee, mois, dernier_jour_du_mois, 23, 59, 59)
        yield (
            max(premier_jour, debut),
            min(dernier_jour, fin),
        )
        mois += 1
        if mois > 12:
            mois = 1
            annee += 1


def fetch_eco2mix_page(where_clause: str, limit: int, offset: int) -> dict:
    """Récupère une page de résultats (l'API plafonne généralement à 100 lignes par appel)."""
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


def fetch_eco2mix_periode(debut: datetime, fin: datetime) -> list[dict]:
    """Récupère toutes les lignes d'une tranche (max 10 000 lignes, largement suffisant pour un mois)."""
    where_clause = (
        f"date_heure >= date'{debut.strftime('%Y-%m-%dT%H:%M:%S')}' "
        f"and date_heure <= date'{fin.strftime('%Y-%m-%dT%H:%M:%S')}'"
    )
    all_records = []
    offset = 0
    while True:
        page = fetch_eco2mix_page(where_clause, PAGE_SIZE, offset)
        records = page.get("results", [])
        if not records:
            break
        all_records.extend(records)
        total = page.get("total_count", len(all_records))
        offset += PAGE_SIZE
        if offset >= total:
            break
    return all_records


def save_to_postgres(rows: list[dict]) -> None:
    """Insère les lignes dans la même table fact_eco2mix (upsert sur date_heure)."""
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
    print(f"Backfill eCO2mix du {BACKFILL_START.date()} au {BACKFILL_END.date()}, mois par mois...")
    total_insere = 0
    for debut_mois, fin_mois in generer_tranches_mensuelles(BACKFILL_START, BACKFILL_END):
        print(f"  Mois {debut_mois.strftime('%Y-%m')} ({debut_mois.date()} -> {fin_mois.date()})...")
        records = fetch_eco2mix_periode(debut_mois, fin_mois)
        print(f"    {len(records)} lignes récupérées, insertion en base...")
        save_to_postgres(records)
        total_insere += len(records)

    print(f"Terminé : {total_insere} lignes insérées/mises à jour au total dans PostgreSQL (table fact_eco2mix).")