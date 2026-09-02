"""
Construction de la couche Silver : vues PostgreSQL nettoyées à partir des
tables brutes alimentées par les 6 collecteurs (Bloc 1 + Bloc 4).
Ne casse rien dans les tables sources : uniquement des CREATE OR REPLACE VIEW.
"""
import os
import psycopg2

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

SQL = """
CREATE SCHEMA IF NOT EXISTS silver;

-- Consommation nationale : uniquement les valeurs réalisées (pas les prévisions)
CREATE OR REPLACE VIEW silver.consommation_nationale AS
SELECT
    start_date AS date_heure,
    value_mw   AS consommation_mw
FROM public.fact_consommation
WHERE type = 'REALISED';

-- Consommation régionale : uniquement le statut consolidé (règle qualité ODRE)
CREATE OR REPLACE VIEW silver.consommation_regionale AS
SELECT
    date_heure,
    region,
    code_insee_region,
    consommation_brute_electricite_mw AS consommation_mw
FROM public.fact_consommation_regionale
WHERE statut_rte = 'Consolidé';

-- Météo par région, renommage cohérent
CREATE OR REPLACE VIEW silver.meteo AS
SELECT
    date_heure,
    region,
    temperature_2m,
    windspeed_10m,
    cloud_cover_total
FROM public.fact_meteoid;

-- Météo agrégée au niveau national (moyenne des 13 régions), pour être
-- jointe directement à la consommation nationale
CREATE OR REPLACE VIEW silver.meteo_nationale AS
SELECT
    date_heure,
    ROUND(AVG(temperature_2m), 2) AS temperature_moyenne_nationale,
    ROUND(AVG(windspeed_10m), 2)  AS vent_moyen_national
FROM silver.meteo
GROUP BY date_heure;

-- Prix de marché, on écarte les lignes sans valeur
CREATE OR REPLACE VIEW silver.prix_marche AS
SELECT date_heure, prix_eur_mwh
FROM public.fact_prix_marche
WHERE prix_eur_mwh IS NOT NULL;

-- Actualités scrapées, telles quelles (déjà propres à la collecte)
CREATE OR REPLACE VIEW silver.actualites AS
SELECT url_source, date_publication, titre, contenu_resume
FROM public.fact_actualites;

-- Dimension région : liste unique des régions connues (dédoublonnée)
CREATE OR REPLACE VIEW silver.dim_region AS
SELECT DISTINCT region, code_insee_region
FROM public.fact_consommation_regionale;

-- Mix énergétique, intensité carbone et échanges aux frontières (déjà propre à la collecte)
CREATE OR REPLACE VIEW silver.eco2mix AS
SELECT
    date_heure,
    consommation AS consommation_eco2mix_mw,
    nucleaire, eolien, solaire, hydraulique, gaz, charbon, fioul, bioenergies,
    taux_co2,
    ech_physiques
FROM public.fact_eco2mix;

-- Table d'analyse prête à l'emploi pour le Bloc 2 : consommation nationale,
-- météo moyenne nationale et prix de marché, alignés sur la même date_heure.
-- La météo est horaire (1 valeur/heure) alors que la consommation est au
-- quart d'heure (4 valeurs/heure) : on joint sur l'heure arrondie pour que
-- les 4 relevés d'une même heure partagent la même valeur météo.
CREATE OR REPLACE VIEW silver.conso_meteo_prix AS
SELECT
    c.date_heure,
    c.consommation_mw,
    m.temperature_moyenne_nationale,
    m.vent_moyen_national,
    p.prix_eur_mwh
FROM silver.consommation_nationale c
LEFT JOIN silver.meteo_nationale m ON date_trunc('hour', c.date_heure) = m.date_heure
LEFT JOIN silver.prix_marche p     ON c.date_heure = p.date_heure
ORDER BY c.date_heure;

-- Table d'analyse étendue pour le Bloc 4 : consommation, météo, prix ET carbone
-- (dont les échanges aux frontières), alignés sur la même date_heure (les deux
-- sources RTE partagent le même pas de 15 min).
CREATE OR REPLACE VIEW silver.conso_meteo_prix_carbone AS
SELECT
    c.date_heure,
    c.consommation_mw,
    m.temperature_moyenne_nationale,
    m.vent_moyen_national,
    p.prix_eur_mwh,
    e.taux_co2,
    e.nucleaire, e.eolien, e.solaire, e.hydraulique, e.gaz, e.charbon, e.fioul, e.bioenergies,
    e.ech_physiques
FROM silver.consommation_nationale c
LEFT JOIN silver.meteo_nationale m ON date_trunc('hour', c.date_heure) = m.date_heure
LEFT JOIN silver.prix_marche p     ON c.date_heure = p.date_heure
LEFT JOIN silver.eco2mix e         ON c.date_heure = e.date_heure
ORDER BY c.date_heure;
"""


def build_silver() -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(SQL)
    finally:
        conn.close()


if __name__ == "__main__":
    print("Construction de la couche Silver (schéma 'silver')...")
    build_silver()
    print("Vues Silver créées/mises à jour avec succès :")
    print("  - silver.consommation_nationale")
    print("  - silver.consommation_regionale")
    print("  - silver.meteo")
    print("  - silver.meteo_nationale")
    print("  - silver.prix_marche")
    print("  - silver.actualites")
    print("  - silver.dim_region")
    print("  - silver.eco2mix")
    print("  - silver.conso_meteo_prix  (table d'analyse combinée pour le Bloc 2)")
    print("  - silver.conso_meteo_prix_carbone  (table d'analyse combinée pour le Bloc 4)")