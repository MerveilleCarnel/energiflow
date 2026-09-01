"""
Construction de la couche Gold : indicateurs pré-calculés (vues matérialisées)
à partir de la couche Silver, prêts à être branchés directement sur un
tableau de bord (Power BI) sans recalcul à la volée.
"""
import os
import psycopg2

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

SQL = """
CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Consommation nationale moyenne/pic/creux, par mois
DROP MATERIALIZED VIEW IF EXISTS gold.consommation_mensuelle;
CREATE MATERIALIZED VIEW gold.consommation_mensuelle AS
SELECT
    date_trunc('month', date_heure) AS mois,
    ROUND(AVG(consommation_mw), 0)  AS consommation_moyenne_mw,
    ROUND(MAX(consommation_mw), 0)  AS pic_mw,
    ROUND(MIN(consommation_mw), 0)  AS creux_mw
FROM silver.consommation_nationale
GROUP BY mois
ORDER BY mois;

-- 2. Top 10 des pics de consommation nationale (toutes périodes confondues)
DROP MATERIALIZED VIEW IF EXISTS gold.top_pics_nationaux;
CREATE MATERIALIZED VIEW gold.top_pics_nationaux AS
SELECT date_heure, consommation_mw
FROM silver.consommation_nationale
ORDER BY consommation_mw DESC
LIMIT 10;

-- 3. Consommation moyenne : les 3 régions les plus consommatrices + le reste agrégé
DROP MATERIALIZED VIEW IF EXISTS gold.consommation_regions_cles;
CREATE MATERIALIZED VIEW gold.consommation_regions_cles AS
WITH classement AS (
    SELECT
        region,
        AVG(consommation_mw) AS consommation_moyenne_mw,
        RANK() OVER (ORDER BY AVG(consommation_mw) DESC) AS rang
    FROM silver.consommation_regionale
    GROUP BY region
)
SELECT
    CASE WHEN rang <= 3 THEN region ELSE 'Autres régions' END AS region,
    ROUND(SUM(consommation_moyenne_mw), 0) AS consommation_moyenne_mw
FROM classement
GROUP BY 1
ORDER BY consommation_moyenne_mw DESC;

-- 4. Corrélations clés (Pearson), calculées directement en SQL
DROP MATERIALIZED VIEW IF EXISTS gold.correlations;
CREATE MATERIALIZED VIEW gold.correlations AS
SELECT
    ROUND(CORR(temperature_moyenne_nationale, consommation_mw)::numeric, 3) AS r_temperature_consommation,
    ROUND(CORR(prix_eur_mwh, consommation_mw)::numeric, 3)                  AS r_prix_consommation
FROM silver.conso_meteo_prix;
"""


def build_gold() -> None:
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
    print("Construction de la couche Gold (schéma 'gold')...")
    build_gold()
    print("Vues matérialisées Gold créées/rafraîchies avec succès :")
    print("  - gold.consommation_mensuelle")
    print("  - gold.top_pics_nationaux")
    print("  - gold.consommation_regions_cles")
    print("  - gold.correlations")