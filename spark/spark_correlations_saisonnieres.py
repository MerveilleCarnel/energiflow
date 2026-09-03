"""
Traitement par calculs distribués (Apache Spark / PySpark) — 3e méthode de
traitement de la donnée du pipeline EnergiFlow, aux côtés du pipeline temps
réel (scripts collect_*.py) et de l'orchestrateur (Airflow).

Calcule les corrélations carbone/consommation séparément par saison
(hiver : déc-fév, été : juin-août) à partir de silver.conso_meteo_prix_carbone,
en s'appuyant sur les agrégations distribuées de Spark plutôt que sur des
calculs SQL classiques. Écrit le résultat dans gold.correlations_saisonnieres.
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, month, corr, col, isnan

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")


def get_engine():
    url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    return create_engine(url)


def main():
    print("Lecture de silver.conso_meteo_prix_carbone depuis PostgreSQL...")
    engine = get_engine()
    df_pandas = pd.read_sql("SELECT * FROM silver.conso_meteo_prix_carbone", engine)
    print(f"{len(df_pandas)} lignes chargées.")

    # Étape 1 : psycopg2 renvoie les colonnes NUMERIC sous forme de Decimal
    # (dtype 'object' en pandas), non interprétable par Spark. Conversion en float.
    colonnes_numeriques = [
        "consommation_mw", "temperature_moyenne_nationale", "vent_moyen_national",
        "prix_eur_mwh", "taux_co2", "ech_physiques",
        "nucleaire", "eolien", "solaire", "hydraulique", "gaz", "charbon", "fioul", "bioenergies",
    ]
    for c in colonnes_numeriques:
        if c in df_pandas.columns:
            df_pandas[c] = pd.to_numeric(df_pandas[c], errors="coerce")

    print("Démarrage de la session Spark (calcul distribué local)...")
    spark = SparkSession.builder.appName("EnergiFlow-CorrelationsSaisonnieres").master("local[*]").getOrCreate()

    df = spark.createDataFrame(df_pandas)

    df = df.withColumn(
        "saison",
        when(month("date_heure").isin([12, 1, 2]), "hiver")
        .when(month("date_heure").isin([6, 7, 8]), "ete")
        .otherwise("mi_saison"),
    )

    resultats = []
    for saison in ["hiver", "ete", "mi_saison"]:
        sous_ensemble = df.filter(df.saison == saison)
        n = sous_ensemble.count()
        if n < 2:
            continue

        # Étape 2 : les NaN issus de pandas ne sont PAS ignorés par Spark comme
        # le sont les NULL SQL (un seul NaN contamine tout le calcul). On filtre
        # donc explicitement les lignes valides avant chaque corrélation.
        valide_carbone = sous_ensemble.filter(
            col("taux_co2").isNotNull() & ~isnan("taux_co2")
            & col("consommation_mw").isNotNull() & ~isnan("consommation_mw")
        )
        r_carbone = valide_carbone.select(corr("taux_co2", "consommation_mw")).first()[0]

        valide_export = sous_ensemble.filter(
            col("ech_physiques").isNotNull() & ~isnan("ech_physiques")
            & col("taux_co2").isNotNull() & ~isnan("taux_co2")
        )
        r_export = valide_export.select(corr("ech_physiques", "taux_co2")).first()[0]

        resultats.append({
            "saison": saison,
            "nb_lignes": n,
            "r_carbone_consommation": round(r_carbone, 3) if r_carbone is not None else None,
            "r_export_carbone": round(r_export, 3) if r_export is not None else None,
        })
        print(f"  Saison {saison} ({n} lignes) : "
              f"r_carbone_consommation={resultats[-1]['r_carbone_consommation']}, "
              f"r_export_carbone={resultats[-1]['r_export_carbone']}")

    spark.stop()

    resultat_df = pd.DataFrame(resultats)
    resultat_df.to_sql("correlations_saisonnieres", engine, schema="gold", if_exists="replace", index=False)
    print("Résultats écrits dans gold.correlations_saisonnieres.")


if __name__ == "__main__":
    main()