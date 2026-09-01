"""
Backfill ponctuel (à exécuter une seule fois) : récupère la météo historique
depuis janvier 2025 pour les 13 régions, via l'API Historical Weather
d'Open-Meteo (données de réanalyse ERA5, séparée de l'API temps réel).
Même schéma de table que collect_meteo.py.
"""
import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

BACKFILL_START = "2025-01-01"
# L'API archive ne couvre que jusqu'à 5 jours avant aujourd'hui
BACKFILL_END = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

REGIONS = {
    "Île-de-France":               (48.8566, 2.3522),
    "Auvergne-Rhône-Alpes":        (45.7640, 4.8357),
    "Nouvelle-Aquitaine":          (44.8378, -0.5792),
    "Occitanie":                   (43.6047, 1.4442),
    "Hauts-de-France":             (50.6292, 3.0573),
    "Grand Est":                   (48.5734, 7.7521),
    "Provence-Alpes-Côte d'Azur":  (43.2965, 5.3698),
    "Pays de la Loire":            (47.2184, -1.5536),
    "Normandie":                   (49.4432, 1.0999),
    "Bretagne":                    (48.1173, -1.6778),
    "Bourgogne-Franche-Comté":     (47.3220, 5.0415),
    "Centre-Val de Loire":         (47.9029, 1.9093),
    "Corse":                       (41.9192, 8.7386),
}


def fetch_meteo_historique(lat: float, lon: float) -> dict:
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": BACKFILL_START,
        "end_date": BACKFILL_END,
        "hourly": "temperature_2m,windspeed_10m,cloud_cover",
        "timezone": "UTC",
    }
    response = requests.get(ARCHIVE_URL, params=params, timeout=60)
    if not response.ok:
        print("  Réponse brute d'Open-Meteo :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def save_to_postgres(rows: list[dict]) -> None:
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            for r in rows:
                cur.execute("""
                    INSERT INTO fact_meteoid (date_heure, region, temperature_2m, windspeed_10m, cloud_cover_total)
                    VALUES (%(date_heure)s, %(region)s, %(temperature_2m)s, %(windspeed_10m)s, %(cloud_cover_total)s)
                    ON CONFLICT (date_heure, region) DO UPDATE
                    SET temperature_2m = EXCLUDED.temperature_2m,
                        windspeed_10m = EXCLUDED.windspeed_10m,
                        cloud_cover_total = EXCLUDED.cloud_cover_total;
                """, r)
    finally:
        conn.close()


if __name__ == "__main__":
    print(f"Backfill météo du {BACKFILL_START} au {BACKFILL_END}, pour {len(REGIONS)} régions...")
    total_rows = 0

    for region, (lat, lon) in REGIONS.items():
        print(f"  {region}...", end=" ")
        data = fetch_meteo_historique(lat, lon)
        times = data["hourly"]["time"]
        temps = data["hourly"]["temperature_2m"]
        winds = data["hourly"]["windspeed_10m"]
        clouds = data["hourly"]["cloud_cover"]

        rows = [
            {"date_heure": t, "region": region, "temperature_2m": temp,
             "windspeed_10m": wind, "cloud_cover_total": cloud}
            for t, temp, wind, cloud in zip(times, temps, winds, clouds)
        ]
        save_to_postgres(rows)
        total_rows += len(rows)
        print(f"{len(rows)} relevés insérés.")
        time.sleep(1)

    print(f"\nBackfill météo terminé : {total_rows} relevés insérés au total.")
    print("Pense à relancer build_silver.py puis build_gold.py.")