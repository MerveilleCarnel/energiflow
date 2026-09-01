"""
Collecte des données météo (API Open-Meteo, gratuite, sans authentification)
pour les 13 régions métropolitaines, et sauvegarde en base PostgreSQL.
"""
import os
import requests
import psycopg2

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Coordonnées des chefs-lieux des 13 régions métropolitaines
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


def fetch_meteo(lat: float, lon: float) -> dict:
    """Récupère la météo horaire des 7 derniers jours pour une position donnée."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,windspeed_10m,cloud_cover",
        "past_days": 7,
        "forecast_days": 1,
        "timezone": "UTC",
    }
    response = requests.get(OPEN_METEO_URL, params=params)
    if not response.ok:
        print("Réponse brute d'Open-Meteo :", response.status_code, response.text)
    response.raise_for_status()
    return response.json()


def save_to_postgres(rows: list[dict]) -> None:
    """Crée la table si besoin et insère les relevés (upsert sur date_heure + region)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS fact_meteoid (
                    date_heure         TIMESTAMPTZ NOT NULL,
                    region             TEXT NOT NULL,
                    temperature_2m     NUMERIC,
                    windspeed_10m      NUMERIC,
                    cloud_cover_total  NUMERIC,
                    PRIMARY KEY (date_heure, region)
                );
            """)
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
    all_rows = []

    for region, (lat, lon) in REGIONS.items():
        print(f"Collecte météo : {region}...")
        data = fetch_meteo(lat, lon)
        times = data["hourly"]["time"]
        temps = data["hourly"]["temperature_2m"]
        winds = data["hourly"]["windspeed_10m"]
        clouds = data["hourly"]["cloud_cover"]

        for t, temp, wind, cloud in zip(times, temps, winds, clouds):
            all_rows.append({
                "date_heure": t,
                "region": region,
                "temperature_2m": temp,
                "windspeed_10m": wind,
                "cloud_cover_total": cloud,
            })

    print(f"{len(all_rows)} relevés météo extraits pour {len(REGIONS)} régions.")
    print("Exemple :", all_rows[0] if all_rows else "(aucun relevé)")

    save_to_postgres(all_rows)
    print(f"{len(all_rows)} relevés insérés/mis à jour dans PostgreSQL (table fact_meteoid).")