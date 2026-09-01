"""
Test rapide (ne touche pas à PostgreSQL) : l'API RTE Consumption accepte-t-elle
une plage de dates remontant à l'hiver dernier (décembre 2025) ?
"""
import os
import base64
import requests

RTE_CLIENT_ID = os.environ.get("RTE_CLIENT_ID", "")
RTE_CLIENT_SECRET = os.environ.get("RTE_CLIENT_SECRET", "")
RTE_BASIC_AUTH = os.environ.get("RTE_BASIC_AUTH", "")

TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
BASE_URL = "https://digital.iservices.rte-france.com/open_api/consumption/v1"


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


if __name__ == "__main__":
    # Une semaine de décembre 2025 (hiver dernier), même format que collect_rte.py
    start = "2025-12-01T00:00:00+01:00"
    end = "2025-12-07T23:59:59+01:00"

    print(f"Test : consommation nationale du {start} au {end}")
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start, "end_date": end}
    response = requests.get(f"{BASE_URL}/short_term", headers=headers, params=params)

    print("Code de statut HTTP :", response.status_code)
    if response.ok:
        data = response.json()
        total_points = sum(len(block["values"]) for block in data["short_term"])
        print(f"SUCCÈS : {total_points} points récupérés pour cette semaine de décembre 2025.")
        print("=> L'API accepte de remonter jusqu'à l'hiver dernier, le backfill est possible.")
    else:
        print("ÉCHEC. Réponse brute de RTE :")
        print(response.text)
        print("=> L'API refuse cette plage de dates historique (voir le message ci-dessus pour la raison).")