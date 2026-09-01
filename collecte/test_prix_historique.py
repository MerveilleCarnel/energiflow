"""
Test rapide (ne touche pas à PostgreSQL) : l'API RTE Wholesale Market
conserve-t-elle un historique de prix, ou seulement le prix du lendemain ?
"""
import os
import base64
import requests

RTE_CLIENT_ID = os.environ.get("RTE_CLIENT_ID", "")
RTE_CLIENT_SECRET = os.environ.get("RTE_CLIENT_SECRET", "")
RTE_BASIC_AUTH = os.environ.get("RTE_BASIC_AUTH", "")

TOKEN_URL = "https://digital.iservices.rte-france.com/token/oauth/"
BASE_URL = "https://digital.iservices.rte-france.com/open_api/wholesale_market/v3"


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
    start = "2025-12-01T00:00:00+01:00"
    end = "2025-12-07T23:59:59+01:00"

    print(f"Test : prix de marché du {start} au {end}")
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    params = {"start_date": start, "end_date": end}
    response = requests.get(f"{BASE_URL}/france_power_exchanges", headers=headers, params=params)

    print("Code de statut HTTP :", response.status_code)
    if response.ok:
        data = response.json()
        root_key = next(iter(data))
        print(f"SUCCÈS. Clé racine : '{root_key}', {len(data[root_key])} bloc(s) reçu(s).")
        print("=> L'API garde un historique de prix, le backfill est possible.")
    else:
        print("ÉCHEC. Réponse brute de RTE :")
        print(response.text)
        print("=> L'API ne conserve pas d'historique de prix accessible de cette façon.")
        
        