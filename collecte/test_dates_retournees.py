from collect_prix import get_access_token, fetch_prix_marche, flatten_prix

token = get_access_token()
data = fetch_prix_marche(token, "2025-03-01T00:00:00+01:00", "2025-03-07T23:59:59+01:00")
points = flatten_prix(data)

dates = [p["date_heure"] for p in points if p["date_heure"]]
print(f"{len(points)} points reçus")
print("Date min renvoyée :", min(dates))
print("Date max renvoyée :", max(dates))
print("Plage demandée : 2025-03-01 -> 2025-03-07")