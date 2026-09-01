from collect_prix import get_access_token, fetch_prix_marche

token = get_access_token()

plages = [
    ("2025-03-01T00:00:00+01:00", "2025-03-07T23:59:59+01:00"),   # déjà testé : ÉCHEC
    ("2025-03-08T00:00:00+01:00", "2025-03-14T23:59:59+01:00"),
    ("2025-03-15T00:00:00+01:00", "2025-03-21T23:59:59+01:00"),
    ("2025-03-21T00:00:00+01:00", "2025-03-27T23:59:59+01:00"),   # déjà testé : OK
]

for start, end in plages:
    try:
        data = fetch_prix_marche(token, start, end)
        print(f"OK : {start} -> {end}")
    except Exception as e:
        print(f"ÉCHEC : {start} -> {end} — {e}")