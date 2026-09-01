"""
Analyse exploratoire sous Python (pandas, numpy, scipy, matplotlib) des
données réelles collectées par EnergiFlow (couche Silver dans PostgreSQL).
Reproduit et vérifie les résultats déjà obtenus en SQL (CORR()), et génère
les graphiques destinés au dossier du Bloc 2.
"""
import os
import pandas as pd
import numpy as np
from scipy import stats
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "energiflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "energiflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "energiflow")

OUTPUT_DIR = "/app/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

print("Extraction des données depuis PostgreSQL (schéma silver)...")
df_national = pd.read_sql("SELECT * FROM silver.conso_meteo_prix ORDER BY date_heure", engine)
df_regional = pd.read_sql("SELECT * FROM silver.consommation_regionale ORDER BY date_heure", engine)
print(f"{len(df_national)} lignes récupérées (national), {len(df_regional)} lignes (régional).")

# ------------------------------------------------------------------
# 1. Statistiques descriptives
# ------------------------------------------------------------------
desc = df_national["consommation_mw"].describe()
print("\nStatistiques descriptives - consommation nationale (MW) :")
print(desc)
desc.to_csv(f"{OUTPUT_DIR}/stats_descriptives.csv")

# ------------------------------------------------------------------
# 2. Corrélation température / consommation (vérifie le résultat SQL CORR())
# ------------------------------------------------------------------
df_temp = df_national.dropna(subset=["temperature_moyenne_nationale", "consommation_mw"])
if len(df_temp) >= 2:
    r_temp, p_temp = stats.pearsonr(df_temp["temperature_moyenne_nationale"], df_temp["consommation_mw"])
    print(f"\nCorrélation température / consommation : r = {r_temp:.3f}, p = {p_temp:.4g} (n={len(df_temp)})")
else:
    r_temp, p_temp = None, None
    print("\nPas assez de données pour la corrélation température/consommation.")

# ------------------------------------------------------------------
# 3. Corrélation prix / consommation
# ------------------------------------------------------------------
df_prix = df_national.dropna(subset=["prix_eur_mwh", "consommation_mw"])
if len(df_prix) >= 2:
    r_prix, p_prix = stats.pearsonr(df_prix["prix_eur_mwh"], df_prix["consommation_mw"])
    print(f"Corrélation prix / consommation : r = {r_prix:.3f}, p = {p_prix:.4g} (n={len(df_prix)})")
else:
    r_prix, p_prix = None, None
    print("Pas encore assez de chevauchement entre prix et consommation pour calculer la corrélation "
          "(fact_prix_marche et fact_consommation ne se recoupent pas encore dans le temps).")

# ------------------------------------------------------------------
# 4. Graphique 1 : évolution de la consommation nationale
# ------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(9, 4))
ax.plot(df_national["date_heure"], df_national["consommation_mw"], color="#1f5c8a", linewidth=1)
ax.set_title("Consommation électrique nationale (données réelles)")
ax.set_ylabel("Consommation (MW)")
fig.autofmt_xdate(rotation=30)
fig.tight_layout()
fig.savefig(f"{OUTPUT_DIR}/chart_consommation_nationale.png", dpi=150)
plt.close(fig)

# ------------------------------------------------------------------
# 5. Graphique 2 : consommation moyenne par région
# ------------------------------------------------------------------
reg_moy = df_regional.groupby("region")["consommation_mw"].mean().sort_values()
fig, ax = plt.subplots(figsize=(7, 5))
ax.barh(reg_moy.index, reg_moy.values, color="#5b9bd5")
ax.set_title("Consommation moyenne par région (données réelles)")
ax.set_xlabel("Consommation moyenne (MW)")
fig.tight_layout()
fig.savefig(f"{OUTPUT_DIR}/chart_consommation_regionale.png", dpi=150)
plt.close(fig)

# ------------------------------------------------------------------
# 6. Graphique 3 : nuage de points température / consommation
# ------------------------------------------------------------------
if len(df_temp) >= 2:
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    ax.scatter(df_temp["temperature_moyenne_nationale"], df_temp["consommation_mw"],
               s=12, alpha=0.5, color="#5b9bd5")
    z = np.polyfit(df_temp["temperature_moyenne_nationale"], df_temp["consommation_mw"], 1)
    xs = np.linspace(df_temp["temperature_moyenne_nationale"].min(),
                      df_temp["temperature_moyenne_nationale"].max(), 50)
    ax.plot(xs, np.polyval(z, xs), color="#c0392b", linewidth=2)
    ax.set_title(f"Consommation vs température (r = {r_temp:.2f})")
    ax.set_xlabel("Température moyenne nationale (°C)")
    ax.set_ylabel("Consommation (MW)")
    fig.tight_layout()
    fig.savefig(f"{OUTPUT_DIR}/chart_correlation_temperature.png", dpi=150)
    plt.close(fig)

# ------------------------------------------------------------------
# Note sur le test hiver/été
# ------------------------------------------------------------------
mois_presents = pd.to_datetime(df_national["date_heure"]).dt.month.unique()
if len(mois_presents) < 2 or (set(mois_presents) & {12, 1, 2} == set() or set(mois_presents) & {6, 7, 8} == set()):
    print("\nRemarque : la fenêtre de données actuelle ne couvre pas à la fois l'hiver et l'été "
          "(mois présents : " + ", ".join(map(str, sorted(mois_presents))) + "). "
          "Le test statistique hiver/été ne peut pas être réalisé sur des données réelles pour l'instant "
          "— à refaire une fois plusieurs mois de collecte accumulés.")

# ------------------------------------------------------------------
# 7. Matrice de corrélation entre toutes les variables numériques
# ------------------------------------------------------------------
corr_cols = ["consommation_mw", "temperature_moyenne_nationale", "vent_moyen_national", "prix_eur_mwh"]
corr_matrix = df_national[corr_cols].corr()
print("\nMatrice de corrélation :")
print(corr_matrix.round(3))
corr_matrix.to_csv(f"{OUTPUT_DIR}/matrice_correlation.csv")

fig, ax = plt.subplots(figsize=(6.5, 5.5))
im = ax.imshow(corr_matrix, cmap="coolwarm", vmin=-1, vmax=1)
ax.set_xticks(range(len(corr_cols)))
ax.set_xticklabels(corr_cols, rotation=45, ha="right")
ax.set_yticks(range(len(corr_cols)))
ax.set_yticklabels(corr_cols)
for i in range(len(corr_cols)):
    for j in range(len(corr_cols)):
        val = corr_matrix.iloc[i, j]
        text = f"{val:.2f}" if pd.notna(val) else "n/a"
        ax.text(j, i, text, ha="center", va="center", color="black", fontsize=9)
fig.colorbar(im, ax=ax, label="Coefficient de corrélation (Pearson)")
ax.set_title("Matrice de corrélation")
fig.tight_layout()
fig.savefig(f"{OUTPUT_DIR}/chart_matrice_correlation.png", dpi=150)
plt.close(fig)

# ------------------------------------------------------------------
# 8. Boxplot : distribution de la consommation par heure de la journée
#    (on n'a pas encore de saisonnalité en base, mais le cycle horaire
#    quotidien, lui, est déjà pleinement observable)
# ------------------------------------------------------------------
df_national["heure"] = pd.to_datetime(df_national["date_heure"]).dt.hour
data_by_hour = [df_national.loc[df_national["heure"] == h, "consommation_mw"].dropna().values for h in range(24)]
fig, ax = plt.subplots(figsize=(11, 5))
ax.boxplot(data_by_hour, tick_labels=list(range(24)))
ax.set_title("Distribution de la consommation nationale par heure de la journée")
ax.set_xlabel("Heure")
ax.set_ylabel("Consommation (MW)")
fig.tight_layout()
fig.savefig(f"{OUTPUT_DIR}/chart_boxplot_horaire.png", dpi=150)
plt.close(fig)

# ------------------------------------------------------------------
# 9. Détection de dérives / valeurs aberrantes (z-score au-delà de ±3)
# ------------------------------------------------------------------
moyenne = df_national["consommation_mw"].mean()
ecart_type = df_national["consommation_mw"].std()
df_national["z_score_consommation"] = (df_national["consommation_mw"] - moyenne) / ecart_type
outliers = df_national.loc[
    df_national["z_score_consommation"].abs() > 3,
    ["date_heure", "consommation_mw", "z_score_consommation"]
]
print(f"\n{len(outliers)} valeur(s) aberrante(s) détectée(s) (|z-score| > 3, sur {len(df_national)} lignes) :")
if len(outliers) > 0:
    print(outliers.to_string(index=False))
else:
    print("Aucune dérive significative détectée sur la fenêtre de données actuelle.")
outliers.to_csv(f"{OUTPUT_DIR}/valeurs_aberrantes.csv", index=False)

# ------------------------------------------------------------------
# 10. Normalisation (min-max) et standardisation (z-score)
# ------------------------------------------------------------------
df_norm = df_national.copy()
col = "consommation_mw"
df_norm["consommation_mw_standardisee"] = (df_norm[col] - moyenne) / ecart_type
df_norm["consommation_mw_normalisee_minmax"] = (df_norm[col] - df_norm[col].min()) / (df_norm[col].max() - df_norm[col].min())
df_norm.to_csv(f"{OUTPUT_DIR}/donnees_normalisees.csv", index=False)
print("\nDonnées standardisées (z-score) et normalisées (min-max) enregistrées dans donnees_normalisees.csv")
print("  - consommation_mw_standardisee : moyenne 0, écart-type 1 (utile pour comparer des variables d'échelles différentes)")
print("  - consommation_mw_normalisee_minmax : ramenée entre 0 et 1 (utile pour certains algorithmes de ML)")

print(f"\nFichiers générés dans {OUTPUT_DIR}/ :")
print("  - stats_descriptives.csv")
print("  - matrice_correlation.csv")
print("  - valeurs_aberrantes.csv")
print("  - donnees_normalisees.csv")
print("  - chart_consommation_nationale.png")
print("  - chart_consommation_regionale.png")
print("  - chart_correlation_temperature.png")
print("  - chart_matrice_correlation.png")
print("  - chart_boxplot_horaire.png")
print("Terminé.")