# EnergiFlow

Plateforme Data de surveillance et d'anticipation de la consommation électrique en France, développée dans le cadre du Mastère 2 Data Engineering (RNCP39586, Ynov Campus).

## Problématique

Comment concevoir une infrastructure data permettant de surveiller en temps réel la consommation électrique nationale, d'anticiper les pics de demande et d'informer les opérateurs énergétiques pour réduire les coûts et l'empreinte carbone ?

## Architecture

Le projet repose sur une architecture en trois couches :

- **Bronze** : données brutes collectées depuis les sources externes
- **Silver** : données nettoyées et normalisées (vues SQL)
- **Gold** : données agrégées prêtes pour l'analyse (vues matérialisées)

### Sources de données

| Source | Donnée | Script |
|---|---|---|
| API RTE Consumption | Consommation électrique nationale | `collect_rte.py` |
| API Open-Meteo | Données météo (13 régions) | `collect_meteo.py` |
| API RTE Wholesale Market | Prix de marché | `collect_prix.py` |
| ODRE (Opendatasoft) | Consommation régionale | `collect_odre.py` |
| Actualités News Environnement | Actualités énergie (scraping) | `collect_scraping.py` |

### Stack technique

- **PostgreSQL** : stockage (Bronze/Silver/Gold)
- **Apache Airflow** : orchestration et automatisation (DAG quotidien, 6h)
- **Docker / Docker Compose** : conteneurisation
- **Python** : collecte, transformation, analyse (pandas, SQLAlchemy, scipy)
- **Tableau Public** : dashboard de restitution

## Structure du dépôt

\`\`\`
.
├── airflow/            # Orchestration
│   ├── dags/            # DAG de collecte quotidienne
│   └── scripts/         # Scripts de collecte et transformation exécutés par Airflow
├── collecte/            # Scripts de collecte et transformation (conteneurs autonomes)
├── analyse/              # Notebooks et scripts d'analyse statistique
└── docker-compose.yml   # Orchestration des conteneurs
\`\`\`

## Démarrage

\`\`\`bash
# Copier le fichier d'environnement et renseigner les identifiants API
cp collecte/.env.example collecte/.env

# Lancer l'infrastructure
docker compose up -d

# Airflow est accessible sur http://localhost:8090
\`\`\`

## Projet

Ce projet est réalisé individuellement dans le cadre de la certification RNCP39586 - Expert(e) en science des données, et couvre progressivement les 5 blocs de compétences :

- **Bloc 1** — Collecte, transformation et sécurisation des données
- **Bloc 2** — Analyse, organisation et valorisation des données
- **Bloc 3** — Élaboration et pilotage du projet
- **Bloc 4** — Conception et exploitation de l'infrastructure Data