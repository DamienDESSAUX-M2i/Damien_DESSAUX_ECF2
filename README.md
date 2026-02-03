# ECF2 : Energie bâtiments

ECF2 de la formation Développeur Concepteur en Science des Donnée de M2i.

## Description du projet

L'objectif du projet est de construire un pipeline complet d'ingestion, de nettoyage, d'analyse et de visualisation des donnees de consommation electrique, de gaz et d'eau, enrichies par les donnees meteorologiques et les caracteristiques des batiments.

Les donnees sont disponibles dans le dossier `data_ecf/` et peuvent etre regenerees avec le script `generate_data_ecf.py`.

```bash
python3 generate_data_ecf.py
```
## Structure du projet

```
DAMIEN_DESSAUX_ECF2/
├── README.md
├── data/
│   ├── batiments.csv
│   ├── consommations_raw.csv
│   ├── meteo_raw.csv
│   └── tarifs_energie.csv
├── notebooks/
│   ├── 01_exploration_spark.ipynb
│   ├── 02_nettoyage_spark.py
│   ├── 03_agregations_spark.ipynb
│   ├── 04_nettoyage_meteo_pandas.ipynb
│   ├── 05_fusion_enrichissement.ipynb
│   ├── 06_statistiques_descriptives.ipynb
│   ├── 07_analyse_correlations.ipynb
│   ├── 08_detection_anomalies.ipynb
│   ├── 09_visualisations_matplotlib.ipynb
│   ├── 10_visualisations_seaborn.ipynb
│   └── 11_dashboard_executif.ipynb
├── output/
│   ├── consommations_clean/
│   ├── consommations_agregees.parquet
│   ├── meteo_clean.csv
│   ├── consommations_enrichies.csv
│   ├── consommations_enrichies.parquet
│   ├── matrice_correlation.csv
│   ├── anomalies_detectees.csv
│   ├── figures/
│   └── rapport_synthese.md
├── docker-compose.yaml
├── generate_data_ecf.py
└── requirements.txt

```

## Prérequis

- Docker et Docker Compose
- Python 3.11.9
- Git

## Installation

### 1. Cloner le projet depuis GitHub.

```bash
git clone https://github.com/DamienDESSAUX-M2i/Damien_DESSAUX_ECF2.git
```

### 2. Créer un environement virtuel et installer les dépendances.

```bash
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement
## Linux/Mac:
source venv/bin/activate
## Windows:
venv\Scripts\activate

# Installer les dépendances
pip install -r requirements.txt
```

### 3. Démarrer l'infrastructure Docker.

Quatre services seront lancés `spark-master`, `spark-worker-1`, `spark-worker-2` et `spark-worker-3`.

```bash
docker-compose up -d
```

## Utilisation

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /notebooks/02_nettoyage_spark.py
```
