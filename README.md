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
│   ├── consommations_agregees/
│   ├── meteo_clean.csv
│   ├── consommations_enrichies.csv
│   ├── consommations_enrichies.parquet
│   ├── matrice_correlation.csv
│   ├── pics_consommation.csv               <- Détection anomalies 08
│   ├── sous_consommation.csv               <- Détection anomalies 08
│   ├── dpe_errone.csv                      <- Détection anomalies 08
│   ├── figures/                            <- Toutes les figures générées par les notebooks 09, 10 et 11
│   ├── logs/                               <- logs de 02_nettoyage_spark et 03_aggregation_spark
│   ├── tableaux_synthese/                  <- CSV de 06_statistiques_descriptives
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

### Scripts Jupyter

Les fichiers `.ipynb` ne sont pas indépendants et doivent être lancé dans l'ordre numérique.
Ces fichiers seront exécutés via `Jupyter lab` en veillant à sélectionner pour kernel l'environnment virtuel précédemment créé.

### Pipelines de traitement

Pour lancer la pipeline de traitement `02_nettoyage_spark` utilisez la commande :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /notebooks/02_nettoyage_spark.py
```

#### Erreurs Java

L'utilisation d'un notebook pour la partie `03_agregations_spark` m'a posé des difficultés.
En chargeant le fichier PARQUET `output/consommation_clean` via la la bibliotèque spark dans un notebook Jupyter comme suit,

```python
spark.read.parquet("../output/consommation_clean")
```

j'obtiens l'erreur suivante :

```txt
Py4JJavaError: An error occurred while calling o61.parquet.: java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
```

J'ai essayé de charge le fichier PARQUET avec Pandas puis de créer un DataFrame Spark, cependant lors de l'exécution du notebook Jupyter, j'obtiens l'erreur suivante :

```txt
ConnectionRefusedError: [WinError 10061] Aucune connexion n’a pu être établie car l’ordinateur cible l’a expressément refusée
```

Pour générer le fichier Parquet `consommations_agregees`, j'ai donc créé un script python `03_agregations_spark.py` à éxécuter dans le conteneur spark.
Pour lancer la pipeline de traitement `03_agregations_spark`, utilisez la commande :

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /notebooks/03_agregations_spark.py
```
