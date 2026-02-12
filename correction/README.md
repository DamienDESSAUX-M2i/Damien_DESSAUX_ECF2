# ECF - Analyse et prediction de la consommation energetique des batiments publics

## Titre Professionnel Data Engineer - RNCP35288
### Competences evaluees : C2.1, C2.2, C2.3, C2.4

---

## Structure du projet

```
ecf_energie/
├── README.md
├── requirements.txt
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
└── output/
    ├── consommations_clean/          # Parquet partitionne
    ├── consommations_agregees.parquet
    ├── meteo_clean.csv
    ├── consommations_enrichies.csv
    ├── consommations_enrichies.parquet
    ├── matrice_correlation.csv
    ├── anomalies_detectees.csv
    ├── figures/
    └── rapport_synthese.md
```

## Installation

```bash
pip install -r requirements.txt
```

## Execution du pipeline complet

### Etape 1 : Exploration initiale (PySpark)
```bash
jupyter notebook notebooks/01_exploration_spark.ipynb
```

### Etape 2 : Nettoyage des consommations (PySpark)
```bash
cd notebooks
python 02_nettoyage_spark.py
```
ou
```bash
spark-submit notebooks/02_nettoyage_spark.py
```

### Etape 3 : Agregations avancees (PySpark)
```bash
jupyter notebook notebooks/03_agregations_spark.ipynb
```

### Etape 4 : Nettoyage meteo (Pandas)
```bash
jupyter notebook notebooks/04_nettoyage_meteo_pandas.ipynb
```

### Etape 5 : Fusion et enrichissement
```bash
jupyter notebook notebooks/05_fusion_enrichissement.ipynb
```

### Etape 6 : Statistiques descriptives
```bash
jupyter notebook notebooks/06_statistiques_descriptives.ipynb
```

### Etape 7 : Analyse des correlations
```bash
jupyter notebook notebooks/07_analyse_correlations.ipynb
```

### Etape 8 : Detection d'anomalies
```bash
jupyter notebook notebooks/08_detection_anomalies.ipynb
```

### Etape 9 : Visualisations Matplotlib
```bash
jupyter notebook notebooks/09_visualisations_matplotlib.ipynb
```

### Etape 10 : Visualisations Seaborn
```bash
jupyter notebook notebooks/10_visualisations_seaborn.ipynb
```

### Etape 11 : Dashboard executif
```bash
jupyter notebook notebooks/11_dashboard_executif.ipynb
```

## Ordre d'execution

Les notebooks doivent etre executes dans l'ordre numerique car chaque etape depend des resultats de la precedente :

1. `01_exploration_spark.ipynb` - Audit qualite des donnees brutes
2. `02_nettoyage_spark.py` - Nettoyage et export Parquet
3. `03_agregations_spark.ipynb` - Agregations et vues SQL
4. `04_nettoyage_meteo_pandas.ipynb` - Nettoyage donnees meteo
5. `05_fusion_enrichissement.ipynb` - Fusion de toutes les sources
6. `06_statistiques_descriptives.ipynb` - Analyse statistique
7. `07_analyse_correlations.ipynb` - Correlations
8. `08_detection_anomalies.ipynb` - Detection anomalies
9. `09_visualisations_matplotlib.ipynb` - Graphiques Matplotlib
10. `10_visualisations_seaborn.ipynb` - Graphiques Seaborn
11. `11_dashboard_executif.ipynb` - Dashboard final

## Environnement technique

- Python 3.9+
- PySpark 3.5+
- Pandas 2.0+
- Matplotlib 3.8+
- Seaborn 0.13+
- Jupyter Notebook / JupyterLab

## Donnees

- **batiments.csv** : 146 batiments publics (propre)
- **consommations_raw.csv** : ~7.7M lignes de consommation (avec defauts)
- **meteo_raw.csv** : ~252K lignes meteo (avec defauts)
- **tarifs_energie.csv** : 10 lignes de tarifs (propre)

