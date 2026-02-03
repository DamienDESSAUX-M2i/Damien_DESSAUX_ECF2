# ECF - Concepteur d'Applications de Donnees

## Titre Professionnel Data Engineer - RNCP35288
### Competences evaluees : C2.1, C2.2, C2.3, C2.4

## Titre de l'ECF
**Analyse et prediction de la consommation energetique des batiments publics**

---

## Contexte professionnel

Vous etes Data Engineer au sein d'une agence regionale de transition energetique. L'agence souhaite developper une plateforme d'analyse de la consommation energetique des batiments publics (ecoles, mairies, gymnases, piscines) afin d'identifier les leviers d'optimisation et de reduction des couts.

Vous devez construire un pipeline complet d'ingestion, de nettoyage, d'analyse et de visualisation des donnees de consommation electrique, de gaz et d'eau, enrichies par les donnees meteorologiques et les caracteristiques des batiments.

---

## Objectifs de l'ECF

Demontrer votre maitrise des competences du referentiel RNCP35288 :

### C2.1 - Collecter des donnees en respectant les normes et standards de l'entreprise
- Charger des donnees volumineuses depuis differentes sources (CSV, fichiers meteo)
- Identifier et documenter les problemes de qualite des donnees sources
- Respecter les contraintes techniques (formats, volumetrie)

### C2.2 - Traiter des donnees structurees avec un langage de programmation
- Implementer des pipelines de transformation avec PySpark
- Nettoyer et standardiser les donnees avec Pandas
- Creer des fonctions de traitement reutilisables (UDF Spark, fonctions Pandas)
- Gerer les erreurs et les cas limites

### C2.3 - Analyser des donnees structurees pour repondre a un besoin metier
- Calculer des indicateurs cles de performance (KPI) energetiques
- Identifier des correlations entre consommation et facteurs externes (meteo, caracteristiques batiments)
- Detecter des anomalies et proposer des actions correctives
- Produire des analyses statistiques exploitables

### C2.4 - Presenter des donnees analysees de facon intelligible
- Concevoir des visualisations professionnelles avec Matplotlib
- Realiser des analyses visuelles avancees avec Seaborn
- Construire un dashboard executif synthetique multi-panneaux
- Rediger un rapport de synthese avec recommandations actionnables

---

## Donnees fournies

Les donnees sont disponibles dans le dossier `data_ecf/` et peuvent etre regenerees avec le script `generate_data_ecf.py`.

```bash
python3 generate_data_ecf.py
```

### 1. Batiments (batiments.csv)
Referentiel propre des batiments publics de la region.

**Colonnes** :
- `batiment_id` : Identifiant unique (ex: BAT0001)
- `nom` : Nom du batiment
- `type` : Type (ecole, mairie, gymnase, piscine, mediatheque)
- `commune` : Commune
- `surface_m2` : Surface en m2
- `annee_construction` : Annee de construction (1950-2020)
- `classe_energetique` : DPE (A, B, C, D, E, F, G)
- `nb_occupants_moyen` : Nombre moyen d'occupants

**Caracteristiques** :
- 146 batiments
- 15 communes (Paris, Lyon, Marseille, Toulouse, Bordeaux, Lille, Nantes, Strasbourg, Montpellier, Nice, Rennes, Reims, Le Havre, Saint-Etienne, Toulon)
- Donnees propres (pas de defauts)
- Taille fichier : 8.7 KB

### 2. Consommations energetiques (consommations_raw.csv)
Donnees de consommation brutes avec defauts intentionnels.

**Colonnes** :
- `batiment_id` : Identifiant du batiment
- `timestamp` : Date et heure de la mesure
- `type_energie` : Type (electricite, gaz, eau)
- `consommation` : Valeur de consommation
- `unite` : Unite (kWh pour electricite/gaz, m3 pour eau)

**Defauts intentionnels** :
- Formats de dates multiples (ISO, FR, US, ISO avec T)
- Valeurs negatives (erreurs de capteurs - 0.5%)
- Valeurs aberrantes pics >15000 (1%)
- Doublons (2%)
- Separateurs decimaux mixtes (12% avec virgule au lieu de point)
- Valeurs textuelles ("erreur", "N/A", "---", "null" - 1.5%)
- Periodes de donnees manquantes aleatoires (1%)

**Caracteristiques** :
- 7 758 868 lignes
- Periode : 2023-01-01 a 2024-12-31 (2 ans)
- Mesures horaires pour 146 batiments x 3 types d'energie
- Taille fichier : 333 MB

### 3. Donnees meteorologiques (meteo_raw.csv)
Donnees meteo par commune avec defauts.

**Colonnes** :
- `commune` : Nom de la commune
- `timestamp` : Date et heure
- `temperature_c` : Temperature en degres Celsius
- `humidite_pct` : Humidite en %
- `rayonnement_solaire_wm2` : Rayonnement solaire en W/m2
- `vitesse_vent_kmh` : Vitesse du vent en km/h
- `precipitation_mm` : Precipitations en mm

**Defauts intentionnels** :
- Valeurs manquantes (2% des enregistrements supprimes aleatoirement)
- Temperatures impossibles (<-70 ou >100 - 0.8%)
- Humidite >100% (1.5%)
- Rayonnement solaire negatif (0.5%)
- Formats de dates incoherents (4 formats differents)
- Valeurs textuelles dans temperature ("", "NA", "null" - 2%)
- Separateurs decimaux mixtes (8% avec virgule)

**Caracteristiques** :
- 252 612 lignes
- Periode : 2023-01-01 a 2024-12-31 (2 ans)
- Mesures horaires pour 15 communes
- Taille fichier : 13 MB

### 4. Referentiel tarifaire (tarifs_energie.csv)
Tarifs de l'energie par periode (propre).

**Colonnes** :
- `date_debut` : Date de debut de validite (YYYY-MM-DD)
- `date_fin` : Date de fin de validite (YYYY-MM-DD)
- `type_energie` : Type d'energie (electricite, gaz, eau)
- `tarif_unitaire` : Tarif en euros

**Caracteristiques** :
- 10 lignes
- Periode : 2023-2024
- Tarifs semestriels pour electricite et gaz, annuels pour eau
- Donnees propres (pas de defauts)
- Taille fichier : 398 bytes

---

## Travail demande

### Partie 1 : Ingestion et nettoyage avec Spark (4-5h)

**Competence evaluee : C2.1 - Collecter des donnees en respectant les normes et standards**

#### Etape 1.1 : Exploration initiale
- Charger les donnees de consommation avec PySpark
- Analyser le schema infere et identifier les problemes de typage
- Calculer les statistiques descriptives par type d'energie
- Identifier les batiments avec le plus de mesures
- Produire un rapport d'audit de qualite des donnees

**Livrables** :
- Notebook `01_exploration_spark.ipynb`
- Rapport d'audit (format markdown ou DataFrame)

#### Etape 1.2 : Pipeline de nettoyage Spark

**Competence evaluee : C2.2 - Traiter des donnees structurees avec un langage de programmation**

- Creer des UDF pour parser les timestamps multi-formats
- Convertir les valeurs avec virgule en float
- Filtrer les valeurs negatives et les outliers (>10000)
- Dedupliquer sur (batiment_id, timestamp, type_energie)
- Calculer les agregations :
  - Consommations horaires moyennes par batiment
  - Consommations journalieres par batiment et type d'energie
  - Consommations mensuelles par commune
- Sauvegarder en Parquet partitionne par date et type d'energie

**Livrables** :
- Script `02_nettoyage_spark.py` (executable en ligne de commande)
- Fichiers Parquet dans `output/consommations_clean/`
- Log de traitement (lignes en entree/sortie, lignes supprimees)

#### Etape 1.3 : Agregations avancees Spark
- Joindre consommations avec referentiel batiments
- Calculer l'intensite energetique (kWh/m2)
- Identifier les batiments hors norme (>3x la mediane de leur categorie)
- Calculer les totaux par commune et par type de batiment
- Creer une vue SQL exploitable

**Livrables** :
- Notebook `03_agregations_spark.ipynb`
- Table agregee `consommations_agregees.parquet`
- Requetes Spark SQL demonstrant l'utilisation de la vue

---

### Partie 2 : Nettoyage avance avec Pandas (3-4h)

**Competence evaluee : C2.2 - Traiter des donnees structurees avec un langage de programmation**

#### Etape 2.1 : Nettoyage des donnees meteo
- Charger `meteo_raw.csv` avec Pandas
- Standardiser les formats de dates
- Convertir les colonnes numeriques en gerant les erreurs
- Corriger les valeurs aberrantes :
  - Temperatures hors [-40, 50] -> NaN puis interpolation
  - Humidite hors [0, 100] -> clipping
  - Rayonnement solaire negatif -> 0
- Traiter les valeurs manquantes :
  - Interpolation lineaire pour temperature et humidite
  - Forward fill pour precipitation
- Ajouter des colonnes temporelles (jour, mois, saison, jour de semaine)

**Livrables** :
- Notebook `04_nettoyage_meteo_pandas.ipynb`
- Dataset nettoye `output/meteo_clean.csv`
- Rapport avant/apres nettoyage (completude par colonne)

#### Etape 2.2 : Fusion et enrichissement
- Charger les consommations nettoyees (depuis Parquet)
- Fusionner avec les donnees meteo (sur commune et timestamp arrondi a l'heure)
- Fusionner avec le referentiel batiments
- Fusionner avec les tarifs pour calculer le cout financier
- Creer des features derivees :
  - Consommation par occupant
  - Consommation par m2
  - Cout journalier, mensuel, annuel
  - Indice de performance energetique (IPE)
  - Ecart a la moyenne de la categorie

**Livrables** :
- Notebook `05_fusion_enrichissement.ipynb`
- Dataset final `output/consommations_enrichies.csv` et `.parquet`
- Dictionnaire de donnees (description de toutes les colonnes)

---

### Partie 3 : Analyse exploratoire (2-3h)

**Competence evaluee : C2.3 - Analyser des donnees structurees pour repondre a un besoin metier**

#### Etape 3.1 : Statistiques descriptives
- Calculer les statistiques par type d'energie, type de batiment et commune
- Identifier les batiments les plus/moins energivores
- Calculer la repartition des consommations par classe energetique DPE
- Analyser l'evolution temporelle (tendances mensuelles, saisonnalite)
- Comparer la consommation theorique (selon DPE) vs reelle

**Livrables** :
- Notebook `06_statistiques_descriptives.ipynb`
- Tableaux de synthese exportes en CSV

#### Etape 3.2 : Analyse des correlations
- Calculer la matrice de correlation entre :
  - Consommations (electricite, gaz, eau)
  - Variables meteo (temperature, humidite, rayonnement, vent)
  - Caracteristiques batiments (surface, nb occupants, annee construction)
- Identifier les correlations significatives (>0.5 ou <-0.5)
- Analyser l'impact de la temperature sur la consommation de chauffage
- Etudier l'effet du rayonnement solaire sur la consommation electrique

**Livrables** :
- Notebook `07_analyse_correlations.ipynb`
- Matrice de correlation exportee `output/matrice_correlation.csv`
- Synthese des insights (format markdown)

#### Etape 3.3 : Detection d'anomalies
- Identifier les pics de consommation anormaux (>3 ecarts-types)
- Detecter les periodes de sous-consommation suspectes (batiment ferme non declare)
- Reperer les batiments dont la consommation ne correspond pas a leur DPE
- Lister les batiments necessitant un audit energetique

**Livrables** :
- Notebook `08_detection_anomalies.ipynb`
- Liste des anomalies `output/anomalies_detectees.csv`
- Rapport de recommandations d'audit

---

### Partie 4 : Visualisation (3-4h)

**Competence evaluee : C2.4 - Presenter des donnees analysees de facon intelligible**

#### Etape 4.1 : Graphiques Matplotlib
Produire les visualisations suivantes avec Matplotlib :

1. Evolution temporelle de la consommation totale par type d'energie (line plot)
2. Distribution des consommations par type de batiment (boxplot)
3. Heatmap consommation moyenne par heure et jour de semaine
4. Scatter plot temperature vs consommation de chauffage avec regression
5. Comparaison consommation par classe energetique (bar chart)

Chaque graphique doit inclure : titre explicite, labels des axes avec unites, legende, annotations pertinentes.

**Livrables** :
- Notebook `09_visualisations_matplotlib.ipynb`
- 5 figures PNG (300 dpi) dans `output/figures/`

#### Etape 4.2 : Visualisations Seaborn
Produire les visualisations suivantes avec Seaborn :

1. Pairplot des consommations (electricite, gaz, eau) par saison
2. Violin plot de la consommation electrique par type de batiment
3. Heatmap annotee de la matrice de correlation complete
4. FacetGrid : evolution mensuelle par commune (top 6 communes)
5. Jointplot : relation surface vs consommation avec distributions marginales
6. Catplot : consommation par classe energetique et type de batiment

**Livrables** :
- Notebook `10_visualisations_seaborn.ipynb`
- 6 figures PNG (300 dpi) dans `output/figures/`

#### Etape 4.3 : Dashboard executif
Creer un dashboard multi-panneaux (2x3) synthetisant les resultats :

- Panel 1 : Evolution de la consommation totale (6 derniers mois)
- Panel 2 : Top 10 batiments les plus energivores (bar horizontal)
- Panel 3 : Repartition des couts par type d'energie (pie chart)
- Panel 4 : Consommation moyenne par classe DPE avec ecart-type
- Panel 5 : Carte de chaleur par commune
- Panel 6 : Economies potentielles par amelioration du DPE

**Livrables** :
- Notebook `11_dashboard_executif.ipynb`
- Figure dashboard `output/figures/dashboard_energie.png`

---

### Partie 5 : Synthese et recommandations (1-2h)

**Competence evaluee : C2.4 - Presenter des donnees analysees de facon intelligible**

#### Rapport de conclusions
Rediger un rapport de synthese (format markdown) incluant :

1. Resume executif (5-10 lignes)
2. Metriques cles :
   - Consommation totale et cout annuel
   - Consommation moyenne par m2 et par type
   - Nombre de batiments par classe energetique
   - Potentiel d'economies identifie
3. Insights principaux :
   - Batiments prioritaires pour renovation
   - Impact de la meteo sur la consommation
   - Anomalies detectees et actions correctives
   - Comparaison inter-communes
4. Recommandations concretes :
   - Actions immediates (correction anomalies)
   - Actions court terme (optimisation usage)
   - Actions long terme (renovation energetique)
   - Batiments a auditer en priorite

**Livrables** :
- Fichier `output/rapport_synthese.md`
- Presentation des conclusions (5 slides maximum)

---

## Livrables finaux attendus

```
ecf_energie/
├── README.md                                # Instructions d'execution
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
│   ├── consommations_clean/               # Parquet partitionne
│   ├── consommations_agregees.parquet
│   ├── meteo_clean.csv
│   ├── consommations_enrichies.csv
│   ├── consommations_enrichies.parquet
│   ├── matrice_correlation.csv
│   ├── anomalies_detectees.csv
│   ├── figures/                           # Tous les graphiques
│   └── rapport_synthese.md
└── requirements.txt
```

---

## Criteres d'evaluation par competence RNCP

| Competence RNCP | Criteres d'evaluation | Points |
|-----------------|----------------------|--------|
| **C2.1 - Collecter des donnees** | Chargement correct des sources, identification exhaustive des problemes de qualite, documentation de l'audit | 20 |
| **C2.2 - Traiter des donnees** | Pipeline Spark fonctionnel et optimise, nettoyage Pandas pertinent, gestion des erreurs, code reutilisable (UDF, fonctions), fusion correcte des sources | 30 |
| **C2.3 - Analyser des donnees** | KPI pertinents calcules, correlations identifiees et interpretees, anomalies detectees avec actions proposees, comparaisons pertinentes | 25 |
| **C2.4 - Presenter des donnees** | Visualisations Matplotlib professionnelles (5), visualisations Seaborn avancees (6), dashboard synthetique et lisible, rapport actionnable avec recommandations claires | 25 |
| **Total** | | **100** |

### Grille detaillee

**C2.1 - Collecter (20 points)**
- Chargement PySpark sans erreur : 7 pts
- Identification complete des problemes : 7 pts
- Rapport d'audit structure : 6 pts

**C2.2 - Traiter (30 points)**
- UDF Spark correctes : 8 pts
- Pipeline nettoyage Spark complet : 10 pts
- Nettoyage Pandas meteo : 7 pts
- Fusion et enrichissement : 5 pts

**C2.3 - Analyser (25 points)**
- Statistiques descriptives : 8 pts
- Analyse correlations : 8 pts
- Detection anomalies : 9 pts

**C2.4 - Presenter (25 points)**
- Matplotlib (5 graphiques) : 9 pts
- Seaborn (6 graphiques) : 9 pts
- Dashboard executif : 4 pts
- Rapport conclusions : 3 pts

---

## Consignes

### Duree
- **1 a 2 jours** (8 a 16 heures de travail effectif)
- Gestion autonome du temps entre les parties

### Environnement technique
- Python 3.9+
- PySpark 3.5+
- Pandas 2.0+
- Matplotlib 3.8+
- Seaborn 0.13+
- Jupyter Notebook ou JupyterLab

### Ressources autorisees
- Documentation officielle (PySpark, Pandas, Matplotlib, Seaborn)
- Stack Overflow et forums techniques
- Supports de cours Jedha
- TP realises precedemment

### Ressources interdites
- Communication avec d'autres candidats
- Utilisation de solutions completes trouvees en ligne
- Intelligence artificielle generative (ChatGPT, etc.)

### Restitution
- Depot de l'ensemble du dossier `ecf_energie/` compresse
- Tous les notebooks doivent etre executes (avec les cellules de sortie)
- Le fichier README.md doit expliquer comment executer le pipeline complet
- Deadline : [A definir par le formateur]

---

## Correspondance avec le referentiel RNCP35288

### Bloc de competences : Collecter, stocker et mettre a disposition des donnees

**C2.1 - Collecter des donnees en respectant les normes et standards de l'entreprise**
- Partie 1.1 : Exploration initiale et audit qualite
- Identification systematique des problemes de donnees sources
- Documentation structuree des anomalies detectees

**C2.2 - Traiter des donnees structurees avec un langage de programmation**
- Partie 1.2 : Pipeline Spark avec UDF personnalisees
- Partie 1.3 : Agregations complexes Spark SQL
- Partie 2 : Nettoyage avance Pandas (meteo + fusion)
- Gestion des erreurs, cas limites, code reutilisable

**C2.3 - Analyser des donnees structurees pour repondre a un besoin metier**
- Partie 3 : Analyse exploratoire complete
- Calcul de KPI energetiques pertinents
- Identification de correlations meteo/consommation
- Detection d'anomalies avec recommandations actionnables

**C2.4 - Presenter des donnees analysees de facon intelligible**
- Partie 4 : 11 visualisations professionnelles (Matplotlib + Seaborn)
- Partie 5 : Dashboard executif synthetique
- Rapport de synthese avec recommandations claires

### Criteres de reussite RNCP

Pour valider chaque competence, le candidat doit obtenir au minimum 50% des points attribues a cette competence :

- C2.1 : minimum 10/20 points
- C2.2 : minimum 15/30 points
- C2.3 : minimum 13/25 points
- C2.4 : minimum 13/25 points

**Validation globale** : minimum 60/100 points ET validation de toutes les competences

---

## Conseils pour reussir

1. **Gestion du temps** : Repartissez bien les 8-16h sur les 5 parties
2. **Incremental** : Testez chaque etape avant de passer a la suivante
3. **Documentation** : Commentez votre code et vos choix
4. **Validation** : Verifiez la coherence des resultats a chaque etape
5. **Professionnalisme** : Soignez la presentation (notebooks, graphiques, rapport)
6. **Priorisation** : Si manque de temps, privilegiez la qualite sur l'exhaustivite

---

*Bon courage pour cet ECF*
