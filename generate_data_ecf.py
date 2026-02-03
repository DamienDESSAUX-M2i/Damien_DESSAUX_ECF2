#!/usr/bin/env python3
"""
Script de generation des donnees pour l'ECF Energie.
Genere des donnees realistes de consommation energetique avec defauts intentionnels.
"""

import random
import csv
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Configuration
OUTPUT_DIR = Path(__file__).parent / "data_ecf"
SEED = 42
random.seed(SEED)

# Types de batiments
TYPES_BATIMENTS = {
    'ecole': {'surface_min': 800, 'surface_max': 3000, 'occupants_min': 100, 'occupants_max': 500},
    'mairie': {'surface_min': 300, 'surface_max': 1500, 'occupants_min': 20, 'occupants_max': 100},
    'gymnase': {'surface_min': 1000, 'surface_max': 2500, 'occupants_min': 50, 'occupants_max': 300},
    'piscine': {'surface_min': 1500, 'surface_max': 4000, 'occupants_min': 100, 'occupants_max': 400},
    'mediatheque': {'surface_min': 500, 'surface_max': 2000, 'occupants_min': 30, 'occupants_max': 150}
}

COMMUNES = [
    'Paris', 'Lyon', 'Marseille', 'Toulouse', 'Bordeaux',
    'Lille', 'Nantes', 'Strasbourg', 'Montpellier', 'Nice',
    'Rennes', 'Reims', 'Le Havre', 'Saint-Etienne', 'Toulon'
]

CLASSES_ENERGETIQUES = ['A', 'B', 'C', 'D', 'E', 'F', 'G']

# Formats de dates pour introduire des incoherences
DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",      # ISO
    "%d/%m/%Y %H:%M",         # FR
    "%m/%d/%Y %H:%M:%S",      # US
    "%Y-%m-%dT%H:%M:%S",      # ISO avec T
]


def generate_batiments():
    """Genere le fichier batiments.csv (propre)."""
    batiments = []
    batiment_id = 1

    for commune in COMMUNES:
        # 8 a 12 batiments par commune
        num_batiments = random.randint(8, 12)

        for _ in range(num_batiments):
            type_bat = random.choice(list(TYPES_BATIMENTS.keys()))
            config = TYPES_BATIMENTS[type_bat]

            surface = random.randint(config['surface_min'], config['surface_max'])
            nb_occupants = random.randint(config['occupants_min'], config['occupants_max'])
            annee = random.randint(1950, 2020)

            # DPE : les batiments recents ont de meilleures classes
            if annee >= 2010:
                classe = random.choices(CLASSES_ENERGETIQUES[:4], weights=[10, 20, 30, 40])[0]
            elif annee >= 1990:
                classe = random.choices(CLASSES_ENERGETIQUES[2:6], weights=[15, 25, 35, 25])[0]
            else:
                classe = random.choices(CLASSES_ENERGETIQUES[4:], weights=[20, 40, 40])[0]

            batiments.append({
                'batiment_id': f'BAT{batiment_id:04d}',
                'nom': f'{type_bat.capitalize()} {commune} {batiment_id}',
                'type': type_bat,
                'commune': commune,
                'surface_m2': surface,
                'annee_construction': annee,
                'classe_energetique': classe,
                'nb_occupants_moyen': nb_occupants
            })
            batiment_id += 1

    output_path = OUTPUT_DIR / "batiments.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'batiment_id', 'nom', 'type', 'commune', 'surface_m2',
            'annee_construction', 'classe_energetique', 'nb_occupants_moyen'
        ])
        writer.writeheader()
        writer.writerows(batiments)

    print(f"Generated {len(batiments)} batiments -> {output_path}")
    return batiments


def generate_consommations(batiments, start_date, end_date):
    """Genere le fichier consommations_raw.csv avec defauts intentionnels."""
    records = []

    # Consommation de reference par type
    CONSO_BASE = {
        'ecole': {'electricite': 80, 'gaz': 120, 'eau': 5},
        'mairie': {'electricite': 60, 'gaz': 90, 'eau': 3},
        'gymnase': {'electricite': 100, 'gaz': 150, 'eau': 8},
        'piscine': {'electricite': 200, 'gaz': 300, 'eau': 50},
        'mediatheque': {'electricite': 70, 'gaz': 100, 'eau': 4}
    }

    current_date = start_date
    while current_date < end_date:
        for bat in batiments:
            # Skip aleatoire (pannes de capteurs)
            if random.random() < 0.01:
                continue

            type_bat = bat['type']
            surface = bat['surface_m2']
            classe = bat['classe_energetique']

            # Facteur DPE (G consomme 3x plus que A)
            facteur_dpe = {'A': 0.5, 'B': 0.7, 'C': 0.9, 'D': 1.0, 'E': 1.3, 'F': 1.7, 'G': 2.0}[classe]

            for type_energie in ['electricite', 'gaz', 'eau']:
                # Conso de base proportionnelle a la surface
                base = CONSO_BASE[type_bat][type_energie] * (surface / 1000) * facteur_dpe

                # Variation saisonniere (plus de chauffage en hiver)
                month = current_date.month
                if type_energie in ['electricite', 'gaz']:
                    if month in [11, 12, 1, 2]:
                        base *= 1.6
                    elif month in [6, 7, 8]:
                        base *= 0.7

                # Variation horaire (ferme la nuit)
                hour = current_date.hour
                if hour < 6 or hour > 22:
                    base *= 0.1
                elif hour in [8, 9, 10, 14, 15, 16]:
                    base *= 1.3

                # Variation jour de semaine (ferme week-end pour certains)
                if current_date.weekday() >= 5 and type_bat in ['ecole', 'mairie']:
                    base *= 0.2

                # Bruit aleatoire
                value = base * random.uniform(0.7, 1.3)

                # Introduction de defauts intentionnels
                defect_type = random.random()

                if defect_type < 0.005:
                    value = -abs(value)  # Valeur negative
                elif defect_type < 0.01:
                    value = random.uniform(15000, 50000)  # Valeur aberrante
                elif defect_type < 0.015:
                    value = random.choice(["erreur", "N/A", "---", "null"])  # Valeur textuelle

                # Format de valeur (parfois virgule)
                if isinstance(value, float):
                    if random.random() < 0.12:
                        value_str = f"{value:.2f}".replace(".", ",")
                    else:
                        value_str = f"{value:.2f}"
                else:
                    value_str = str(value)

                # Format de date aleatoire
                date_format = random.choice(DATE_FORMATS)
                timestamp_str = current_date.strftime(date_format)

                # Unite
                unite = 'kWh' if type_energie in ['electricite', 'gaz'] else 'm3'

                records.append({
                    'batiment_id': bat['batiment_id'],
                    'timestamp': timestamp_str,
                    'type_energie': type_energie,
                    'consommation': value_str,
                    'unite': unite
                })

        # Avancer d'une heure
        current_date += timedelta(hours=1)

    # Ajouter des doublons (2%)
    num_duplicates = int(len(records) * 0.02)
    duplicates = random.sample(records, num_duplicates)
    records.extend(duplicates)
    random.shuffle(records)

    output_path = OUTPUT_DIR / "consommations_raw.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['batiment_id', 'timestamp', 'type_energie', 'consommation', 'unite'])
        writer.writeheader()
        writer.writerows(records)

    print(f"Generated {len(records)} consommations records -> {output_path}")


def generate_meteo(communes, start_date, end_date):
    """Genere le fichier meteo_raw.csv avec defauts intentionnels."""
    records = []

    current_date = start_date
    while current_date < end_date:
        for commune in communes:
            # Skip aleatoire (pannes stations meteo)
            if random.random() < 0.02:
                continue

            # Temperature de base selon saison
            month = current_date.month
            if month in [12, 1, 2]:
                temp_base = 5
            elif month in [3, 4, 5]:
                temp_base = 15
            elif month in [6, 7, 8]:
                temp_base = 25
            else:
                temp_base = 12

            # Variation geographique (simplifie)
            if commune in ['Marseille', 'Nice', 'Toulon', 'Montpellier']:
                temp_base += 4
            elif commune in ['Lille', 'Strasbourg', 'Reims']:
                temp_base -= 2

            temperature = temp_base + random.uniform(-7, 7)
            humidite = random.uniform(30, 95)
            rayonnement = max(0, random.uniform(0, 800) if 6 <= current_date.hour <= 20 else random.uniform(0, 50))
            vent = random.uniform(0, 40)
            precipitation = random.uniform(0, 15) if random.random() < 0.25 else 0

            # Introduction de defauts
            defect_type = random.random()

            if defect_type < 0.008:
                temperature = random.choice([-70, 80, 100])  # Temperature impossible
            elif defect_type < 0.015:
                humidite = random.uniform(105, 150)  # Humidite >100
            elif defect_type < 0.02:
                temperature = random.choice(["", "NA", "null"])  # Valeur manquante
            elif defect_type < 0.025:
                rayonnement = -random.uniform(10, 100)  # Rayonnement negatif

            # Format temperature (virgule parfois)
            if isinstance(temperature, (int, float)):
                if random.random() < 0.08:
                    temp_str = f"{temperature:.1f}".replace(".", ",")
                else:
                    temp_str = f"{temperature:.1f}"
            else:
                temp_str = str(temperature)

            # Format humidite
            humidity_str = f"{humidite:.1f}" if isinstance(humidite, (int, float)) else str(humidite)

            # Format de date aleatoire
            date_format = random.choice(DATE_FORMATS)
            timestamp_str = current_date.strftime(date_format)

            records.append({
                'commune': commune,
                'timestamp': timestamp_str,
                'temperature_c': temp_str,
                'humidite_pct': humidity_str,
                'rayonnement_solaire_wm2': f"{rayonnement:.1f}",
                'vitesse_vent_kmh': f"{vent:.1f}",
                'precipitation_mm': f"{precipitation:.1f}"
            })

        # Avancer d'une heure
        current_date += timedelta(hours=1)

    # Simuler blocs de donnees manquantes (panne station) - simplification sans parsing de date
    records_filtered = []
    skip_count = int(len(records) * 0.02)  # Supprimer 2% aleatoirement
    indices_to_skip = set(random.sample(range(len(records)), skip_count))

    for i, record in enumerate(records):
        if i not in indices_to_skip:
            records_filtered.append(record)

    random.shuffle(records_filtered)

    output_path = OUTPUT_DIR / "meteo_raw.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'commune', 'timestamp', 'temperature_c', 'humidite_pct',
            'rayonnement_solaire_wm2', 'vitesse_vent_kmh', 'precipitation_mm'
        ])
        writer.writeheader()
        writer.writerows(records_filtered)

    print(f"Generated {len(records_filtered)} meteo records -> {output_path}")


def generate_tarifs():
    """Genere le fichier tarifs_energie.csv (propre)."""
    tarifs = [
        # Annee 2023
        {'date_debut': '2023-01-01', 'date_fin': '2023-06-30', 'type_energie': 'electricite', 'tarif_unitaire': 0.18},
        {'date_debut': '2023-07-01', 'date_fin': '2023-12-31', 'type_energie': 'electricite', 'tarif_unitaire': 0.20},
        {'date_debut': '2023-01-01', 'date_fin': '2023-06-30', 'type_energie': 'gaz', 'tarif_unitaire': 0.09},
        {'date_debut': '2023-07-01', 'date_fin': '2023-12-31', 'type_energie': 'gaz', 'tarif_unitaire': 0.10},
        {'date_debut': '2023-01-01', 'date_fin': '2023-12-31', 'type_energie': 'eau', 'tarif_unitaire': 3.50},

        # Annee 2024
        {'date_debut': '2024-01-01', 'date_fin': '2024-06-30', 'type_energie': 'electricite', 'tarif_unitaire': 0.21},
        {'date_debut': '2024-07-01', 'date_fin': '2024-12-31', 'type_energie': 'electricite', 'tarif_unitaire': 0.22},
        {'date_debut': '2024-01-01', 'date_fin': '2024-06-30', 'type_energie': 'gaz', 'tarif_unitaire': 0.11},
        {'date_debut': '2024-07-01', 'date_fin': '2024-12-31', 'type_energie': 'gaz', 'tarif_unitaire': 0.12},
        {'date_debut': '2024-01-01', 'date_fin': '2024-12-31', 'type_energie': 'eau', 'tarif_unitaire': 3.75},
    ]

    output_path = OUTPUT_DIR / "tarifs_energie.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['date_debut', 'date_fin', 'type_energie', 'tarif_unitaire'])
        writer.writeheader()
        writer.writerows(tarifs)

    print(f"Generated {len(tarifs)} tarifs -> {output_path}")


def main():
    """Point d'entree principal."""
    print("=" * 60)
    print("GENERATION DES DONNEES POUR L'ECF ENERGIE")
    print("=" * 60)

    # Creer le dossier de sortie
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Periode: 2 ans de donnees (2023-2024)
    start_date = datetime(2023, 1, 1, 0, 0, 0)
    end_date = datetime(2025, 1, 1, 0, 0, 0)

    print(f"Periode: {start_date.date()} -> {end_date.date()}")
    print()

    # Generer les batiments
    batiments = generate_batiments()
    print()

    # Generer les consommations
    generate_consommations(batiments, start_date, end_date)
    print()

    # Generer les donnees meteo
    generate_meteo(COMMUNES, start_date, end_date)
    print()

    # Generer les tarifs
    generate_tarifs()
    print()

    print("=" * 60)
    print("GENERATION TERMINEE!")
    print(f"Fichiers generes dans: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
