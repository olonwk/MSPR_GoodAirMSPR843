#!/usr/bin/env python3
# =============================================================================
# spark/transform_spark.py — Transformation distribuée des données brutes
# =============================================================================
#
# Remplace la tâche clean_and_transform() du DAG Airflow en utilisant PySpark
# pour un traitement parallèle des enregistrements JSON issus des APIs
# AQICN (qualité de l'air) et OWM (météo OpenWeatherMap).
#
# Utilisation :
#   spark-submit --master spark://spark-master:7077 transform_spark.py [data_dir]
#   spark-submit --master local[*]                  transform_spark.py [data_dir]
#
# Fonctionnement :
#   1. Lecture des fichiers JSON bruts dans data/raw/
#   2. Distribution des enregistrements sur le cluster via sc.parallelize()
#   3. Transformation parallèle sur les workers Spark (map + filter)
#   4. Collecte des résultats sur le driver et écriture dans data/cleaned/
#   5. Archivage des fichiers bruts dans data/raw_archive/
#
# Format de sortie : compatible avec load_to_postgres() du DAG.
# =============================================================================

import sys
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession


# === Configuration des chemins ================================================

BASE_DIR        = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/opt/airflow/data")
RAW_DIR         = BASE_DIR / "raw"
CLEANED_DIR     = BASE_DIR / "cleaned"
RAW_ARCHIVE_DIR = BASE_DIR / "raw_archive"


# === Fonctions de transformation (exécutées sur les workers Spark) ============
# Ces fonctions doivent être sérialisables (pas de closures complexes).

def transform_aqicn_entry(entry: dict, date_extraction: str):
    """
    Transforme un enregistrement brut AQICN.
    Retourne None si :
      - l'API a renvoyé une erreur (status != "ok")
      - le timestamp de la mesure est antérieur à l'année d'extraction
    Le résultat est directement compatible avec load_to_postgres() :
      - ville_cible, source_api, date_extraction ajoutés
      - data.data aplati au niveau racine (idx, city, aqi, iaqi, time…)
    """
    if entry.get("data", {}).get("status") != "ok":
        return None
    result = {
        "ville_cible":    entry["ville"],
        "source_api":     "aqicn",
        "date_extraction": date_extraction,
    }
    result.update(entry["data"].get("data", {}))

    time_str = result.get("time", {}).get("iso") or result.get("time", {}).get("s")
    if time_str:
        try:
            mesure_year = datetime.fromisoformat(time_str).year
            extraction_year = datetime.fromisoformat(date_extraction).year
            if mesure_year < extraction_year:
                return None
        except (ValueError, TypeError):
            return None

    return result


def transform_owm_entry(entry: dict, date_extraction: str):
    """
    Transforme un enregistrement brut OWM.
    Retourne None si :
      - l'API a renvoyé une erreur (cod != 200)
      - le timestamp de la mesure est antérieur à l'année d'extraction
    Le résultat est directement compatible avec load_to_postgres() :
      - ville_cible, source_api, date_extraction ajoutés
      - data aplati au niveau racine (coord, weather, main, wind, sys…)
    """
    if entry.get("data", {}).get("cod") != 200:
        return None
    result = {
        "ville_cible":    entry["ville"],
        "source_api":     "owm",
        "date_extraction": date_extraction,
    }
    result.update(entry["data"])

    dt = result.get("dt")
    if dt:
        try:
            mesure_year = datetime.fromtimestamp(int(dt), tz=timezone.utc).year
            extraction_year = datetime.fromisoformat(date_extraction).year
            if mesure_year < extraction_year:
                return None
        except (ValueError, TypeError, OSError):
            return None

    return result


# === Pipeline Spark ============================================================

def process_file(sc, file_path: Path, source: str, output_dir: Path) -> int:
    """
    Traite un fichier JSON brut avec Spark RDD :
      - Lit le fichier sur le driver
      - Distribue les enregistrements sur le cluster (sc.parallelize)
      - Applique la transformation en parallèle sur les workers (rdd.map + filter)
      - Collecte les résultats sur le driver et écrit le fichier nettoyé

    Retourne le nombre d'entrées transformées avec succès.
    """
    date_extraction = datetime.now(timezone.utc).isoformat()

    with open(file_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

    # Distribuer les enregistrements : chaque worker traite un sous-ensemble
    num_slices = min(len(raw_data), 8)
    rdd = sc.parallelize(raw_data, numSlices=num_slices)

    if source == "aqicn":
        results = (
            rdd
            .map(lambda entry: transform_aqicn_entry(entry, date_extraction))
            .filter(lambda x: x is not None)
            .collect()
        )
    else:  # owm
        results = (
            rdd
            .map(lambda entry: transform_owm_entry(entry, date_extraction))
            .filter(lambda x: x is not None)
            .collect()
        )

    output_path = output_dir / f"full_cleaned_{file_path.name}"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"[{source.upper()}] {file_path.name} → {len(results)} entrées → {output_path.name}")
    return len(results)


def main():
    CLEANED_DIR.mkdir(parents=True, exist_ok=True)
    RAW_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = sorted(RAW_DIR.glob("*.json"))
    if not raw_files:
        print("Aucun fichier JSON à traiter dans data/raw/")
        sys.exit(0)

    spark = (
        SparkSession.builder
        .appName("GoodAir-Transform")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    print(f"Spark UI  : {sc.uiWebUrl}")
    print(f"Master    : {sc.master}")
    print(f"Fichiers  : {len(raw_files)} à traiter dans {RAW_DIR}")

    total = 0
    for file in raw_files:
        try:
            if "aqicn" in file.name:
                source = "aqicn"
            elif "owm" in file.name:
                source = "owm"
            else:
                print(f"[SKIP] {file.name} — source non reconnue")
                continue

            count = process_file(sc, file, source, CLEANED_DIR)
            shutil.move(str(file), str(RAW_ARCHIVE_DIR / file.name))
            total += count

        except Exception as e:
            import traceback
            print(f"[ERROR] {file.name} : {e}")
            traceback.print_exc()

    spark.stop()
    print(f"\nTransformation Spark terminée : {total} entrées traitées.")


if __name__ == "__main__":
    main()
