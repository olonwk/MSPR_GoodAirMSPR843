from airflow import DAG  # pyright: ignore[reportMissingImports]
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.operators.bash import BashOperator  # pyright: ignore[reportMissingImports]
from datetime import datetime, timezone
import json
import os
import httpx
from pathlib import Path

# --- CONFIGURATION ---
AQICN_KEY = os.getenv("AQICN_KEY")
OWM_KEY   = os.getenv("OWM_KEY")

# Chemins dans le container Docker
BASE_DATA_DIR   = Path("/opt/airflow/data")
RAW_DIR         = BASE_DATA_DIR / "raw"
RAW_ARCHIVE_DIR = BASE_DATA_DIR / "raw_archive"
CLEANED_DIR     = BASE_DATA_DIR / "cleaned"
LOADED_DIR      = BASE_DATA_DIR / "loaded"

# MinIO — Data Lake (désactivé)
# MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio:9000")
# MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
# MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")
# MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "goodair-raw")

VILLES = [
    # Grandes métropoles
    "Paris", "Marseille", "Lyon", "Toulouse", "Nice",
    "Nantes", "Montpellier", "Strasbourg", "Bordeaux", "Lille",
    "Rennes", "Reims", "Saint-Etienne", "Toulon", "Le Havre",
    "Grenoble", "Dijon", "Angers", "Nimes", "Clermont-Ferrand",
    # Villes moyennes
    "Perpignan", "Metz", "Besancon", "Orleans", "Rouen",
    "Mulhouse", "Caen", "Nancy", "Avignon", "Valence",
    "Cannes", "Antibes", "La Rochelle", "Pau", "Calais",
    "Brest", "Limoges", "Tours", "Amiens", "Poitiers",
    # Autres préfectures / villes notables
    "Ajaccio", "Bayonne", "Colmar", "Troyes", "Chambery",
    "Saint-Malo", "Lorient", "Vannes", "Quimper", "Dunkerque",
    "Boulogne-sur-Mer", "Charleville-Mezieres", "Laval", "Tarbes",
    "Agen", "Albi", "Rodez", "Aurillac", "Le Mans", "Cholet",
    # Villes de l'Hérault
    "Beziers", "Sete", "Agde", "Lunel", "Frontignan",
    "Meze", "Lodeve", "Pezenas", "Clermont-l'Herault"
]


# ================================================================
# Helpers MinIO (désactivé)
# ================================================================
# def _get_minio_client():
#     import boto3
#     from botocore.client import Config
#     return boto3.client(
#         "s3",
#         endpoint_url=f"http://{MINIO_ENDPOINT}",
#         aws_access_key_id=MINIO_ACCESS_KEY,
#         aws_secret_access_key=MINIO_SECRET_KEY,
#         config=Config(signature_version="s3v4"),
#         region_name="us-east-1",
#     )
#
# def _upload_to_minio(local_path: Path, object_key: str):
#     try:
#         client = _get_minio_client()
#         try:
#             client.head_bucket(Bucket=MINIO_BUCKET)
#         except Exception:
#             client.create_bucket(Bucket=MINIO_BUCKET)
#         client.upload_file(str(local_path), MINIO_BUCKET, object_key)
#         print(f"MinIO : {object_key} uploadé avec succès")
#     except Exception as e:
#         print(f"MinIO upload ignoré ({object_key}) : {e}")


# ================================================================
# TÂCHE 1 : EXTRACT — Collecte des données brutes depuis les APIs
# ================================================================
def fetch_api_data():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Collecte AQICN
    results_aqi = []
    for city in VILLES:
        try:
            r = httpx.get(f"https://api.waqi.info/feed/{city}/?token={AQICN_KEY}", timeout=10)
            results_aqi.append({"ville": city, "data": r.json()})
        except Exception as e:
            print(f"Erreur AQI {city}: {e}")

    aqicn_file = RAW_DIR / f"aqicn_{timestamp}.json"
    with open(aqicn_file, "w") as f:
        json.dump(results_aqi, f)
    # _upload_to_minio(aqicn_file, f"aqicn/aqicn_{timestamp}.json")

    # Collecte OWM
    results_owm = []
    for city in VILLES:
        try:
            r = httpx.get(
                f"https://api.openweathermap.org/data/2.5/weather?q={city},FR&appid={OWM_KEY}&units=metric",
                timeout=10
            )
            results_owm.append({"ville": city, "data": r.json()})
        except Exception as e:
            print(f"Erreur OWM {city}: {e}")

    owm_file = RAW_DIR / f"owm_{timestamp}.json"
    with open(owm_file, "w") as f:
        json.dump(results_owm, f)
    # _upload_to_minio(owm_file, f"owm/owm_{timestamp}.json")

    return f"Collecte terminée : {timestamp}"


# ================================================================
# TÂCHE 3 : LOAD — Chargement dans les tables de staging PostgreSQL
# ================================================================
def load_to_postgres():
    import psycopg2

    LOADED_DIR.mkdir(parents=True, exist_ok=True)
    db_conn = os.getenv("GOODAIR_DB_CONN", "postgresql://airflow:airflow@postgres/airflow")
    conn = psycopg2.connect(db_conn)
    cur  = conn.cursor()

    inserted_air     = 0
    inserted_weather = 0
    files_processed  = 0

    for file in sorted(CLEANED_DIR.glob("full_cleaned_*.json")):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)

            for entry in data:
                source = entry.get("source_api")

                if source == "aqicn":
                    iaqi      = entry.get("iaqi", {})
                    time_data = entry.get("time", {})
                    city_data = entry.get("city", {})
                    geo       = city_data.get("geo", [])

                    cur.execute("""
                        INSERT INTO goodair.air_quality (
                            ville, source_api, date_extraction,
                            station_idx, station_name,
                            aqi, dominant_pollutant, mesure_timestamp, timezone,
                            co, h, no2, o3, p, pm10, pm25, so2, t, w, lat, lon
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        entry.get("ville_cible"), "aqicn", entry.get("date_extraction"),
                        entry.get("idx"), city_data.get("name"),
                        entry.get("aqi"), entry.get("dominentpol"),
                        time_data.get("iso") or time_data.get("s"), time_data.get("tz"),
                        iaqi.get("co",   {}).get("v"), iaqi.get("h",    {}).get("v"),
                        iaqi.get("no2",  {}).get("v"), iaqi.get("o3",   {}).get("v"),
                        iaqi.get("p",    {}).get("v"), iaqi.get("pm10", {}).get("v"),
                        iaqi.get("pm25", {}).get("v"), iaqi.get("so2",  {}).get("v"),
                        iaqi.get("t",    {}).get("v"), iaqi.get("w",    {}).get("v"),
                        geo[0] if len(geo) > 0 else None,
                        geo[1] if len(geo) > 1 else None,
                    ))
                    inserted_air += 1

                elif source == "owm":
                    coord        = entry.get("coord", {})
                    weather_list = entry.get("weather", [{}])
                    weather_item = weather_list[0] if weather_list else {}
                    main         = entry.get("main", {})
                    wind         = entry.get("wind", {})
                    clouds       = entry.get("clouds", {})
                    sys_data     = entry.get("sys", {})
                    dt           = entry.get("dt")

                    cur.execute("""
                        INSERT INTO goodair.weather (
                            ville, source_api, date_extraction,
                            city_name, country, mesure_timestamp,
                            weather_main, weather_description,
                            temp, feels_like, temp_min, temp_max,
                            pressure, humidity, visibility,
                            wind_speed, wind_deg, clouds, lat, lon
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        entry.get("ville_cible"), "owm", entry.get("date_extraction"),
                        entry.get("name"), sys_data.get("country"),
                        datetime.fromtimestamp(dt, tz=timezone.utc) if dt else None,
                        weather_item.get("main"), weather_item.get("description"),
                        main.get("temp"), main.get("feels_like"),
                        main.get("temp_min"), main.get("temp_max"),
                        main.get("pressure"), main.get("humidity"),
                        entry.get("visibility"),
                        wind.get("speed"), wind.get("deg"), clouds.get("all"),
                        coord.get("lat"), coord.get("lon"),
                    ))
                    inserted_weather += 1

            conn.commit()
            file.rename(LOADED_DIR / file.name)
            files_processed += 1

        except Exception as e:
            conn.rollback()
            print(f"Erreur chargement {file.name}: {e}")

    cur.close()
    conn.close()
    return (
        f"Staging chargé : {files_processed} fichiers, "
        f"{inserted_air} mesures air, {inserted_weather} mesures météo"
    )


# ================================================================
# TÂCHE 4 : DWH — Alimentation du modèle en étoile (Data Warehouse)
# ================================================================
def alimenter_datawarehouse():
    import psycopg2

    db_conn = os.getenv("GOODAIR_DB_CONN", "postgresql://airflow:airflow@postgres/airflow")
    conn = psycopg2.connect(db_conn)
    cur  = conn.cursor()

    # 1. Remplissage de dim_ville
    cur.execute("""
        INSERT INTO goodair.dim_ville (nom, lat, lon)
        SELECT DISTINCT ville, lat, lon FROM goodair.air_quality
        WHERE ville IS NOT NULL
        ON CONFLICT (nom) DO NOTHING;

        INSERT INTO goodair.dim_ville (nom, lat, lon)
        SELECT DISTINCT ville, lat, lon FROM goodair.weather
        WHERE ville IS NOT NULL
        ON CONFLICT (nom) DO NOTHING;
    """)

    # 2. Remplissage de dim_temps (arrondi à l'heure)
    cur.execute("""
        INSERT INTO goodair.dim_temps (date_heure, annee, mois, jour, heure, jour_semaine)
        SELECT DISTINCT
            date_trunc('hour', mesure_timestamp::timestamp),
            EXTRACT(YEAR  FROM mesure_timestamp::timestamp)::int,
            EXTRACT(MONTH FROM mesure_timestamp::timestamp)::int,
            EXTRACT(DAY   FROM mesure_timestamp::timestamp)::int,
            EXTRACT(HOUR  FROM mesure_timestamp::timestamp)::int,
            EXTRACT(DOW   FROM mesure_timestamp::timestamp)::int
        FROM goodair.air_quality
        WHERE mesure_timestamp IS NOT NULL
        ON CONFLICT (date_heure) DO NOTHING;

        INSERT INTO goodair.dim_temps (date_heure, annee, mois, jour, heure, jour_semaine)
        SELECT DISTINCT
            date_trunc('hour', mesure_timestamp),
            EXTRACT(YEAR  FROM mesure_timestamp)::int,
            EXTRACT(MONTH FROM mesure_timestamp)::int,
            EXTRACT(DAY   FROM mesure_timestamp)::int,
            EXTRACT(HOUR  FROM mesure_timestamp)::int,
            EXTRACT(DOW   FROM mesure_timestamp)::int
        FROM goodair.weather
        WHERE mesure_timestamp IS NOT NULL
        ON CONFLICT (date_heure) DO NOTHING;
    """)

    # 3. Remplissage de fait_air_quality
    cur.execute("""
        INSERT INTO goodair.fait_air_quality
            (id_ville, id_temps, aqi, dominant_pollutant,
             co, h, no2, o3, p, pm10, pm25, so2, t, w)
        SELECT
            v.id, t.id,
            aq.aqi, aq.dominant_pollutant,
            aq.co, aq.h, aq.no2, aq.o3, aq.p,
            aq.pm10, aq.pm25, aq.so2, aq.t, aq.w
        FROM goodair.air_quality aq
        JOIN goodair.dim_ville v ON v.nom = aq.ville
        JOIN goodair.dim_temps t
          ON t.date_heure = date_trunc('hour', aq.mesure_timestamp::timestamp)
        WHERE aq.mesure_timestamp IS NOT NULL
        ON CONFLICT (id_ville, id_temps) DO NOTHING;
    """)

    # 4. Remplissage de fait_weather
    cur.execute("""
        INSERT INTO goodair.fait_weather
            (id_ville, id_temps, weather_main, weather_description,
             temp, feels_like, temp_min, temp_max,
             pressure, humidity, visibility,
             wind_speed, wind_deg, clouds)
        SELECT
            v.id, t.id,
            w.weather_main, w.weather_description,
            w.temp, w.feels_like, w.temp_min, w.temp_max,
            w.pressure, w.humidity, w.visibility,
            w.wind_speed, w.wind_deg, w.clouds
        FROM goodair.weather w
        JOIN goodair.dim_ville v ON v.nom = w.ville
        JOIN goodair.dim_temps t
          ON t.date_heure = date_trunc('hour', w.mesure_timestamp)
        WHERE w.mesure_timestamp IS NOT NULL
        ON CONFLICT (id_ville, id_temps) DO NOTHING;
    """)

    conn.commit()

    # Récupérer les compteurs pour le rapport
    cur.execute("SELECT COUNT(*) FROM goodair.dim_ville")
    nb_villes = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM goodair.dim_temps")
    nb_temps = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM goodair.fait_air_quality")
    nb_air = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM goodair.fait_weather")
    nb_weather = cur.fetchone()[0]

    cur.close()
    conn.close()
    return (
        f"Data Warehouse alimenté : "
        f"{nb_villes} villes, {nb_temps} périodes, "
        f"{nb_air} faits air qualité, {nb_weather} faits météo"
    )


# ================================================================
# DÉFINITION DU DAG — Pipeline ETL + DWH complet
# ================================================================
with DAG(
    dag_id="MSPR_GoodAir_Pipeline_V1",
    start_date=datetime(2026, 3, 5),
    schedule_interval="0 * * * *",  # Toutes les heures
    catchup=False,
    tags=["GoodAir", "Prod"]
) as dag:

    task_extract = PythonOperator(
        task_id="extraction_api",
        python_callable=fetch_api_data,
        retries=2,
    )

    # ── Tâche 2 : TRANSFORM via Apache Spark ─────────────────────────────────
    # spark-submit soumet le job au cluster Spark (spark-master:7077).
    # Le script spark/transform_spark.py distribue la transformation sur les
    # workers Spark et écrit les fichiers nettoyés dans data/cleaned/.
    task_transform = BashOperator(
        task_id="transformation_nettoyage_spark",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--name goodair-transform "
            "--executor-memory 512m "
            "--driver-memory 512m "
            "/opt/airflow/spark/transform_spark.py "
            "/opt/airflow/data"
        ),
        retries=1,
    )

    task_load = PythonOperator(
        task_id="chargement_postgres",
        python_callable=load_to_postgres,
        retries=1,
    )

    task_dwh = PythonOperator(
        task_id="alimentation_datawarehouse",
        python_callable=alimenter_datawarehouse,
        retries=1,
    )

    # Pipeline complet : Extract → Transform (Spark) → Load → DWH
    task_extract >> task_transform >> task_load >> task_dwh
