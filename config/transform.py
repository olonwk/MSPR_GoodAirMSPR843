# =============================================================================
# transform.py — Nettoyage et transformation des données brutes API
# =============================================================================
#
# Ce script traite les fichiers JSON collectés depuis deux APIs :
#   - AQICN  (World Air Quality Index)  : qualité de l'air
#   - OWM    (OpenWeatherMap)           : météo
#
# Utilisation : python transform.py
#   → Traite automatiquement tous les fichiers JSON du dossier courant
#   → Écrit les fichiers nettoyés dans le sous-dossier cleaned/
#
# =============================================================================
# STRUCTURE DES DONNÉES AQICN (après transformation)
# =============================================================================
#
#   ville         : nom de la ville (tel que fourni à l'API)
#   source        : "aqicn" (indicateur d'origine)
#   lat / lon     : coordonnées GPS de la station de mesure
#   aqi           : Air Quality Index global (0-500)
#                     0-50   Bon
#                     51-100 Modéré
#                    101-150 Mauvais pour les groupes sensibles
#                    151-200 Mauvais
#                    201-300 Très mauvais
#                    301-500 Dangereux
#   dominentpol   : polluant dominant (ex : "pm25", "o3")
#   timestamp     : horodatage ISO 8601 de la dernière mesure
#   timezone      : fuseau horaire de la station (ex : "+01:00")
#
#   iaqi          : mesures individuelles par polluant (Individual AQI)
#     co          : monoxyde de carbone (mg/m³)
#     h           : humidité relative (%)
#     no2         : dioxyde d'azote (µg/m³)
#     o3          : ozone (µg/m³)
#     p           : pression atmosphérique (hPa)
#     pm10        : particules fines ≤ 10 µm (µg/m³)
#     pm25        : particules fines ≤ 2.5 µm (µg/m³) — le plus surveillé
#     so2         : dioxyde de soufre (µg/m³)
#     t           : température (°C)
#     w           : vitesse du vent (m/s)
#
#   forecast      : prévisions journalières (uniquement dates de l'année courante)
#     o3          : prévisions ozone        — avg / min / max sur la journée
#     pm10        : prévisions PM10         — avg / min / max sur la journée
#     pm25        : prévisions PM2.5        — avg / min / max sur la journée
#     uvi         : indice UV (0-11+)       — avg / min / max sur la journée
#                     0-2   Faible
#                     3-5   Modéré
#                     6-7   Élevé
#                     8-10  Très élevé
#                     11+   Extrême
#
# =============================================================================
# STRUCTURE DES DONNÉES OWM (après transformation)
# =============================================================================
#
#   ville               : nom de la ville (tel que fourni à l'API)
#   source              : "owm" (indicateur d'origine)
#   lat / lon           : coordonnées GPS de la station de mesure
#   timestamp           : horodatage ISO 8601 de la mesure
#   timezone_offset_s   : décalage UTC en secondes (ex : 3600 = UTC+1)
#
#   weather_main        : condition météo principale (ex : "Clear", "Rain", "Clouds")
#   weather_description : condition météo détaillée en français (ex : "ciel dégagé")
#
#   temp                : température actuelle (°C)
#   feels_like          : température ressentie (°C)
#   temp_min            : température minimale observée sur la zone (°C)
#   temp_max            : température maximale observée sur la zone (°C)
#   pressure            : pression atmosphérique au niveau de la mer (hPa)
#   humidity            : humidité relative (%)
#   sea_level           : pression au niveau de la mer (hPa) — identique à pressure
#   grnd_level          : pression au sol (hPa) — utile en altitude
#
#   visibility          : visibilité horizontale (mètres, max 10 000)
#
#   wind_speed          : vitesse du vent (m/s)
#   wind_deg            : direction du vent (degrés, 0=Nord, 90=Est, 180=Sud, 270=Ouest)
#   wind_gust           : vitesse des rafales (m/s) — absent si pas de rafale
#
#   clouds              : couverture nuageuse (%, 0=ciel dégagé, 100=couvert)
#
#   sunrise             : heure du lever du soleil (ISO 8601, heure locale)
#   sunset              : heure du coucher du soleil (ISO 8601, heure locale)
#   country             : code pays ISO 3166-1 alpha-2 (ex : "FR")
#
# =============================================================================

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path


# Année courante : utilisée pour filtrer les prévisions AQICN périmées
CURRENT_YEAR = datetime.now().year


# ─── Détection du type de fichier ────────────────────────────────────────────

def detect_source(raw: list) -> str:
    """
    Détermine l'origine du fichier JSON en inspectant la structure du premier enregistrement.
    - La clé "status" est propre aux réponses AQICN
    - La clé "cod"    est propre aux réponses OpenWeatherMap
    """
    if not raw:
        raise ValueError("Fichier vide.")
    first = raw[0].get("data", {})
    if "status" in first:
        return "aqicn"
    if "cod" in first:
        return "owm"
    raise ValueError("Format non reconnu (ni AQICN ni OWM).")


# ─── Transformations AQICN ───────────────────────────────────────────────────

def _unwrap_iaqi(iaqi: dict) -> dict:
    """
    L'API AQICN encapsule chaque mesure dans un objet { "v": valeur }.
    Cette fonction aplatit la structure : { "pm25": { "v": 68 } } -> { "pm25": 68 }
    """
    return {key: val["v"] for key, val in iaqi.items() if "v" in val}


def _filter_forecast(forecast: dict) -> dict:
    """
    Les prévisions AQICN contiennent parfois des entrées avec des dates très anciennes
    (bug connu de l'API). On ne conserve que celles dont l'année correspond à l'année courante.
    """
    cleaned = {}
    for pollutant, entries in forecast.get("daily", {}).items():
        valid = [e for e in entries if e.get("day", "").startswith(str(CURRENT_YEAR))]
        if valid:
            cleaned[pollutant] = valid
    return cleaned


def transform_aqicn(entry: dict) -> dict | None:
    """
    Transforme un enregistrement brut AQICN en structure aplatie et nettoyée.
    Retourne None si la requête API a échoué (status != "ok").

    Suppressions : attributions, debug, idx, city.url, city.location, time.v (timestamp Unix)
    Transformations :
      - geo [lat, lon]  -> champs lat / lon séparés
      - iaqi            -> valeurs désenveloppées (suppression du niveau { "v": ... })
      - forecast        -> filtrage des dates périmées
    """
    ville = entry.get("ville", "")
    outer = entry.get("data", {})

    # On ignore les villes pour lesquelles l'API a renvoyé une erreur
    if outer.get("status") != "ok":
        print(f"[SKIP] {ville} — statut AQICN : {outer.get('status')}")
        return None

    # L'API AQICN imbrique les données sous data.data
    d = outer["data"]
    geo = d.get("city", {}).get("geo", [None, None])
    time_info = d.get("time", {})

    return {
        "ville": ville,
        "source": "aqicn",
        "lat": geo[0],
        "lon": geo[1],
        "aqi": d.get("aqi"),                          # Indice de qualité de l'air global
        "dominentpol": d.get("dominentpol"),           # Polluant dominant
        "timestamp": time_info.get("iso"),             # Horodatage ISO 8601 de la mesure
        "timezone": time_info.get("tz"),               # Fuseau horaire de la station
        "iaqi": _unwrap_iaqi(d.get("iaqi", {})),       # Mesures individuelles par polluant
        "forecast": _filter_forecast(d.get("forecast", {})),  # Prévisions (dates valides uniquement)
    }


# ─── Transformations OWM ─────────────────────────────────────────────────────

def _unix_to_iso(ts: int, tz_offset_seconds: int = 0) -> str:
    """Convertit un timestamp Unix en chaîne ISO 8601 avec le fuseau horaire local de la ville."""
    tz = timezone(timedelta(seconds=tz_offset_seconds))
    return datetime.fromtimestamp(ts, tz=tz).isoformat()


def transform_owm(entry: dict) -> dict | None:
    """
    Transforme un enregistrement brut OpenWeatherMap en structure aplatie et nettoyée.
    Retourne None si la requête API a échoué (cod != 200).

    Suppressions : base, sys.type, sys.id, id (city ID interne), cod, weather[].icon, weather[].id
    Transformations :
      - coord              -> champs lat / lon séparés
      - weather (tableau)  -> weather_main + weather_description (on prend le 1er élément)
      - main               -> champs remontés au niveau racine
      - wind               -> champs wind_speed, wind_deg, wind_gust au niveau racine
      - clouds.all         -> clouds
      - dt, sunrise, sunset (Unix) -> chaînes ISO 8601 avec fuseau horaire local
    """
    ville = entry.get("ville", "")
    d = entry.get("data", {})

    # On ignore les villes pour lesquelles l'API a renvoyé une erreur
    if d.get("cod") != 200:
        print(f"[SKIP] {ville} — cod OWM : {d.get('cod')}")
        return None

    tz_offset = d.get("timezone", 0)   # Décalage horaire en secondes par rapport à UTC
    coord = d.get("coord", {})
    main = d.get("main", {})
    wind = d.get("wind", {})
    sys_info = d.get("sys", {})
    # OWM renvoie une liste de conditions météo ; on prend la première (dominante)
    weather = (d.get("weather") or [{}])[0]

    return {
        "ville": ville,
        "source": "owm",
        "lat": coord.get("lat"),
        "lon": coord.get("lon"),
        "timestamp": _unix_to_iso(d["dt"], tz_offset),   # Horodatage de la mesure
        "timezone_offset_s": tz_offset,
        "weather_main": weather.get("main"),              # Ex : "Clear", "Clouds", "Rain"
        "weather_description": weather.get("description"),# Ex : "ciel dégagé"
        "temp": main.get("temp"),                         # Température ressentie (°C)
        "feels_like": main.get("feels_like"),
        "temp_min": main.get("temp_min"),
        "temp_max": main.get("temp_max"),
        "pressure": main.get("pressure"),                 # Pression au niveau de la mer (hPa)
        "humidity": main.get("humidity"),                 # Humidité relative (%)
        "sea_level": main.get("sea_level"),               # Pression au niveau de la mer (hPa)
        "grnd_level": main.get("grnd_level"),             # Pression au sol (hPa)
        "visibility": d.get("visibility"),                # Visibilité en mètres (max 10 000)
        "wind_speed": wind.get("speed"),                  # Vitesse du vent (m/s)
        "wind_deg": wind.get("deg"),                      # Direction du vent (degrés)
        "wind_gust": wind.get("gust"),                    # Rafales (m/s) — absent si calme
        "clouds": d.get("clouds", {}).get("all"),         # Couverture nuageuse (%)
        "sunrise": _unix_to_iso(sys_info["sunrise"], tz_offset) if "sunrise" in sys_info else None,
        "sunset": _unix_to_iso(sys_info["sunset"], tz_offset) if "sunset" in sys_info else None,
        "country": sys_info.get("country"),               # Code pays ISO (ex : "FR")
    }


# ─── Pipeline principal ───────────────────────────────────────────────────────

# Associe chaque source détectée à sa fonction de transformation
TRANSFORMERS = {
    "aqicn": transform_aqicn,
    "owm": transform_owm,
}


OUTPUT_DIR = "cleaned"


def transform_file(input_file: Path, output_dir: Path) -> None:
    """
    Charge un fichier JSON brut (AQICN ou OWM), applique la transformation adaptée
    et écrit le résultat dans output_dir sous le même nom de fichier.
    """
    with open(input_file, encoding="utf-8") as f:
        raw = json.load(f)

    # Détection automatique de la source selon la structure du fichier
    source = detect_source(raw)
    transform = TRANSFORMERS[source]

    output_path = output_dir / input_file.name

    # Transformation de chaque entrée ; les None (erreurs API) sont écartés
    results = [r for entry in raw if (r := transform(entry)) is not None]

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"  [{source.upper()}] {input_file.name} → {len(results)} villes → {output_path}")


def find_and_transform_all(directory: Path) -> None:
    """
    Parcourt le dossier donné, transforme tous les fichiers JSON reconnus (AQICN ou OWM)
    et écrit les résultats dans un sous-dossier 'cleaned/'.

    Sont ignorés :
      - les fichiers déjà présents dans le dossier de sortie
      - les fichiers dont le format n'est ni AQICN ni OWM
    """
    output_dir = directory / OUTPUT_DIR
    output_dir.mkdir(exist_ok=True)  # Crée le dossier cleaned/ s'il n'existe pas

    json_files = sorted(directory.glob("*.json"))

    if not json_files:
        print("Aucun fichier JSON trouvé dans le dossier courant.")
        return

    print(f"Dossier de sortie : {output_dir}")
    print(f"{len(json_files)} fichier(s) JSON détecté(s)\n")

    ok, skipped = 0, 0
    for json_file in json_files:
        try:
            transform_file(json_file, output_dir)
            ok += 1
        except ValueError as e:
            # Format non reconnu (ni AQICN ni OWM) : on passe silencieusement
            print(f"  [SKIP] {json_file.name} — {e}")
            skipped += 1

    print(f"\n✅ {ok} fichier(s) transformé(s), {skipped} ignoré(s).")


if __name__ == "__main__":
    current_dir = Path.cwd()
    find_and_transform_all(current_dir)
