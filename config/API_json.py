import httpx
import json
import time
from datetime import datetime

villes_france = [
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

AQICN_KEY = "254ac36feb11085edbf9e38c2cb95abdb26edbd7"
OWM_KEY = "4451c513b7950cf680f50ee157858d00"

def collecter_donnees():
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    print(f"\n{'='*50}")
    print(f"  Collecte lancée à {timestamp}")
    print(f"{'='*50}\n")

    # === AQICN ===
    resultats_aqicn = []
    for city in villes_france:
        url = f"https://api.waqi.info/feed/{city}/?token={AQICN_KEY}"
        try:
            response = httpx.get(url, timeout=10)
            data = response.json()
            resultats_aqicn.append({"ville": city, "data": data})
            print(f"[AQICN] {city} : OK")
        except Exception as e:
            print(f"[AQICN] {city} : ERREUR - {e}")
        time.sleep(0.5)

    with open(f"response_aqicn_france_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(resultats_aqicn, f, indent=2, ensure_ascii=False)
    print(f"\n✅ AQICN : {len(resultats_aqicn)} villes sauvegardées\n")

    # === OWM ===
    resultats_owm = []
    for city in villes_france:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city},FR&appid={OWM_KEY}&units=metric&lang=fr"
        try:
            response = httpx.get(url, timeout=10)
            data = response.json()
            resultats_owm.append({"ville": city, "data": data})
            print(f"[OWM] {city} : OK")
        except Exception as e:
            print(f"[OWM] {city} : ERREUR - {e}")
        time.sleep(0.5)

    with open(f"response_owm_france_{timestamp}.json", "w", encoding="utf-8") as f:
        json.dump(resultats_owm, f, indent=2, ensure_ascii=False)
    print(f"\n✅ OWM : {len(resultats_owm)} villes sauvegardées")

# === Boucle principale ===
while True:
    collecter_donnees()
    print(f"\n⏳ Prochaine collecte dans 1 heure...")
    time.sleep(3600)  # 3600 secondes = 1 heure