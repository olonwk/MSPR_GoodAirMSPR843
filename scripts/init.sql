-- ============================================================
-- GoodAir - Schéma de base de données
-- Initialisation automatique au démarrage de PostgreSQL
-- ============================================================

CREATE SCHEMA IF NOT EXISTS goodair;

-- ----------------------------------------------------------
-- COUCHE STAGING : données brutes normalisées
-- ----------------------------------------------------------

-- Table : qualité de l'air (source AQICN)
CREATE TABLE IF NOT EXISTS goodair.air_quality (
    id                  SERIAL PRIMARY KEY,
    ville               VARCHAR(100),
    source_api          VARCHAR(20)  DEFAULT 'aqicn',
    date_extraction     TIMESTAMP,
    station_idx         INTEGER,
    station_name        VARCHAR(200),
    aqi                 INTEGER,
    dominant_pollutant  VARCHAR(20),
    mesure_timestamp    TIMESTAMP,
    timezone            VARCHAR(50),
    co                  FLOAT,
    h                   FLOAT,
    no2                 FLOAT,
    o3                  FLOAT,
    p                   FLOAT,
    pm10                FLOAT,
    pm25                FLOAT,
    so2                 FLOAT,
    t                   FLOAT,
    w                   FLOAT,
    lat                 FLOAT,
    lon                 FLOAT,
    created_at          TIMESTAMP    DEFAULT NOW()
);

-- Table : météo (source OpenWeatherMap)
CREATE TABLE IF NOT EXISTS goodair.weather (
    id                  SERIAL PRIMARY KEY,
    ville               VARCHAR(100),
    source_api          VARCHAR(20)  DEFAULT 'owm',
    date_extraction     TIMESTAMP,
    city_name           VARCHAR(100),
    country             VARCHAR(10),
    mesure_timestamp    TIMESTAMP,
    weather_main        VARCHAR(50),
    weather_description VARCHAR(200),
    temp                FLOAT,
    feels_like          FLOAT,
    temp_min            FLOAT,
    temp_max            FLOAT,
    pressure            INTEGER,
    humidity            INTEGER,
    visibility          INTEGER,
    wind_speed          FLOAT,
    wind_deg            INTEGER,
    clouds              INTEGER,
    lat                 FLOAT,
    lon                 FLOAT,
    created_at          TIMESTAMP    DEFAULT NOW()
);

-- Index staging
CREATE INDEX IF NOT EXISTS idx_aq_ville        ON goodair.air_quality(ville);
CREATE INDEX IF NOT EXISTS idx_aq_timestamp    ON goodair.air_quality(mesure_timestamp);
CREATE INDEX IF NOT EXISTS idx_aq_aqi          ON goodair.air_quality(aqi);
CREATE INDEX IF NOT EXISTS idx_weather_ville   ON goodair.weather(ville);
CREATE INDEX IF NOT EXISTS idx_weather_ts      ON goodair.weather(mesure_timestamp);


-- ----------------------------------------------------------
-- COUCHE DATA WAREHOUSE : modèle en étoile
-- ----------------------------------------------------------

-- Dimension : Ville
CREATE TABLE IF NOT EXISTS goodair.dim_ville (
    id      SERIAL PRIMARY KEY,
    nom     VARCHAR(100) UNIQUE NOT NULL,
    lat     FLOAT,
    lon     FLOAT,
    pays    VARCHAR(10) DEFAULT 'FR'
);

-- Dimension : Temps (granularité à l'heure)
CREATE TABLE IF NOT EXISTS goodair.dim_temps (
    id           SERIAL PRIMARY KEY,
    date_heure   TIMESTAMP UNIQUE NOT NULL,
    annee        INTEGER,
    mois         INTEGER,
    jour         INTEGER,
    heure        INTEGER,
    jour_semaine INTEGER   -- 0 = Dimanche, 6 = Samedi
);

-- Fait : Qualité de l'air
CREATE TABLE IF NOT EXISTS goodair.fait_air_quality (
    id                  SERIAL PRIMARY KEY,
    id_ville            INTEGER REFERENCES goodair.dim_ville(id),
    id_temps            INTEGER REFERENCES goodair.dim_temps(id),
    aqi                 INTEGER,
    dominant_pollutant  VARCHAR(20),
    co                  FLOAT,
    h                   FLOAT,
    no2                 FLOAT,
    o3                  FLOAT,
    p                   FLOAT,
    pm10                FLOAT,
    pm25                FLOAT,
    so2                 FLOAT,
    t                   FLOAT,
    w                   FLOAT,
    UNIQUE (id_ville, id_temps)
);

-- Fait : Météo
CREATE TABLE IF NOT EXISTS goodair.fait_weather (
    id                  SERIAL PRIMARY KEY,
    id_ville            INTEGER REFERENCES goodair.dim_ville(id),
    id_temps            INTEGER REFERENCES goodair.dim_temps(id),
    weather_main        VARCHAR(50),
    weather_description VARCHAR(200),
    temp                FLOAT,
    feels_like          FLOAT,
    temp_min            FLOAT,
    temp_max            FLOAT,
    pressure            INTEGER,
    humidity            INTEGER,
    visibility          INTEGER,
    wind_speed          FLOAT,
    wind_deg            INTEGER,
    clouds              INTEGER,
    UNIQUE (id_ville, id_temps)
);

-- Index Data Warehouse
CREATE INDEX IF NOT EXISTS idx_fait_aq_ville   ON goodair.fait_air_quality(id_ville);
CREATE INDEX IF NOT EXISTS idx_fait_aq_temps   ON goodair.fait_air_quality(id_temps);
CREATE INDEX IF NOT EXISTS idx_fait_aq_aqi     ON goodair.fait_air_quality(aqi);
CREATE INDEX IF NOT EXISTS idx_fait_wt_ville   ON goodair.fait_weather(id_ville);
CREATE INDEX IF NOT EXISTS idx_fait_wt_temps   ON goodair.fait_weather(id_temps);