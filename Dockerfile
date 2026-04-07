# =============================================================================
# Dockerfile — Airflow personnalisé avec support Apache Spark (PySpark 3.5)
# =============================================================================
# Java est indispensable pour que PySpark puisse démarrer la JVM Spark.
# Les dépendances Python sont intégrées ici (remplace _PIP_ADDITIONAL_REQUIREMENTS).
# =============================================================================

FROM apache/airflow:2.10.5-python3.10

USER root

# ── Java (requis par PySpark) ─────────────────────────────────────────────────
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ── Dépendances Python ────────────────────────────────────────────────────────
USER airflow

RUN pip install --no-cache-dir \
    "apache-airflow==2.10.5" \
    pyspark==3.4.0 \
    "apache-airflow-providers-apache-spark==4.10.0" \
    httpx \
    requests \
    psycopg2-binary \
    boto3

# ── Rendre spark-submit accessible dans le PATH ───────────────────────────────
# pyspark embarque spark-submit dans son répertoire bin/. On crée un lien
# symbolique dans /usr/local/bin/ pour le rendre disponible en tant que root.
USER root
RUN find /home/airflow/.local -name "spark-submit" -type f \
    -exec ln -sf {} /usr/local/bin/spark-submit \; \
    && chmod +x /usr/local/bin/spark-submit

# Indiquer à PySpark quel interpréteur Python utiliser sur driver et workers
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.10

USER airflow
