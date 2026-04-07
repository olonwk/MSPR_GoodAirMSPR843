from airflow import DAG # pyright: ignore[reportMissingImports]
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from datetime import datetime

# Une fonction toute simple que Airflow va appeler
def hello_goodair():
    print("🚀 Airflow est opérationnel sur la machine ")
    return "Succès"

# Définition du DAG (le workflow)
with DAG(
    dag_id="01_hello_world_test",  # Le nom qui apparaîtra dans l'interface
    start_date=datetime(2026, 3, 1),
    schedule_interval=None,         # On ne le planifie pas, on le lancera à la main
    catchup=False,
    tags=["test"]
) as dag:

    # La tâche unique
    task_test = PythonOperator(
        task_id="dire_bonjour",
        python_callable=hello_goodair
    )