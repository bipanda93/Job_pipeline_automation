from __future__ import annotations

import csv
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

# ============================================================
# CONFIG GLOBALE
# ============================================================

POSTGRES_CONN_ID = "postgres_default"

# Mapping des sources -> chemins CSV (vus depuis le conteneur Airflow)
SOURCES = [
    {
        "name": "hellowork",
        "csv_path": "/opt/airflow_project/dags/data/raw/hellowork/hellowork_details.csv",
        "table_name": "raw_hellowork_jobs",
    },
    {
        "name": "wttj",
        "csv_path": "/opt/airflow_project/dags/data/raw/wttj/wttj_details.csv",
        "table_name": "raw_wttj_jobs",
    },
    {
        "name": "linkedin",
        "csv_path": "/opt/airflow_project/dags/data/raw/linkedin/linkedin_jobs.csv",
        "table_name": "raw_linkedin_jobs",
    },
    {
        "name": "indeed",
        "csv_path": "/opt/airflow_project/dags/data/raw/indeed/indeed_jobs.csv",
        "table_name": "raw_indeed_jobs",
    },
    {
        "name": "france_travail",
        "csv_path": "/opt/airflow_project/dags/data/raw/france_travail/france_travail_jobs.csv",
        "table_name": "raw_francetravail_jobs",
    },
]


# ============================================================
# FONCTIOnanN UTILITAIRE : charger 1 CSV -> Postgres
# ============================================================

def load_csv_to_postgres(csv_path: str, table_name: str, **context):
    """
    Charge un fichier CSV brut dans une table Postgres.

    - Crée la table si elle n'existe pas (toutes colonnes TEXT)
    - TRUNCATE la table
    - COPY tout le CSV dans la table
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV introuvable : {csv_path}")

    # Lire l'en-tête
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError(f"Le fichier CSV est vide : {csv_path}")

    if not header:
        raise ValueError(f"Le CSV ne contient pas d'en-tête : {csv_path}")

    # Construire la définition des colonnes (tout en TEXT pour le raw)
    cols_sql = ", ".join([f'"{col}" TEXT' for col in header])

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    # Créer la table si besoin
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {cols_sql}
    );
    """
    hook.run(create_sql)

    # Full refresh
    hook.run(f"TRUNCATE TABLE {table_name};")

    # COPY depuis le CSV
    copy_sql = f"""
    COPY {table_name}
    FROM STDIN
    WITH CSV HEADER DELIMITER ',';
    """
    hook.copy_expert(sql=copy_sql, filename=csv_path)


# ============================================================
# DÉFINITION DU DAG
# ============================================================

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="load_all_jobboards_to_postgres",
    description="Charge tous les CSV de job boards (HelloWork, WTTJ, LinkedIn, Indeed, France Travail) en tables RAW dans Postgres",
    default_args=default_args,
    schedule_interval="0 4 * * *",  # chaque jour à 4h du matin
    start_date=days_ago(1),
    catchup=False,
    tags=["jobs", "raw", "multi_source"],
) as dag:

    tasks = {}

    for src in SOURCES:
        name = src["name"]
        csv_path = src["csv_path"]
        table_name = src["table_name"]

        task = PythonOperator(
            task_id=f"load_{name}_csv_to_postgres",
            python_callable=load_csv_to_postgres,
            op_kwargs={
                "csv_path": csv_path,
                "table_name": table_name,
            },
        )
        tasks[name] = task

