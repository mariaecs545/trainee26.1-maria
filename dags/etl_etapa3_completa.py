from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def processar_dados():
    caminho_arquivo = '/opt/airflow/dags/dados_processo_seletivo.csv'
    df = pd.read_csv(caminho_arquivo, sep=None, engine='python')
    print(df.columns.tolist())
    print(f"Total inicial: {len(df)} registros")

    # remove espacos dos nomes
    df.columns = df.columns.str.strip()

    # remove a coluna indice
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])
    
    # remover registros OpenRouteService
    df = df[
        df["geoapi_id"]
        .astype(str)
        .str.upper()
        != "OPENROUTESERVICE"
    ]
    print(f"Após remover OpenRouteService: {len(df)}")

    # converter as coordenadas para numero
    df["latitude"] = pd.to_numeric(
        df["latitude"],
        errors = "coerce"
    )

    df["longitude"] = pd.to_numeric(
        df["longitude"],
        errors="coerce"
    )

    # remover coordenadas inválidas
    df = df.dropna(
        subset = ["latitude", "longitude"]
    )
    
    print(f"Após remover coordenadas vazias: {len(df)}")

    # evita pontos fora do país
    df = df[
        (df["latitude"] >= -34) &
        (df["latitude"] <= 6) &
        (df["longitude"] >= -74) &
        (df["longitude"] <= -34)
    ]
    print(f"Após validação geográfica: {len(df)}")

    # conexao PostgreSQL
    engine = create_engine(
        "postgresql+psycopg2://admin:admin@postgres:5432/banco_etl"
    )

    df.to_sql(
        "enderecos_etapa3",
        engine,
        if_exists = "replace",
        index = False
    )

    print(f"{len(df)} registros inseridos com sucesso.")

with DAG(
    dag_id = 'dag_etapa3_completa',
    start_date=datetime(2025, 1, 1),
    schedule='@daily',
    catchup = False
) as dag:
    tarefa_etl = PythonOperator(
        task_id = 'executar_etl_completo',
        python_callable = processar_dados
    )
