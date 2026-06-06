from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def processar_dados():
    caminho_arquivo = '/opt/airflow/dags/dados_processo_seletivo.csv'
    df = pd.read_csv(caminho_arquivo, sep=None, engine='python')
    df_aracaju = df[df['city'].astype(str).str.upper() == 'ARACAJU'] # filtra aracaju

    # conexao PostgreSQL
    engine = create_engine(
        "postgresql+psycopg2://admin:admin@postgres:5432/banco_etl"
    )

    # salva na tabela
    df_aracaju.to_sql(
        "enderecos_aracaju",
        engine,
        if_exists="replace",
        index=False
    )

    print(f"{len(df_aracaju)} registros inseridos.")

with DAG(
    dag_id = 'dag_filtro_aracaju',
    start_date=datetime(2026, 6, 4),
    schedule='@daily'
) as dag:
    tarefa_etl = PythonOperator(
        task_id = 'filtrar_e_salvar_enderecos',
        python_callable = processar_dados
    )
