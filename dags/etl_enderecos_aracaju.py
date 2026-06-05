from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

def processar_dados():
    df = pd.read_csv('https://docs.google.com/uc?export=download&id=1SrT78fHBCDJx9vox7tlcpq_kVccmjCj1')
    df_aracaju = df[df['cidade'] == 'Aracaju']

with DAG(
    dag_id = 'dag_filtro_aracaju',
    start_date=datetime(2026, 6, 4),
    schedule='@daily'
) as dag:
    tarefa_etl = PythonOperator(
        task_id = 'filtrar_e_salvar_enderecos',
        python_callable = processar_dados
    )
