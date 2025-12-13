from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def extrair_csv():
    """Extrai dados do CSV"""
    df = pd.read_csv('/opt/airflow/dags/dados.csv')
    # Salva temporariamente para próxima task
    df.to_csv('/opt/airflow/dags/temp_dados.csv', index=False)
    print(f"Total de registros: {len(df)}")

def filtrar_aracaju():
    """Filtra apenas endereços de Aracaju"""
    df = pd.read_csv('/opt/airflow/dags/temp_dados.csv')
    
    # Filtrar Aracaju
    df_aracaju = df[df['city'].str.contains('Aracaju', case=False, na=False)]
    
    print(f"Registros de Aracaju: {len(df_aracaju)}")
    
    # Salvar no PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    # 1. Criar tabela
    cur.execute("""
        CREATE TABLE IF NOT EXISTS enderecos_aracaju (
            id SERIAL PRIMARY KEY,
            city TEXT,
            state TEXT,
            latitude FLOAT,
            longitude FLOAT,
            data_importacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 2. Seleciona as colunas que existem no CSV
    dados = df_aracaju[['city', 'state', 'latitude', 'longitude']].values.tolist()
    
    # 3. Ajuste o INSERT
    execute_values(cur, 
        "INSERT INTO enderecos_aracaju (city, state, latitude, longitude) VALUES %s",
        dados
    )
    
    conn.commit()
    cur.close()
    conn.close()
    print("Dados salvos no PostgreSQL!")

with DAG(
    'etapa2_importar_aracaju',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etapa2']
) as dag:
    
    task_extrair = PythonOperator(
        task_id='extrair_csv',
        python_callable=extrair_csv
    )
    
    task_filtrar = PythonOperator(
        task_id='filtrar_aracaju',
        python_callable=filtrar_aracaju
    )
    
    task_extrair >> task_filtrar