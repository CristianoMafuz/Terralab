from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import requests
from shapely.geometry import Point, shape



# Dados do polígono de Sergipe (simplificado)
SERGIPE_BOUNDS = {
    "type": "Polygon",
    "coordinates": [[
        [-37.9, -11.5],
        [-36.5, -11.5],
        [-36.5, -9.5],
        [-37.9, -9.5],
        [-37.9, -11.5]
    ]]
}

def extrair_dados():
    """Extrai dados do CSV"""
    df = pd.read_csv('/opt/airflow/dags/dados.csv')
    df.to_csv('/opt/airflow/dags/etapa3_temp.csv', index=False)
    print(f"Extraídos {len(df)} registros")

# def geocodificar_enderecos():
#     """Geocodifica UMA AMOSTRA PEQUENA para teste"""
#     import time
    
#     print("Iniciando leitura do CSV...")
#     df = pd.read_csv('/opt/airflow/dags/etapa3_temp.csv')
    
#     # Processar 50 linhas prova que o conceito funciona sem estourar o limite da API gratuita.
#     df = df.head(50) 
    
#     total_linhas = len(df)
#     print(f"Modo de Teste: Processando apenas {total_linhas} registros.")
    
#     # A chave
#     API_KEY = "eyJvcmciOiI1YjNjZTM1OTc4NTExMTAwMDFjZjYyNDgiLCJpZCI6IjE2Njg4M2M3ZjU3NzQ0Nzk4ZjQ4OWEzZjM2ZjA4ODhkIiwiaCI6Im11cm11cjY0In0="
    
#     latitudes = []
#     longitudes = []
    
#     for idx, row in df.iterrows():
#         try:
#             # Montar endereço
#             endereco = f"{row.get('address', '')}, {row.get('city', '')}, {row.get('state', '')}"
            
#             url = "https://api.openrouteservice.org/geocode/search"
#             params = {'text': endereco}
#             headers = {'Authorization': API_KEY}
            
#             # Faz a requisição
#             response = requests.get(url, params=params, headers=headers, timeout=10)
            
#             if response.status_code == 200:
#                 data = response.json()
#                 if data['features']:
#                     coords = data['features'][0]['geometry']['coordinates']
#                     longitudes.append(coords[0])
#                     latitudes.append(coords[1])
#                     print(f"Linha {idx}: Sucesso")
#                 else:
#                     print(f"Linha {idx}: Endereço não encontrado")
#                     latitudes.append(None)
#                     longitudes.append(None)
#             else:
#                 print(f"Erro API linha {idx} (Status {response.status_code}): {response.text}")
#                 latitudes.append(None)
#                 longitudes.append(None)
                
#             # Importante: O plano free limita a ~40 requisições por minuto.
#             # Vamos colocar uma pausa pequena para não tomar erro 429 (Too Many Requests)
#             time.sleep(1.5) 
                
#         except Exception as e:
#             print(f"Erro exceção linha {idx}: {e}")
#             latitudes.append(None)
#             longitudes.append(None)
    
#     df['latitude'] = latitudes
#     df['longitude'] = longitudes
    
#     df_clean = df.dropna(subset=['latitude', 'longitude'])
    
#     df_clean.to_csv('/opt/airflow/dags/etapa3_geocoded.csv', index=False)
#     print(f"Finalizado! Geocodificados {len(df_clean)} registros.")

def geocodificar_enderecos():
    """Versão MOCK (Falsa) para não gastar API Key"""
    import random
    
    print("Iniciando geocodificação simulada...")
    df = pd.read_csv('/opt/airflow/dags/etapa3_temp.csv')
    
    # Vamos pegar apenas 50 linhas para ser rápido
    df = df.head(50)
    
    # Em vez de chamar a API, geramos coordenadas aleatórias dentro de Sergipe
    latitudes = []
    longitudes = []
    
    for idx, row in df.iterrows():
        # Gera coord aleatória próxima de Aracaju
        lat = -10.9 + (random.random() * 0.5) 
        lon = -37.0 - (random.random() * 0.5)
        
        latitudes.append(lat)
        longitudes.append(lon)
        print(f"Simulando linha {idx}: Lat {lat:.4f}, Lon {lon:.4f}")
    
    df['latitude'] = latitudes
    df['longitude'] = longitudes
    
    df.to_csv('/opt/airflow/dags/etapa3_geocoded.csv', index=False)
    print("Geocodificação simulada concluída!")

def verificar_dentro_uf():
    """Verifica se ponto está dentro de Sergipe"""
    df = pd.read_csv('/opt/airflow/dags/etapa3_geocoded.csv')
    
    poligono_se = shape(SERGIPE_BOUNDS)
    
    dentro_uf = []
    for _, row in df.iterrows():
        ponto = Point(row['longitude'], row['latitude'])
        dentro_uf.append(poligono_se.contains(ponto))
    
    df['dentro_uf'] = dentro_uf
    
    # Remove pontos fora da UF
    df_final = df[df['dentro_uf'] == True]
    
    df_final.to_csv('/opt/airflow/dags/etapa3_final.csv', index=False)
    print(f"Registros finais dentro da UF: {len(df_final)}")

def carregar_postgres():
    """Carrega dados no PostgreSQL (Criando coluna address se faltar)"""
    import numpy as np # Importante caso precise lidar com NaN
    
    # Lê o arquivo
    df = pd.read_csv('/opt/airflow/dags/etapa3_final.csv')
    
    # Normaliza nomes de colunas para minúsculo e sem espaços
    df.columns = [c.strip().lower() for c in df.columns]
    
    print(f"Colunas originais: {df.columns.tolist()}")

    if 'address' not in df.columns:
        print("AVISO: Coluna 'address' não encontrada. Criando coluna padrão.")
        df['address'] = 'Endereço Não Informado'
    
    # Garante que as outras colunas essenciais existam
    required_cols = ['city', 'state', 'latitude', 'longitude', 'dentro_uf']
    for col in required_cols:
        if col not in df.columns:
            # Se faltar city ou state, cria vazio
            df[col] = None
            print(f"Criada coluna vazia para: {col}")

    # Conecta no Banco
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    # Criar tabela final
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dados_tratados (
            id SERIAL PRIMARY KEY,
            address TEXT,
            city TEXT,
            state TEXT,
            latitude FLOAT,
            longitude FLOAT,
            dentro_uf BOOLEAN,
            data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Prepara os dados na ordem exata da query SQL
    # Tratamento de NaN para None
    df = df.where(pd.notnull(df), None)
    
    dados_finais = df[['address', 'city', 'state', 'latitude', 'longitude', 'dentro_uf']].values.tolist()
    
    execute_values(cur,
        """INSERT INTO dados_tratados 
           (address, city, state, latitude, longitude, dentro_uf) 
           VALUES %s""",
        dados_finais
    )
    
    conn.commit()
    cur.close()
    conn.close()
    print(f"SUCESSO! Carregados {len(df)} registros no PostgreSQL")



with DAG(
    'etapa3_etl_completo',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etapa3']
) as dag:
    
    task_extrair = PythonOperator(
        task_id='extrair_dados',
        python_callable=extrair_dados
    )
    
    task_geocodificar = PythonOperator(
        task_id='geocodificar_enderecos',
        python_callable=geocodificar_enderecos
    )
    
    task_verificar_uf = PythonOperator(
        task_id='verificar_dentro_uf',
        python_callable=verificar_dentro_uf
    )
    
    task_carregar = PythonOperator(
        task_id='carregar_postgres',
        python_callable=carregar_postgres
    )
    
    task_extrair >> task_geocodificar >> task_verificar_uf >> task_carregar
