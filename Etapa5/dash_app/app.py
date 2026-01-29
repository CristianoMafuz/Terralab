import dash
from dash import dcc, html
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
import time

# Conectar ao PostgreSQL com retry
def get_data():
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host='postgres',
                database='airflow',
                user='airflow',
                password='airflow'
            )
            
            # Verifica se a tabela existe
            cur = conn.cursor()
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'dados_tratados'
                );
            """)
            tabela_existe = cur.fetchone()[0]
            
            if not tabela_existe:
                print("⚠️ Tabela 'dados_tratados' não existe ainda. Execute o DAG da Etapa 3 no Airflow primeiro!")
                conn.close()
                return pd.DataFrame()
            
            query = "SELECT * FROM dados_tratados"
            df = pd.read_sql(query, conn)
            conn.close()
            
            if df.empty:
                print("⚠️ Tabela 'dados_tratados' está vazia. Execute o DAG da Etapa 3 no Airflow!")
            
            return df
            
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"Tentativa {attempt + 1} falhou. Tentando novamente em {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Erro após {max_retries} tentativas: {e}")
                return pd.DataFrame()
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")
            return pd.DataFrame()


# Criar app
app = dash.Dash(__name__)

# Layout
app.layout = html.Div([
    html.H1("Dashboard de Ciência de Dados", style={'textAlign': 'center'}),
    
    html.Div([
        html.H2("Mapa 1 - Pontos por ID"),
        dcc.Graph(id='mapa1')
    ]),
    
    html.Div([
        html.H2("Mapa 2 - Dentro/Fora da UF"),
        dcc.Graph(id='mapa2')
    ]),
    
    html.Div([
        html.H2("Dados por Mês"),
        dcc.Graph(id='grafico_mes')
    ]),
    
    html.Div([
        html.H2("Top 3 Cidades"),
        dcc.Graph(id='grafico_cidades')
    ]),
    
    # Adicione um botão ou interval para atualizar
    dcc.Interval(id='interval-component', interval=5000, n_intervals=0)
])

# Callbacks para atualizar gráficos
@app.callback(
    [dash.dependencies.Output('mapa1', 'figure'),
     dash.dependencies.Output('mapa2', 'figure'),
     dash.dependencies.Output('grafico_mes', 'figure'),
     dash.dependencies.Output('grafico_cidades', 'figure')],
    [dash.dependencies.Input('interval-component', 'n_intervals')]
)
def update_graphs(n):
    df = get_data()
    
    if df.empty:
        # Retorna gráficos com mensagem informativa
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            text="⚠️ Nenhum dado encontrado!<br><br>Execute o DAG 'etapa3_etl_completo' no Airflow<br>para processar os dados.<br><br>Acesse: http://localhost:8080", 
            xref="paper", yref="paper",
            x=0.5, y=0.5, 
            showarrow=False,
            font=dict(size=16, color="orange")
        )
        return empty_fig, empty_fig, empty_fig, empty_fig
    
    # Mapa 1 - Colorido por ID
    fig_mapa1 = px.scatter_mapbox(
        df,
        lat='latitude',
        lon='longitude',
        color='id',
        zoom=10,
        height=500,
        title="Pontos por ID"
    )
    fig_mapa1.update_layout(mapbox_style="open-street-map")
    
    # Mapa 2 - Dentro/Fora da UF
    df['cor'] = df['dentro_uf'].map({True: 'Verde (Dentro)', False: 'Vermelho (Fora)'})
    fig_mapa2 = px.scatter_mapbox(
        df,
        lat='latitude',
        lon='longitude',
        color='cor',
        color_discrete_map={'Verde (Dentro)': 'green', 'Vermelho (Fora)': 'red'},
        zoom=10,
        height=500,
        title="Pontos Dentro/Fora da UF"
    )
    fig_mapa2.update_layout(mapbox_style="open-street-map")
    
    # Gráfico por mês
    df['mes'] = pd.to_datetime(df['data_processamento']).dt.to_period('M').astype(str)
    dados_mes = df.groupby('mes').size().reset_index(name='count')
    fig_mes = px.bar(dados_mes, x='mes', y='count', title="Requisições por Mês")
    
    # Top 3 cidades
    top_cidades = df['city'].value_counts().head(3).reset_index()
    top_cidades.columns = ['city', 'count']
    fig_cidades = px.bar(top_cidades, x='city', y='count', title="Top 3 Cidades")
    
    return fig_mapa1, fig_mapa2, fig_mes, fig_cidades

if __name__ == '__main__':
    # app.run_server(host='0.0.0.0', port=8050, debug=True)
    app.run(host='0.0.0.0', port=8050, debug=True)