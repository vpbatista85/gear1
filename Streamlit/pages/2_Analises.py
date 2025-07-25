# import streamlit as st
# import pandas as pd
# import numpy as np
# import plotly.express as px
# import plotly.graph_objects as go
# from plotly.subplots import make_subplots


# # Função para calcular as sequências de voltas limpas
# def calcular_sequencias_voltas_limpas(df):
#     sequencias = []
#     for driver, group in df.groupby('Driver'):
#         # Identificar mudanças na coluna 'Clean' para detectar sequências
#         group['Shift'] = group['Clean'].shift(1)
#         group['New_Sequence'] = (group['Clean'] != group['Shift'])
#         group['Sequence_ID'] = group['New_Sequence'].cumsum()

#         # Filtrar apenas as sequências de voltas limpas (Clean == 1)
#         clean_sequences = group[group['Clean'] == 1]

#         # Contar o tamanho de cada sequência
#         sequence_lengths = clean_sequences.groupby('Sequence_ID').size()

#         # Armazenar os resultados
#         for length in sequence_lengths:
#             sequencias.append({'Driver': driver, 'Sequence_Length': length})
    
#     return pd.DataFrame(sequencias)

# def identificar_sequencias(df):
#     sequencias = []
#     for driver, group in df.groupby('Driver'):
#         group = group.sort_values(by='Lap')
#         group['Shift'] = group['Clean'].shift(1)
#         group['New_Sequence'] = (group['Clean'] != group['Shift'])
#         group['Sequence_ID'] = group['New_Sequence'].cumsum()
#         sequencias.append(group)
#     return pd.concat(sequencias)

# def main():
#     st.set_page_config(
#         page_title="Gear 1 Post Race",
#         page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",
#         layout="wide",
#         initial_sidebar_state="expanded",
#         # menu_items={
#         #     'Get Help': 'https://www.extremelycoolapp.com/help',
#         #     'Report a bug': "https://www.extremelycoolapp.com/bug",
#         #     'About': "# This is a header. This is an *extremely* cool app!"
#         # }
#     )
#     st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)

#     col1, col2 = st.columns(2)
#     col1.title(":green[Análise Pós Corrida]")
#     # col2.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)

#     uploaded_file = st.file_uploader(":green[Escolha o arquivo Excel (Exportado do Garagem 61)]", type=["xlsx"])

#     # # Carregar a aba 'Overview' do arquivo Excel
#     # df_overview = pd.read_excel(uploaded_file, sheet_name='Overview')

#     # # Extrair informações específicas
#     # car = df_overview['Car'].iloc[0]  # Supondo que a informação do carro esteja na primeira linha
#     # track = df_overview['Track'].iloc[0]  # Supondo que a informação da pista esteja na primeira linha
#     # drivers = df_overview['Driver'].dropna().unique().tolist()  # Lista de drivers únicos, removendo valores nulos

#     # # Criar o dicionário com as informações
#     # info_dict = {
#     #     'Car': car,
#     #     'Track': track,
#     #     'Drivers': drivers
#     # }

#     if uploaded_file is not None:
#         # col1.title(f":green[Análise Pós Corrida - {track}]")
#         # col1.write(f":green[Carro -{car}]")
#         # col1.write(f":green[Pilotos -{drivers}]")
#         xls = pd.ExcelFile(uploaded_file)
#         sheet_names = [sheet for sheet in xls.sheet_names if sheet != 'Overview']

#         # st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)
#         st.sidebar.header(":green[Sessões]")
        
#         selected_sheets = [sheet for sheet in sheet_names if st.sidebar.checkbox(sheet, value=True)]
        
#         if selected_sheets:
#             df_list = [pd.read_excel(xls, sheet_name=sheet).assign(SheetName=sheet) for sheet in selected_sheets]
#             final_df = pd.concat(df_list, ignore_index=True)     
#             required_columns = {'Lap time', 'Driver'}
#             if not required_columns.issubset(final_df.columns):
#                 st.error(f"O arquivo deve conter as colunas: {', '.join(required_columns)}")
#                 return

#             final_df["Lap time"] = pd.to_timedelta(final_df["Lap time"], errors='coerce')
#             final_df = final_df.dropna(subset=["Lap time"])


#         tab1, tab2 = st.tabs([":green[Lap Time]", ":green[Safety]"])
#         # Define color sets of paintings
#         gear1_colors = ['rgb(25, 128, 37)','rgb(255, 127, 0)']#verde, laranja
       
            

#         with tab1:
            
#             bin_width = st.sidebar.slider(
#                 ":green[Tamanho do Intervalo (segundos)]",
#                 min_value=0.250,
#                 max_value=1.000,
#                 value=0.500,
#                 step=0.250
#             )

#             lap_times_in_seconds = final_df["Lap time"].dt.total_seconds()
#             Q1 = lap_times_in_seconds.quantile(0.25)
#             Q3 = lap_times_in_seconds.quantile(0.75)
#             IQR = Q3 - Q1
#             filtered_df = final_df[
#                 (lap_times_in_seconds >= Q1 - 1.5 * IQR) & (lap_times_in_seconds <= Q3 + 1.5 * IQR)
#             ]

#             min_lap_time = filtered_df["Lap time"].dt.total_seconds().min()
#             max_lap_time = filtered_df["Lap time"].dt.total_seconds().max()
#             num_bins = int(np.ceil((max_lap_time - min_lap_time) / bin_width))

#             fig_all = go.Figure()
#             fig_all.add_trace(go.Histogram(
#                 x=filtered_df["Lap time"].dt.total_seconds(),
#                 nbinsx=num_bins,
#                 marker_color=gear1_colors[0],
#                 opacity=0.75
#             ))

#             tick_vals = np.arange(min_lap_time, max_lap_time + bin_width, bin_width)
#             tick_texts = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals]

#             fig_all.update_layout(
#                 title="Histograma Geral para Todos os Pilotos",
#                 xaxis_title="Tempo de Volta (MM:SS.mmm)",
#                 yaxis_title="Frequência (Nº Voltas)",
#                 xaxis=dict(
#                     tickmode='array',
#                     tickvals=tick_vals,
#                     ticktext=tick_texts,
#                     range=[min_lap_time, max_lap_time]
#                 )
#             )

#             fig_all.update_layout(bargap=0.02)#test
#             st.plotly_chart(fig_all, use_container_width=True)

#             drivers = filtered_df["Driver"].unique()

#             for driver in drivers:
#                 driver_data = filtered_df[filtered_df["Driver"] == driver]

#                 fig = make_subplots(rows=1, cols=2, shared_yaxes=True, column_widths=[0.7, 0.3])

#                 fig.add_trace(go.Histogram(
#                     x=driver_data["Lap time"].dt.total_seconds(),
#                     nbinsx=num_bins,
#                     marker_color=gear1_colors[1],
#                     opacity=0.75,
#                     name="Histograma"
#                 ), row=1, col=1)

#                 fig.add_trace(go.Box(
#                     x=driver_data["Lap time"].dt.total_seconds(),
#                     marker_color=gear1_colors[1],
#                     name="Box Plot",
#                     # boxpoints='all',
#                     # jitter=0.3,
#                     # pointpos=-1.8,
#                 ), row=1, col=2)

#                 fig.update_layout(
#                     title=f"Análise para {driver}",
#                     xaxis_title="Tempo de Volta (MM:SS.mmm)",
#                     yaxis_title="Frequência (Nº Voltas)",
#                     xaxis=dict(
#                         tickmode='array',
#                         tickvals=tick_vals,
#                         ticktext=tick_texts,
#                         range=[min_lap_time, max_lap_time]
#                     ),
#                     xaxis2=dict(
#                         tickmode='array',
#                         tickvals=tick_vals,
#                         ticktext=tick_texts,
#                         range=[min_lap_time, max_lap_time]
#                     ),
#                     showlegend=False
#                 )
#                 fig.update_xaxes(tickangle=45)
#                 fig.update_layout(bargap=0.02)
#                 st.plotly_chart(fig, use_container_width=True)

#                 # #teste setores
#                 # sector_columns = [col for col in final_df.columns if 'Sector' in col]

#         with tab2:

#             try:
#                 # Verificar se as colunas necessárias estão presentes
#                 if 'Driver' not in final_df.columns or 'Clean' not in final_df.columns:
#                     st.error("O arquivo deve conter as colunas 'Driver' e 'Clean'.")
#                     return

#                 # Agrupar por piloto e status de volta limpa/incidente
                
#                 grouped = final_df.groupby(['Driver', 'Clean']).size().reset_index(name='Counts')

#                 # Separar os dados de voltas limpas e com incidentes
#                 voltas_limpas = grouped[grouped['Clean'] == 1]
#                 voltas_com_incidentes = grouped[grouped['Clean'] == 0]

#                 # Criar o gráfico de barras empilhadas usando `go.Bar`
#                 fig = go.Figure()

#                 fig.add_trace(go.Bar(
#                     x=voltas_limpas['Driver'],
#                     y=voltas_limpas['Counts'],
#                     name='Voltas Limpas',
#                     marker_color=gear1_colors[0]  # Verde
#                 ))

#                 fig.add_trace(go.Bar(
#                     x=voltas_com_incidentes['Driver'],
#                     y=voltas_com_incidentes['Counts'],
#                     name='Voltas com Incidente',
#                     marker_color=gear1_colors[1]  # laranja
#                 ))

#                 # Configurações do layout
#                 fig.update_layout(
#                     title='Total de Voltas por Piloto',
#                     xaxis_title='Piloto',
#                     yaxis_title='Número de Voltas',
#                     barmode='stack',  # Empilhar as barras
#                     template='plotly_white'  # Estilo do gráfico
#                 )

#                 # Exibir o gráfico no Streamlit
#                 st.plotly_chart(fig)

            

#                 # Agrupar por piloto e status de volta limpa/incidente
#                 # grouped = final_df.groupby(['Driver', 'Clean']).size().unstack(fill_value=0) #pie

#                 # # Iterar sobre cada piloto e criar um gráfico de pizza
#                 # for driver in grouped.index:
#                 #     clean_counts = grouped.loc[driver]

#                 #     labels = ['Voltas Limpas','Voltas com Incidente']
#                 #     values = [clean_counts.get(1, 0), clean_counts.get(0, 0)]

                    
#                 #     fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.3, pull=[0.1,0], marker_colors=gear1_colors)])
#                 #     fig.update_layout(title_text=f'Análise de Voltas Limpas para {driver}')

#                 #     st.plotly_chart(fig)
#             except UnboundLocalError:
#                 st.write("Sem dados a exibir.")

#             # Calcular as sequências de voltas limpas
#             sequencias_df = calcular_sequencias_voltas_limpas(final_df)

#             # Determinar a maior sequência de voltas limpas por piloto
#             maiores_sequencias = sequencias_df.groupby('Driver')['Sequence_Length'].max().reset_index()

#             # Criar o gráfico de barras para as maiores sequências
#             fig_bar = go.Figure(data=[
#                 go.Bar(
#                     x=maiores_sequencias['Driver'],
#                     y=maiores_sequencias['Sequence_Length'],
#                     marker_color='rgb(15, 114, 35)',  # Verde
#                 )
#             ])

#             fig_bar.update_layout(
#                 title='Maior Sequência de Voltas Limpas por Piloto',
#                 xaxis_title='Piloto',
#                 yaxis_title='Número de Voltas Limpas Consecutivas',
#                 template='plotly_white'
#             )

#             # Exibir o gráfico de barras no Streamlit
#             st.plotly_chart(fig_bar)

#             ## feature para avaliar taxa de acidentes com o tempo de direção
#             # Ordenar o DataFrame por piloto e número da volta
#             # final_df = final_df.sort_values(by=['Driver', 'LapNumber'])

#             # # Identificar mudanças na coluna 'Clean' para detectar novas sequências
#             # final_df['New_Sequence'] = (final_df['Clean'] != final_df.groupby('Driver')['Clean'].shift(1)).astype(int)

#             # # Atribuir um identificador único para cada sequência
#             # final_df['Sequence_ID'] = final_df.groupby('Driver')['New_Sequence'].cumsum()

#             # # Calcular a diferença de tempo entre voltas consecutivas dentro de cada sequência
#             # final_df['LapTime'] = pd.to_timedelta(final_df['LapTime'])  # Converter 'LapTime' para timedelta, se ainda não estiver
#             # final_df['Time_Diff'] = final_df.groupby(['Driver', 'Sequence_ID'])['LapTime'].diff().fillna(pd.Timedelta(seconds=0))

#             # # Calcular a duração total de cada sequência
#             # sequence_durations = final_df.groupby(['Driver', 'Sequence_ID']).agg(
#             #     Total_Time=('Time_Diff', 'sum'),
#             #     Total_Laps=('LapNumber', 'count'),
#             #     Incidents=('Clean', lambda x: (x == 0).sum())
#             # ).reset_index()

#             # # Definir intervalos de tempo (em minutos)
#             # bins = [0, 5, 10, 15, 20, 25, 30, np.inf]
#             # labels = ['0-5', '5-10', '10-15', '15-20', '20-25', '25-30', '30+']

#             # # Categorizar as sequências em intervalos de tempo
#             # sequence_durations['Time_Bin'] = pd.cut(sequence_durations['Total_Time'].dt.total_seconds() / 60, bins=bins, labels=labels, right=False)

#             # # Calcular a taxa de incidentes para cada intervalo de tempo
#             # incident_rates = sequence_durations.groupby('Time_Bin').agg(
#             #     Total_Incidents=('Incidents', 'sum'),
#             #     Total_Time=('Total_Time', 'sum')
#             # ).reset_index()

#             # incident_rates['Incident_Rate'] = incident_rates['Total_Incidents'] / (incident_rates['Total_Time'].dt.total_seconds() / 3600)  # Incidentes por hora

#             # # Remover intervalos sem dados
#             # incident_rates = incident_rates.dropna()

#             # # Criar o gráfico
#             # fig = px.bar(incident_rates, x='Time_Bin', y='Incident_Rate',
#             #             title='Taxa de Incidentes por Tempo de Pilotagem Contínua',
#             #             labels={'Time_Bin': 'Tempo de Pilotagem Contínua (minutos)', 'Incident_Rate': 'Taxa de Incidentes (por hora)'},
#             #             text_auto=True)

#             # # Exibir o gráfico no Streamlit
#             # st.plotly_chart(fig)

#             # Criar histogramas para cada piloto
#             for driver in sequencias_df['Driver'].unique():
#                 driver_data = sequencias_df[sequencias_df['Driver'] == driver]

#                 fig_hist = go.Figure(data=[
#                     go.Histogram(
#                         x=driver_data['Sequence_Length'],
#                         marker_color='rgb(15, 114, 35)',  # Verde
#                         opacity=0.75
#                     )
#                 ])

#                 fig_hist.update_layout(
#                     title=f'Distribuição das Sequências de Voltas Limpas - {driver}',
#                     xaxis_title='Tamanho da Sequência de Voltas Limpas',
#                     yaxis_title='Frequência',
#                     template='plotly_white'
#                 )

#                 # Exibir o histograma no Streamlit
#                 st.plotly_chart(fig_hist)

# if __name__ == "__main__":
#     main()

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.colors as pc

def detectar_carro_pista(overview_df):
    carro = "Desconhecido"
    pista = "Desconhecida"
    n_rows, n_cols = overview_df.shape

    for row_idx in range(n_rows):
        for col_idx in range(n_cols):
            valor = str(overview_df.iat[row_idx, col_idx]).strip().lower()
            if "car" in valor:
                if row_idx + 1 < n_rows and col_idx + 1 < n_cols:
                    carro = str(overview_df.iat[row_idx + 1, col_idx + 1])
            if "track" in valor:
                if row_idx + 1 < n_rows and col_idx + 1 < n_cols:
                    pista = str(overview_df.iat[row_idx + 1, col_idx + 1])
    return carro, pista

def calcular_sequencias_voltas_limpas(df):
    sequencias = []
    for driver, group in df.groupby('Driver'):
        group['Shift'] = group['Clean'].shift(1)
        group['New_Sequence'] = (group['Clean'] != group['Shift'])
        group['Sequence_ID'] = group['New_Sequence'].cumsum()
        clean_sequences = group[group['Clean'] == 1]
        sequence_lengths = clean_sequences.groupby('Sequence_ID').size()
        for length in sequence_lengths:
            sequencias.append({'Driver': driver, 'Sequence_Length': length})
    return pd.DataFrame(sequencias)

def identificar_sequencias(df):
    sequencias = []
    for driver, group in df.groupby('Driver'):
        group = group.sort_values(by='Lap')
        group['Shift'] = group['Clean'].shift(1)
        group['New_Sequence'] = (group['Clean'] != group['Shift'])
        group['Sequence_ID'] = group['New_Sequence'].cumsum()
        sequencias.append(group)
    return pd.concat(sequencias)

def extrair_info_overview(xls):
    df_overview = pd.read_excel(xls, sheet_name='Overview', header=None)
    carro, pista = detectar_carro_pista(df_overview)
    return carro, pista

def balancear_dataset(df, metodo):
    if metodo == "Sem balanceamento":
        return df

    df_balanceado = pd.DataFrame()

    for carro in df['Car'].unique():
        df_carro = df[df['Car'] == carro]
        pilotos = df_carro['Driver'].unique()

        if metodo == "Uniforme (baseado no menor número de voltas por piloto)":
            # encontra o número mínimo de voltas entre os pilotos desse carro
            min_voltas = df_carro.groupby('Driver').size().min()
            for piloto in pilotos:
                df_piloto = df_carro[df_carro['Driver'] == piloto]
                df_balanceado = pd.concat([df_balanceado, df_piloto.sample(n=min_voltas, replace=False,random_state=42)], ignore_index=True)

        elif metodo == "Mediana":
            median_voltas = int(df_carro.groupby('Driver').size().median())
            for piloto in pilotos:
                df_piloto = df_carro[df_carro['Driver'] == piloto]
                if len(df_piloto) >= median_voltas:
                    df_balanceado = pd.concat([df_balanceado, df_piloto.sample(n=median_voltas, replace=False,random_state=42)], ignore_index=True)
                else:
                    df_balanceado = pd.concat([df_balanceado, df_piloto], ignore_index=True)

        elif metodo == "Mínimo":
            min_voltas = df_carro.groupby('Driver').size().min()
            for piloto in pilotos:
                df_piloto = df_carro[df_carro['Driver'] == piloto]
                df_balanceado = pd.concat([df_balanceado, df_piloto.sample(n=min_voltas, replace=False,random_state=42)], ignore_index=True)

    return df_balanceado

def main():
    st.set_page_config(page_title="Gear 1 Post Race", page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", layout="wide")
    st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)
    st.title(":green[Análise Pós Evento]")

    uploaded_files = st.sidebar.file_uploader(":green[Escolha um ou mais arquivos Excel]", type=["xlsx"], accept_multiple_files=True)

    if uploaded_files:
        all_dfs = []
        info_arquivos = []

        for uploaded_file in uploaded_files:
            xls = pd.ExcelFile(uploaded_file)
            carro, pista = extrair_info_overview(xls)
            sheet_names = [sheet for sheet in xls.sheet_names if sheet != 'Overview']
            info_arquivos.append(f"Arquivo: {uploaded_file.name} | Carro: {carro} | Pista: {pista}")
            for sheet in sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                df['SheetName'] = sheet
                df['Car'] = carro
                df['Track'] = pista
                all_dfs.append(df)

        # Etapa 2: Concatenar os dados
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df_c=final_df.copy()

        # Etapa 3: Seleção via multiselect
        sheet_options = final_df['SheetName'].unique().tolist()
        selected_sheets = st.sidebar.multiselect("Selecionar as Sessões", options=sheet_options, default=sheet_options)

        # Etapa 4: Filtrar o DataFrame com base na seleção
        final_df = final_df[final_df['SheetName'].isin(selected_sheets)]

        if 'Lap time' in final_df.columns:
            final_df['Lap time'] = pd.to_timedelta(final_df['Lap time'], errors='coerce')
        if 'Started at' in final_df.columns:
            final_df['Started at'] = pd.to_datetime(final_df['Started at'], errors='coerce')


        tipo_analise = st.sidebar.selectbox("Tipo de Análise", ["Por Piloto", "Por Carro"])
        if tipo_analise == "Por Carro":
            pista_sel = st.sidebar.selectbox("Escolha a Pista", sorted(final_df['Track'].dropna().unique()))
            df_filtrado = final_df[(final_df['Track'] == pista_sel)]
        elif tipo_analise == "Por Piloto":
            pista_sel = st.sidebar.selectbox("Escolha a Pista", sorted(final_df['Track'].dropna().unique()))
            df_filtrado = final_df[final_df['Track'] == pista_sel]
            carro_sel = st.sidebar.selectbox("Escolha o Carro", sorted(df_filtrado['Car'].dropna().unique()))
            df_filtrado = final_df[(final_df['Car'] == carro_sel) & (final_df['Track'] == pista_sel)]

        balanceamento_opcao = st.sidebar.selectbox(
            "Tipo de Balanceamento",
            ["Sem balanceamento", 
            "Uniforme (baseado no menor número de voltas por piloto)", 
            "Mediana", 
            "Mínimo"]
        )

        # criterio = st.sidebar.selectbox("Critério de Balanceamento:", ["min", "mediana"])
        # cutoff = st.sidebar.slider("Corte mínimo de voltas por piloto:", 0, 50, 0, step=5)

        
        df_filtrado  = balancear_dataset(df_filtrado , balanceamento_opcao)

        #teste
        final_df=df_filtrado

        tab1, tab2, tab3, tab4, tab5 = st.tabs([":green[Lap Time]", ":green[Safety]", ":green[Fuel]", ":green[Temperature]",":orange[Files]"])
        # Define color sets of paintings
        gear1_colors = ['rgb(25, 128, 37)','rgb(255, 127, 0)']#verde, laranja

        with tab5:
            st.subheader("Arquivos Carregados:")
            for info in info_arquivos:
                st.text(info)

        with tab1:
            bin_width = st.sidebar.slider(
                ":green[Tamanho do Intervalo (segundos)]",
                min_value=0.250,
                max_value=1.000,
                value=0.500,
                step=0.250
            )

            lap_times_in_seconds = final_df["Lap time"].dt.total_seconds()
            Q1 = lap_times_in_seconds.quantile(0.25)
            Q3 = lap_times_in_seconds.quantile(0.75)
            IQR = Q3 - Q1
            filtered_df = final_df[
                (lap_times_in_seconds >= Q1 - 1.5 * IQR) & (lap_times_in_seconds <= Q3 + 1.5 * IQR)
            ]

            min_lap_time = filtered_df["Lap time"].dt.total_seconds().min()
            max_lap_time = filtered_df["Lap time"].dt.total_seconds().max()
            num_bins = int(np.ceil((max_lap_time - min_lap_time) / bin_width))

            tick_vals = np.arange(min_lap_time, max_lap_time + bin_width, bin_width)
            tick_texts = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals]

            drivers = filtered_df["Driver"].unique()
            cars = filtered_df["Car"].unique()

            # Agrupar por Carro e contar o número de voltas - Primeiro Grafico de pizza
            voltas_por_carro = filtered_df.groupby("Car").size().reset_index(name="Voltas")

            # Criar o gráfico de pizza
            fig_pizza_carro = px.pie(
                voltas_por_carro,
                names="Car",
                values="Voltas",
                title="Distribuição de Voltas por Carro",
                hole=0.4  # opcional: transforma em gráfico de rosca
            )

            # Exibir apenas o percentual
            fig_pizza_carro.update_traces(textinfo='percent+value')

            # Mostrar o gráfico
            st.plotly_chart(fig_pizza_carro, use_container_width=True)


            # Agrupa as voltas por Carro e Piloto
            voltas_por_piloto_carro = (
                filtered_df.groupby(["Car", "Driver"])
                .size()
                .reset_index(name="Voltas")
            )

            # Lista de drivers únicos
            drivers_unicos = sorted(filtered_df["Driver"].unique())

            # Gera cores distintas (usando a paleta Plotly, com repetição se necessário)
            cores = pc.qualitative.Plotly * (len(drivers_unicos) // len(pc.qualitative.Plotly) + 1)

            # Cria o dicionário com o mapeamento driver → cor
            mapa_cores = {driver: cor for driver, cor in zip(drivers_unicos, cores)}

           # Lista de carros únicos
            carros_unicos = voltas_por_piloto_carro["Car"].unique()

            # Número de colunas por linha (ajuste conforme desejar)
            colunas_por_linha = 3

            # Divide os carros em grupos para exibição por linha
            for i in range(0, len(carros_unicos), colunas_por_linha):
                cols = st.columns(colunas_por_linha)
                for j, carro in enumerate(carros_unicos[i:i + colunas_por_linha]):
                    with cols[j]:
                        dados_carro = voltas_por_piloto_carro[voltas_por_piloto_carro["Car"] == carro]
                        fig = px.pie(
                            dados_carro,
                            names="Driver",
                            values="Voltas",
                            title=f"Distribuição de Voltas - {carro}",
                            color="Driver",  # Usa a cor mapeada
                            color_discrete_map=mapa_cores,
                        )
                        fig.update_traces(textinfo='percent')
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)


            #### Gráfico de stints ###
            df_valid = df_filtrado.dropna(subset=['Run', 'Driver'])
            stint_counts = df_valid.groupby(['Driver', 'Run']).size().reset_index(name='Voltas')
            stint_counts['Run'] = stint_counts['Run'].astype(str)

            fig = px.bar(
                stint_counts,
                x='Run',
                y='Voltas',
                color='Driver',
                barmode='group',
                category_orders={'Run': sorted(stint_counts['Run'].unique(), key=lambda x: int(x))},
                title='Quantidade de Voltas por Stint (Run) por Piloto',
                color_discrete_map=mapa_cores,
            )
            fig.update_layout(xaxis_title='Stint (Run)', yaxis_title='Quantidade de Voltas', height=350)
            st.plotly_chart(fig, use_container_width=True)

            #### Gráficos principais: Histograma e Boxplot ####
            fig_combined = make_subplots(
                rows=2, cols=1,
                shared_xaxes=False,
                row_heights=[0.5, 0.5],
                vertical_spacing=0.20,
                subplot_titles=(
                    "Histograma Geral",
                    "Boxplot"
                )
            )

            agrupador = "Driver" if tipo_analise == "Por Piloto" else "Car"
            cores_hist = gear1_colors[0]
            cores_box = gear1_colors[1]

            fig_combined.add_trace(
                go.Histogram(
                    x=filtered_df["Lap time"].dt.total_seconds(),
                    nbinsx=num_bins,
                    marker_color=cores_hist,
                    opacity=0.75,
                    name="Histograma"
                ),
                row=1, col=1
            )

            for grupo in filtered_df[agrupador].unique():
                grupo_laps = filtered_df[filtered_df[agrupador] == grupo]["Lap time"].dt.total_seconds()
                fig_combined.add_trace(
                    go.Box(
                        y=grupo_laps,
                        name=grupo,
                        boxpoints='outliers',
                        marker_color=cores_box,
                        boxmean=True
                    ),
                    row=2, col=1
                )

            boxplot_min = filtered_df["Lap time"].dt.total_seconds().min()
            boxplot_max = filtered_df["Lap time"].dt.total_seconds().max()
            boxplot_step = bin_width

            tick_vals_y = np.arange(boxplot_min, boxplot_max + boxplot_step, boxplot_step)
            tick_text_y = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals_y]

            fig_combined.update_layout(
                height=800,
                xaxis_title="Tempo de Volta (MM:SS.mmm)",
                yaxis_title="Frequência (Nº Voltas)",
                xaxis=dict(tickmode='array', tickvals=tick_vals, ticktext=tick_texts, range=[min_lap_time, max_lap_time]),
                yaxis2=dict(title="Tempo de Volta", tickmode='array', tickvals=tick_vals_y, ticktext=tick_text_y),
                bargap=0.02,
                showlegend=False
            )
            fig_combined.update_xaxes(tickangle=45)
            st.plotly_chart(fig_combined, use_container_width=True)

            ### Adiciona histogramas por Carro e Piloto (facetado) ###
            fig_combo = px.histogram(
                filtered_df,
                x=filtered_df["Lap time"].dt.total_seconds(),
                color="Driver",
                facet_col="Car",
                nbins=num_bins,
                title="Histograma de Tempo de Volta por Piloto e Carro",
                color_discrete_map=mapa_cores,
            )

            # Geração dos ticks formatados
            min_lap_time = filtered_df["Lap time"].dt.total_seconds().min()
            max_lap_time = filtered_df["Lap time"].dt.total_seconds().max()
            tick_vals = np.arange(min_lap_time, max_lap_time + bin_width, bin_width)
            tick_texts = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals]

            # Aplica os ticks formatados a todos os eixos X
            for axis in fig_combo.layout:
                if axis.startswith("xaxis"):
                    fig_combo.layout[axis].update(
                        tickmode='array',
                        tickvals=tick_vals,
                        ticktext=tick_texts,
                        title="Tempo de Volta (MM:SS.mmm)"
                    )

            # Altura e espaçamento
            fig_combo.update_layout(
                height=500,
                grid_xgap=0.5
            )

            st.plotly_chart(fig_combo, use_container_width=True)

            

            ### Gráficos individuais por grupo (piloto ou carro) ###
            for grupo in filtered_df[agrupador].unique():
                grupo_data = filtered_df[filtered_df[agrupador] == grupo]

                fig = make_subplots(rows=1, cols=2, shared_yaxes=True, column_widths=[0.7, 0.3])

                fig.add_trace(go.Histogram(
                    x=grupo_data["Lap time"].dt.total_seconds(),
                    nbinsx=num_bins,
                    marker_color=cores_box,
                    opacity=0.75,
                    name="Histograma"
                ), row=1, col=1)

                fig.add_trace(go.Box(
                    x=grupo_data["Lap time"].dt.total_seconds(),
                    marker_color=cores_box,
                    name="Box Plot",
                    boxmean=True,
                ), row=1, col=2)

                fig.update_layout(
                    title=f"Análise para {grupo}",
                    xaxis_title="Tempo de Volta (MM:SS.mmm)",
                    yaxis_title="Frequência (Nº Voltas)",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=tick_vals,
                        ticktext=tick_texts,
                        range=[min_lap_time, max_lap_time]
                    ),
                    xaxis2=dict(
                        tickmode='array',
                        tickvals=tick_vals,
                        ticktext=tick_texts,
                        range=[min_lap_time, max_lap_time]
                    ),
                    showlegend=False,
                    bargap=0.02
                )
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
        with tab4:
            # st.subheader("Temperatura ao longo do tempo")
            col1, col2 = st.columns(2)

            # Gráficos de linha
            if 'Track temp' in df_filtrado.columns:
                fig_track_temp = px.line(df_filtrado, x='Started at', y='Track temp', title='Temperatura da Pista')
                fig_track_temp.update_traces(line_color=gear1_colors[0])
                col1.plotly_chart(fig_track_temp, use_container_width=True)

            if 'Air temperature' in df_filtrado.columns:
                fig_air_temp = px.line(df_filtrado, x='Started at', y='Air temperature', title='Temperatura do Ar')
                fig_air_temp.update_traces(line_color=gear1_colors[1])
                col2.plotly_chart(fig_air_temp, use_container_width=True)

            # Linha abaixo com histogramas e box plots
            # st.markdown("### Distribuições de Temperatura")

            col3, col4 = st.columns(2)

            # Histogramas e Box plots
            if 'Track temp' in df_filtrado.columns:
                with col3:
                    st.plotly_chart(
                        px.box(df_filtrado, y='Track temp', title='Box Plot - Temperatura da Pista',
                            color_discrete_sequence=[gear1_colors[0]]),
                        use_container_width=True
                    )

            if 'Air temperature' in df_filtrado.columns:
                with col4:
                    st.plotly_chart(
                        px.box(df_filtrado, y='Air temperature', title='Box Plot - Temperatura do Ar',
                            color_discrete_sequence=[gear1_colors[1]]),
                        use_container_width=True
                    )

            if 'Track temp' in df_filtrado.columns:
                # st.subheader("Análise da Temperatura da Pista")
                grupo_col = "Driver" if tipo_analise == "Por Piloto" else "Car"
                # Histograma
                fig_hist_track = go.Figure()
                fig_hist_track.add_trace(go.Histogram(
                    x=df_filtrado["Track temp"],
                    nbinsx=30,
                    marker_color=gear1_colors[0],
                    opacity=0.75,
                    name="Temperatura da Pista"
                ))
                fig_hist_track.update_layout(
                    title_text="Histograma da Temperatura da Pista",
                    xaxis_title="Track temp (°C)",
                    yaxis_title="Frequência"
                )
                st.plotly_chart(fig_hist_track, use_container_width=True)

                # Boxplot
                q1_track = df_filtrado.groupby(grupo_col)['Track temp'].quantile(0.25)
                q3_track = df_filtrado.groupby(grupo_col)['Track temp'].quantile(0.75)
                iqr_track = q3_track - q1_track
                lower_bounds_track = q1_track - 1.5 * iqr_track
                upper_bounds_track = q3_track + 1.5 * iqr_track
                min_track = lower_bounds_track.min()
                max_track = upper_bounds_track.max()

                fig_box_track = go.Figure()
                fig_box_track.add_trace(go.Box(
                    y=df_filtrado["Track temp"],
                    x=df_filtrado[grupo_col],
                    boxpoints="outliers",
                    marker_color=gear1_colors[0],
                    orientation='v',
                    boxmean=True
                ))
                fig_box_track.update_layout(
                    title_text=f"Boxplot da Temperatura da Pista por {'Piloto' if tipo_analise == 'Por Piloto' else 'Carro'}",
                    xaxis_title="Piloto" if tipo_analise == "Por Piloto" else "Carro",
                    yaxis_title="Temperatura da Pista (°C)",
                    yaxis=dict(range=[min_track, max_track]),
                    margin=dict(l=40, r=40, t=60, b=120),
                    showlegend=False
                )
                st.plotly_chart(fig_box_track, use_container_width=True)

            # Garantir que o tempo de volta está em segundos
                df_filtrado['Lap time (s)'] = df_filtrado['Lap time'].dt.total_seconds()

                # Remover outliers com base no tempo de volta
                q1 = df_filtrado['Lap time (s)'].quantile(0.25)
                q3 = df_filtrado['Lap time (s)'].quantile(0.75)
                iqr = q3 - q1
                filtro = (df_filtrado['Lap time (s)'] >= q1 - 1.5 * iqr) & (df_filtrado['Lap time (s)'] <= q3 + 1.5 * iqr)
                df_filtrado_sem_outliers = df_filtrado[filtro]

                # Função para formatar o tempo em MM:SS.mmm
                def format_lap_time(sec):
                    minutes = int(sec // 60)
                    seconds = sec % 60
                    return f"{minutes:02}:{seconds:06.3f}"

                # Gráfico de dispersão: Temperatura da pista (x) vs Tempo de volta (y)
                fig_temp = go.Figure()

                for grupo in df_filtrado_sem_outliers[grupo_col].unique():
                    df_grupo = df_filtrado_sem_outliers[df_filtrado_sem_outliers[grupo_col] == grupo]
                    fig_temp.add_trace(go.Scatter(
                        x=df_grupo['Track temp'],
                        y=df_grupo['Lap time (s)'],
                        mode='markers',
                        name=grupo,
                        marker=dict(size=8),
                        hovertemplate=(
                            f"<b>{grupo}</b><br>"
                            "Air temp: %{x}°C<br>"
                            "Lap time: %{customdata}<extra></extra>"
                        ),
                        customdata=[format_lap_time(t) for t in df_grupo['Lap time (s)']]
                    ))

                # Ajustar layout
                fig_temp.update_layout(
                    title="Temperatura da Pista vs Tempo de Volta",
                    xaxis_title="Temperatura da Pista (°C)",
                    yaxis_title="Tempo de Volta (MM:SS.mmm)",
                    legend_title="Piloto" if tipo_analise == "Por Piloto" else "Carro",
                    margin=dict(l=40, r=40, t=60, b=40)
                )

                # Formatando os ticks do eixo Y (tempo de volta)
                yticks = np.linspace(df_filtrado_sem_outliers['Lap time (s)'].min(),
                                    df_filtrado_sem_outliers['Lap time (s)'].max(), 6)

                fig_temp.update_yaxes(
                    tickvals=yticks,
                    ticktext=[format_lap_time(v) for v in yticks]
                )

                st.plotly_chart(fig_temp, use_container_width=True)

            if 'Air temperature' in df_filtrado.columns:
                # st.subheader("Análise da Temperatura do Ar")
                # tipo_analise = st.selectbox("Tipo de análise", ["Por Piloto", "Por Carro"])
                grupo_col = "Driver" if tipo_analise == "Por Piloto" else "Car"
                # Histograma
                fig_hist_air = go.Figure()
                fig_hist_air.add_trace(go.Histogram(
                    x=df_filtrado["Air temperature"],
                    nbinsx=30,
                    marker_color=gear1_colors[1],
                    opacity=0.75,
                    name="Temperatura do Ar"
                ))
                fig_hist_air.update_layout(
                    title_text="Histograma da Temperatura do Ar",
                    xaxis_title="Air temperature (°C)",
                    yaxis_title="Frequência"
                )
                st.plotly_chart(fig_hist_air, use_container_width=True)

                # Boxplot
                q1_air = df_filtrado.groupby(grupo_col)['Air temperature'].quantile(0.25)
                q3_air = df_filtrado.groupby(grupo_col)['Air temperature'].quantile(0.75)

                iqr_air = q3_air - q1_air
                lower_bounds_air = q1_air - 1.5 * iqr_air
                upper_bounds_air = q3_air + 1.5 * iqr_air
                min_air = lower_bounds_air.min()
                max_air = upper_bounds_air.max()

                fig_box_air = go.Figure()
                fig_box_air.add_trace(go.Box(
                    y=df_filtrado["Air temperature"],
                    x=df_filtrado[grupo_col],
                    boxpoints="outliers",
                    marker_color=gear1_colors[1],
                    orientation='v',
                    boxmean=True
                ))
                fig_box_air.update_layout(
                    title_text=f"Boxplot da Temperatura do Ar por {'Piloto' if tipo_analise == 'Por Piloto' else 'Carro'}",
                    xaxis_title="Piloto" if tipo_analise == "Por Piloto" else "Carro",
                    yaxis_title="Temperatura do Ar (°C)",
                    yaxis=dict(range=[min_air, max_air]),
                    margin=dict(l=40, r=40, t=60, b=120),
                    showlegend=False
                )
                st.plotly_chart(fig_box_air, use_container_width=True)

                q1 = df_filtrado['Lap time (s)'].quantile(0.25)
                q3 = df_filtrado['Lap time (s)'].quantile(0.75)
                iqr = q3 - q1
                filtro = (df_filtrado['Lap time (s)'] >= q1 - 1.5 * iqr) & (df_filtrado['Lap time (s)'] <= q3 + 1.5 * iqr)
                df_filtrado_sem_outliers = df_filtrado[filtro]

                # Função para formatar o tempo em MM:SS.mmm
                def format_lap_time(sec):
                    minutes = int(sec // 60)
                    seconds = sec % 60
                    return f"{minutes:02}:{seconds:06.3f}"
                


                # Gráfico de dispersão: Temperatura do ar (x) vs Tempo de volta (y)
                fig_air_temp = go.Figure()

                for grupo in df_filtrado_sem_outliers[grupo_col].unique():
                    df_grupo = df_filtrado_sem_outliers[df_filtrado_sem_outliers[grupo_col] == grupo]
                    fig_air_temp.add_trace(go.Scatter(
                        x=df_grupo['Air temperature'],
                        y=df_grupo['Lap time (s)'],
                        mode='markers',
                        name=grupo,
                        marker=dict(size=8),
                        hovertemplate=(
                            f"<b>{grupo}</b><br>"
                            "Air temp: %{x}°C<br>"
                            "Lap time: %{customdata}<extra></extra>"
                        ),
                        customdata=[format_lap_time(t) for t in df_grupo['Lap time (s)']]
                    ))

                # Layout do gráfico
                fig_air_temp.update_layout(
                    title="Temperatura do Ar vs Tempo de Volta",
                    xaxis_title="Temperatura do Ar (°C)",
                    yaxis_title="Tempo de Volta (MM:SS.mmm)",
                    legend_title="Piloto" if tipo_analise == "Por Piloto" else "Carro",
                    margin=dict(l=40, r=40, t=60, b=40)
                )

                # Eixo Y formatado
                yticks = np.linspace(df_filtrado_sem_outliers['Lap time (s)'].min(),
                                    df_filtrado_sem_outliers['Lap time (s)'].max(), 6)

                fig_air_temp.update_yaxes(
                    tickvals=yticks,
                    ticktext=[format_lap_time(v) for v in yticks]
                )

                st.plotly_chart(fig_air_temp, use_container_width=True)

        with tab3:

                if 'Fuel used' in df_filtrado.columns:

                    #  tipo_analise = st.sidebar.selectbox("Tipo de Análise", ["Por Piloto", "Por Carro"])

                    agrupador = 'Driver' if tipo_analise  == 'Por Piloto' else 'Car'

                    # Histograma do Consumo
                    fig_hist = go.Figure()
                    fig_hist.add_trace(go.Histogram(
                        x=df_filtrado["Fuel used"],
                        nbinsx=30,
                        marker_color=gear1_colors[0],
                        opacity=0.75,
                        name=f"Consumo por Volta ({tipo_analise})"
                    ))
                    fig_hist.update_layout(
                        title_text=f"Histograma do Consumo por Volta ({tipo_analise})",
                        xaxis_title="Fuel Used (L)",
                        yaxis_title="Frequência"
                    )
                    st.plotly_chart(fig_hist, use_container_width=True)

                    # Calcular Q1, Q3 e IQR para cada grupo
                    q1 = df_filtrado.groupby(agrupador)['Fuel used'].quantile(0.25)
                    q3 = df_filtrado.groupby(agrupador)['Fuel used'].quantile(0.75)
                    iqr = q3 - q1

                    # Limites sem outliers
                    lower_bounds = q1 - 1.5 * iqr
                    upper_bounds = q3 + 1.5 * iqr

                    min_fuel = lower_bounds.min()
                    max_fuel = upper_bounds.max()

                    # Boxplot vertical por agrupador
                    fig_box_vertical = go.Figure()
                    fig_box_vertical.add_trace(go.Box(
                        y=df_filtrado["Fuel used"],
                        x=df_filtrado[agrupador],
                        boxpoints="outliers",
                        marker_color=gear1_colors[1],
                        orientation='v',
                        boxmean=True
                    ))

                    fig_box_vertical.update_layout(
                        title_text=f"Boxplot do Consumo de Combustível por {agrupador}",
                        xaxis_title=agrupador,
                        yaxis_title="Consumo (L)",
                        yaxis=dict(range=[min_fuel, max_fuel]),
                        margin=dict(l=40, r=40, t=60, b=120),
                        showlegend=False
                    )
                    st.plotly_chart(fig_box_vertical, use_container_width=True)

                    # Garantir que o tempo de volta está em segundos
                    df_filtrado['Lap time (s)'] = df_filtrado['Lap time'].dt.total_seconds()

                    # Remover outliers do tempo
                    q1 = df_filtrado['Lap time (s)'].quantile(0.25)
                    q3 = df_filtrado['Lap time (s)'].quantile(0.75)
                    iqr = q3 - q1
                    filtro = (df_filtrado['Lap time (s)'] >= q1 - 1.5 * iqr) & (df_filtrado['Lap time (s)'] <= q3 + 1.5 * iqr)
                    df_filtrado_sem_outliers = df_filtrado[filtro]

                    def format_lap_time(sec):
                        minutes = int(sec // 60)
                        seconds = sec % 60
                        return f"{minutes:02}:{seconds:06.3f}"

                    # Gráfico de dispersão Fuel vs Lap Time
                    fig_scatter = go.Figure()

                    for grupo in df_filtrado_sem_outliers[agrupador].unique():
                        df_grupo = df_filtrado_sem_outliers[df_filtrado_sem_outliers[agrupador] == grupo]
                        fig_scatter.add_trace(go.Scatter(
                            x=df_grupo['Fuel used'],
                            y=df_grupo['Lap time (s)'],
                            mode='markers',
                            name=grupo,
                            marker=dict(size=8),
                            hovertemplate=(
                                f"<b>{grupo}</b><br>"
                                "Fuel used: %{x:.2f} L<br>"
                                "Lap time: %{customdata}<extra></extra>"
                            ),
                            customdata=[format_lap_time(x) for x in df_grupo['Lap time (s)']]
                        ))

                    yticks = np.linspace(
                        df_filtrado_sem_outliers['Lap time (s)'].min(),
                        df_filtrado_sem_outliers['Lap time (s)'].max(),
                        6
                    )

                    fig_scatter.update_layout(
                        title=f"Consumo vs Tempo de Volta por {agrupador}",
                        xaxis_title="Consumo de Combustível (L)",
                        yaxis_title="Tempo de Volta (MM:SS.mmm)",
                        legend_title=agrupador,
                        margin=dict(l=40, r=40, t=60, b=40)
                    )
                    fig_scatter.update_yaxes(
                        tickvals=yticks,
                        ticktext=[format_lap_time(v) for v in yticks]
                    )

                    st.plotly_chart(fig_scatter, use_container_width=True)

                    #Voltas por tanque   
                    # Garantir ordem correta

                    # Exemplo: tipo_analise = st.selectbox("Tipo de análise", ["Por Carro", "Por Piloto"])

                    df_fuel_sorted = df_filtrado.sort_values(by=["Driver", "Car", "Lap"])

                    # Calcular consumo por volta (diferencial invertido do nível de combustível)
                    df_fuel_sorted["Fuel used"] = df_fuel_sorted.groupby(["Driver", "Car"])["Fuel level"].diff(-1)

                    # Filtrar valores inválidos
                    df_fuel_clean = df_fuel_sorted.dropna(subset=["Fuel used"])
                    df_fuel_clean = df_fuel_clean[(df_fuel_clean["Fuel used"] > 0.1)]  # descartar valores baixos

                    # Remover outliers usando IQR
                    Q1 = df_fuel_clean["Fuel used"].quantile(0.25)
                    Q3 = df_fuel_clean["Fuel used"].quantile(0.75)
                    IQR = Q3 - Q1
                    fuel_min = Q1 - 1.5 * IQR
                    fuel_max = Q3 + 1.5 * IQR

                    df_fuel_filtered = df_fuel_clean[(df_fuel_clean["Fuel used"] >= fuel_min) & (df_fuel_clean["Fuel used"] <= fuel_max)]

                    if tipo_analise == "Por Piloto":
                        df_fuel_filtered["Label"] = df_fuel_filtered["Driver"]
                    else:
                        df_fuel_filtered["Label"] = df_fuel_filtered["Car"]


                    # Obter capacidade do tanque (máximo Fuel Level quando Lap == 0), por Carro
                    tank_capacity = final_df_c[final_df_c["Lap"] == 0].groupby("Car")["Fuel level"].max()

                    # Mapear capacidade do tanque para cada linha
                    df_fuel_filtered["Tank_Capacity"] = df_fuel_filtered["Car"].map(tank_capacity)

                    # Estimar voltas por tanque
                    df_fuel_filtered["Estimated_Laps"] = df_fuel_filtered["Tank_Capacity"] / df_fuel_filtered["Fuel used"]

                    # Remover valores absurdos (só por segurança, ex: > 100)
                    df_fuel_filtered = df_fuel_filtered[df_fuel_filtered["Estimated_Laps"] < 100]

                    # Criar gráfico
                    # Obter os grupos únicos
                    grupos = df_fuel_filtered["Label"].unique()

                    # Criar traços para cada grupo
                    fig = go.Figure()

                    for grupo in grupos:
                        dados = df_fuel_filtered[df_fuel_filtered["Label"] == grupo]["Estimated_Laps"]

                        fig.add_trace(go.Box(
                            y=dados,
                            name=str(grupo),
                            boxpoints="outliers",
                            marker_color=None,
                            boxmean=True,  # mostra a média 
                        ))

                    fig.update_layout(
                        title="Distribuição Estimada de Voltas por Tanque",
                        xaxis_title="Piloto" if tipo_analise == "Por Piloto" else "Carro",
                        yaxis_title="Voltas Estimadas por Tanque"
                    )

                    st.plotly_chart(fig, use_container_width=True)
                
                    #Funcionalidade para verificação de paradas pela duração da prova
                    # Entrada do usuário para a duração da prova
                    duracao_prova_horas = st.number_input("Duração da prova (em horas)", min_value=0.5, max_value=24.0, value=1.0, step=0.5)

                    # Estimar tempo médio de volta em segundos
                    df_fuel_filtered["LapTimeSeconds"] = df_fuel_filtered["Lap time"].dt.total_seconds()
                    tempo_medio_volta = df_fuel_filtered["LapTimeSeconds"].mean()

                    # Estimar número total de voltas
                    voltas_estimadas = (duracao_prova_horas * 3600) / tempo_medio_volta

                    # Estimar consumo total em litros para a duração informada
                    df_fuel_filtered["Consumo_Estimado_Prova_Litros"] = df_fuel_filtered["Fuel used"] * voltas_estimadas

                    # Estimar número de paradas
                    df_fuel_filtered["Paradas_Estimadas"] = df_fuel_filtered["Consumo_Estimado_Prova_Litros"] / df_fuel_filtered["Tank_Capacity"]

                    # Exibir boxplot do consumo estimado total
                    st.markdown("### Distribuição de Consumo Estimado (Litros)")
                    fig_consumo = go.Figure()

                    for label in df_fuel_filtered["Label"].unique():
                        grupo = df_fuel_filtered[df_fuel_filtered["Label"] == label]
                        fig_consumo.add_trace(go.Box(
                            y=grupo["Consumo_Estimado_Prova_Litros"],
                            name=label,
                            boxpoints="outliers",  # mostra os pontos individuais
                            marker_color=None,
                            boxmean=True,  # mostra a média 
                        ))

                    fig_consumo.update_layout(
                        yaxis_title="Consumo (L)",
                        # boxmode="group"
                    )
                    st.plotly_chart(fig_consumo, use_container_width=True)


                    # Distribuição Estimada de Paradas
                    st.markdown("### Distribuição Estimada de Paradas")
                    fig_paradas = go.Figure()

                    for label in df_fuel_filtered["Label"].unique():
                        grupo = df_fuel_filtered[df_fuel_filtered["Label"] == label]
                        fig_paradas.add_trace(go.Box(
                            y=grupo["Paradas_Estimadas"],
                            name=label,
                            boxpoints="outliers",
                            marker_color=None,
                            boxmean=True,  # mostra a média 
                        ))

                    fig_paradas.update_layout(
                        yaxis_title="Nº de Paradas",
                        # boxmode="group"
                    )
                    st.plotly_chart(fig_paradas, use_container_width=True)

        with tab2:

            try:
                # Verificar se as colunas necessárias estão presentes
                if 'Driver' not in final_df.columns or 'Clean' not in final_df.columns:
                    st.error("O arquivo deve conter as colunas 'Driver' e 'Clean'.")
                    return

                # Agrupar por piloto e status de volta limpa/incidente
                
                grouped = final_df.groupby(['Driver', 'Clean']).size().reset_index(name='Counts')

                # Separar os dados de voltas limpas e com incidentes
                voltas_limpas = grouped[grouped['Clean'] == 1]
                voltas_com_incidentes = grouped[grouped['Clean'] == 0]

                # Criar o gráfico de barras empilhadas usando `go.Bar`
                fig = go.Figure()

                fig.add_trace(go.Bar(
                    x=voltas_limpas['Driver'],
                    y=voltas_limpas['Counts'],
                    name='Voltas Limpas',
                    marker_color=gear1_colors[0]  # Verde
                ))

                fig.add_trace(go.Bar(
                    x=voltas_com_incidentes['Driver'],
                    y=voltas_com_incidentes['Counts'],
                    name='Voltas com Incidente',
                    marker_color=gear1_colors[1]  # laranja
                ))

                # Configurações do layout
                fig.update_layout(
                    title='Total de Voltas por Piloto',
                    xaxis_title='Piloto',
                    yaxis_title='Número de Voltas',
                    # barmode='stack',  # Empilhar as barras
                    barmode='group', #Lado a lado
                    template='plotly_white'  # Estilo do gráfico
                )

                # Exibir o gráfico no Streamlit
                st.plotly_chart(fig)

                # Totais gerais
                total_limpas = voltas_limpas['Counts'].sum()
                total_incidentes = voltas_com_incidentes['Counts'].sum()

                # Gráfico de pizza separado
                fig_pizza = go.Figure(
                    data=[go.Pie(
                        labels=['Voltas Limpas', 'Voltas com Incidente'],
                        values=[total_limpas, total_incidentes],
                        marker=dict(colors=[gear1_colors[0], gear1_colors[1]]),
                        textinfo='label+percent',
                        hole=0.4  # Estilo donut, opcional
                    )]
                )

                fig_pizza.update_layout(title='Voltas Limpas vs com Incidente')
                # Exibir o gráfico no Streamlit
                st.plotly_chart(fig_pizza)

                # Criação das pizza por piloto
                pilotos = grouped['Driver'].unique()

                for piloto in pilotos:
                    dados_piloto = grouped[grouped['Driver'] == piloto]

                    # Garantir que existam as duas categorias
                    clean_count = int(dados_piloto[dados_piloto['Clean'] == 1]['Counts'].values[0]) if 1 in dados_piloto['Clean'].values else 0
                    incident_count = int(dados_piloto[dados_piloto['Clean'] == 0]['Counts'].values[0]) if 0 in dados_piloto['Clean'].values else 0

                    fig_pizza2 = go.Figure(data=[go.Pie(
                        labels=['Voltas Limpas', 'Voltas com Incidente'],
                        values=[clean_count, incident_count],
                        marker_colors=[gear1_colors[0], gear1_colors[1]],
                        hole=0.3,
                        textinfo='label+percent',
                    )])

                    fig_pizza2.update_layout(title=f'Voltas de {piloto}')
                    st.plotly_chart(fig_pizza2, use_container_width=True)

            except UnboundLocalError:
                st.write("Sem dados a exibir.")

            # Calcular as sequências de voltas limpas
            sequencias_df = calcular_sequencias_voltas_limpas(final_df)

            # Determinar a maior sequência de voltas limpas por piloto
            maiores_sequencias = sequencias_df.groupby('Driver')['Sequence_Length'].max().reset_index()

            # Criar o gráfico de barras para as maiores sequências
            fig_bar = go.Figure(data=[
                go.Bar(
                    x=maiores_sequencias['Driver'],
                    y=maiores_sequencias['Sequence_Length'],
                    marker_color='rgb(15, 114, 35)',  # Verde
                )
            ])

            fig_bar.update_layout(
                title='Maior Sequência de Voltas Limpas por Piloto',
                xaxis_title='Piloto',
                yaxis_title='Número de Voltas Limpas Consecutivas',
                template='plotly_white'
            )


if __name__ == "__main__":
    main()

