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
                
        final_df = pd.concat(all_dfs, ignore_index=True)

        if 'Lap time' in final_df.columns:
            final_df['Lap time'] = pd.to_timedelta(final_df['Lap time'], errors='coerce')
        if 'Started at' in final_df.columns:
            final_df['Started at'] = pd.to_datetime(final_df['Started at'], errors='coerce')

        # tipo_analise = st.sidebar.selectbox("Tipo de Análise", ["Por Piloto", "Por Carro/Pista"])

        # if tipo_analise == "Por Carro/Pista":
        #     carro_sel = st.sidebar.selectbox("Escolha o Carro", sorted(final_df['Car'].dropna().unique()))
        #     pista_sel = st.sidebar.selectbox("Escolha a Pista", sorted(final_df['Track'].dropna().unique()))
        #     df_filtrado = final_df[(final_df['Car'] == carro_sel) & (final_df['Track'] == pista_sel)]
        # else:
        #     df_filtrado = final_df
        tipo_analise = st.sidebar.selectbox("Tipo de Análise", ["Por Piloto", "Por Carro"])
        if tipo_analise == "Por Carro":
            pista_sel = st.sidebar.selectbox("Escolha a Pista", sorted(final_df['Track'].dropna().unique()))
            df_filtrado = final_df[(final_df['Track'] == pista_sel)]
        elif tipo_analise == "Por Piloto":
            pista_sel = st.sidebar.selectbox("Escolha a Pista", sorted(final_df['Track'].dropna().unique()))
            carro_sel = st.sidebar.selectbox("Escolha o Carro", sorted(final_df['Car'].dropna().unique()))
            df_filtrado = final_df[(final_df['Car'] == carro_sel) & (final_df['Track'] == pista_sel)]

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

            # fig_all = go.Figure()
            # fig_all.add_trace(go.Histogram(
            #     x=filtered_df["Lap time"].dt.total_seconds(),
            #     nbinsx=num_bins,
            #     marker_color=gear1_colors[0],
            #     opacity=0.75
            # ))

            tick_vals = np.arange(min_lap_time, max_lap_time + bin_width, bin_width)
            tick_texts = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals]

            # fig_all.update_layout(
            #     title="Histograma Geral para Todos os Pilotos",
            #     xaxis_title="Tempo de Volta (MM:SS.mmm)",
            #     yaxis_title="Frequência (Nº Voltas)",
            #     xaxis=dict(
            #         tickmode='array',
            #         tickvals=tick_vals,
            #         ticktext=tick_texts,
            #         range=[min_lap_time, max_lap_time]
            #     )
            # )

            # fig_all.update_layout(bargap=0.02)#test
            # st.plotly_chart(fig_all, use_container_width=True)

            drivers = filtered_df["Driver"].unique()

            # Criar figura com 2 subplots
            fig_combined = make_subplots(
                rows=2, cols=1,
                shared_xaxes=False,
                row_heights=[0.5, 0.5],
                vertical_spacing=0.20,
                subplot_titles=(
                    "Histograma Geral para Todos os Pilotos",
                    "Boxplot por Piloto"
                )
            )

            # Histograma na primeira linha
            fig_combined.add_trace(
                go.Histogram(
                    x=filtered_df["Lap time"].dt.total_seconds(),
                    nbinsx=num_bins,
                    marker_color=gear1_colors[0],
                    opacity=0.75,
                    name="Histograma"
                ),
                row=1, col=1
            )

            # Boxplot por piloto na segunda linha
            for driver in drivers:
                driver_laps = filtered_df[filtered_df["Driver"] == driver]["Lap time"].dt.total_seconds()
                fig_combined.add_trace(
                    go.Box(
                        y=driver_laps,
                        name=driver,
                        boxpoints='outliers',
                        marker_color=gear1_colors[1],
                        boxmean=True
                    ),
                    row=2, col=1
                )

            # Definir limites do eixo Y com base nos tempos de volta
            boxplot_min = filtered_df["Lap time"].dt.total_seconds().min()
            boxplot_max = filtered_df["Lap time"].dt.total_seconds().max()
            boxplot_step = bin_width  # mesmo passo do histograma

            tick_vals_y = np.arange(boxplot_min, boxplot_max + boxplot_step, boxplot_step)
            tick_text_y = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals_y]

            # Layout final
            fig_combined.update_layout(
                height=800,
                xaxis_title="Tempo de Volta (MM:SS.mmm)",
                yaxis_title="Frequência (Nº Voltas)",
                xaxis=dict(
                    tickmode='array',
                    tickvals=tick_vals,
                    ticktext=tick_texts,
                    range=[min_lap_time, max_lap_time]
                ),
                yaxis2=dict(
                    title="Tempo de Volta",
                    tickmode='array',
                    tickvals=tick_vals_y,
                    ticktext=tick_text_y,
                ),
                bargap=0.02,
                showlegend=False
            )
            fig_combined.update_xaxes(tickangle=45)
            # Mostrar na tela
            st.plotly_chart(fig_combined, use_container_width=True)

            for driver in drivers:
                driver_data = filtered_df[filtered_df["Driver"] == driver]

                fig = make_subplots(rows=1, cols=2, shared_yaxes=True, column_widths=[0.7, 0.3])

                fig.add_trace(go.Histogram(
                    x=driver_data["Lap time"].dt.total_seconds(),
                    nbinsx=num_bins,
                    marker_color=gear1_colors[1],
                    opacity=0.75,
                    name="Histograma"
                ), row=1, col=1)

                fig.add_trace(go.Box(
                    x=driver_data["Lap time"].dt.total_seconds(),
                    marker_color=gear1_colors[1],
                    name="Box Plot",
                    boxmean=True,
                    # boxpoints='all',
                    # jitter=0.3,
                    # pointpos=-1.8,
                ), row=1, col=2)

                fig.update_layout(
                    title=f"Análise para {driver}",
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
                    showlegend=False
                )
                fig.update_xaxes(tickangle=45)
                fig.update_layout(bargap=0.02)
                st.plotly_chart(fig, use_container_width=True)

                # #teste setores
                # sector_columns = [col for col in final_df.columns if 'Sector' in col]
        with tab4:
            # st.subheader("Temperatura ao longo do tempo")
            col1, col2 = st.columns(2)
            if 'Track temp' in df_filtrado.columns:
                fig_track_temp = px.line(df_filtrado, x='Started at', y='Track temp', title='Temperatura da Pista')
                fig_track_temp.update_traces(line_color=gear1_colors[0]) 
                col1.plotly_chart(fig_track_temp, use_container_width=True)
            if 'Air temperature' in df_filtrado.columns:
                fig_air_temp = px.line(df_filtrado, x='Started at', y='Air temperature', title='Temperatura do Ar')
                fig_air_temp.update_traces(line_color=gear1_colors[1]) 
                col2.plotly_chart(fig_air_temp, use_container_width=True)

        with tab3:
            # if 'Fuel used' in df_filtrado.columns:
            #     # Cálculo de limites dinâmicos para o eixo Y do boxplot
            #     fuel_used_85 = np.percentile(df_filtrado["Fuel used"].dropna(), 85)
            #     fuel_used_15 = np.percentile(df_filtrado["Fuel used"].dropna(), 15)
            #     fuel_used_min = df_filtrado["Fuel used"].min()

            #     fig_fuel = make_subplots(
            #         rows=2, cols=1,
            #         shared_xaxes=False,
            #         vertical_spacing=0.15,
            #         row_heights=[0.5, 0.5],
            #         subplot_titles=("Histograma do Consumo por Volta", "Boxplot de Consumo por Piloto")
            #     )

            #     # Histograma
            #     fig_fuel.add_trace(go.Histogram(
            #         x=df_filtrado["Fuel used"],
            #         nbinsx=30,
            #         marker_color=gear1_colors[0],
            #         opacity=0.75,
            #         name="Consumo por Volta"
            #     ), row=1, col=1)

            #     # Boxplot por piloto
            #     fig_fuel.add_trace(go.Box(
            #         # x=df_filtrado["Driver"],
            #         # y=df_filtrado["Fuel used"],
            #         x=df_filtrado["Fuel used"],
            #         y=df_filtrado["Driver"],
            #         boxpoints="outliers",
            #         marker_color=gear1_colors[1],
            #         name="Boxplot por Piloto",
            #         orientation='h'
            #     ), row=2, col=1)

            #     fig_fuel.update_layout(
            #         height=800,
            #         showlegend=False
            #     )

            #     # Aplica limite de range no eixo Y do boxplot
            #     # fig_fuel.update_yaxes(title_text="Fuel Used", row=1, col=1)
            #     fig_fuel.update_yaxes(title_text="Fuel Used", range=[fuel_used_15 , fuel_used_85], row=2, col=1)
            #     # fig_fuel.update_yaxes(range=[1.0, 2.5], row=2, col=1)

            #     # fig_fuel.update_xaxes(title_text="Fuel Used", row=1, col=1)
            #     # fig_fuel.update_xaxes(title_text="Piloto", row=2, col=1)

            #     fig_fuel.update_xaxes(title_text="Piloto", row=1, col=1)
            #     fig_fuel.update_xaxes(title_text="Fuel Used", row=2, col=1)

            #     st.plotly_chart(fig_fuel, use_container_width=True)

            if 'Fuel used' in df_filtrado.columns:
            # Histograma do Consumo
                fig_hist = go.Figure()
                fig_hist.add_trace(go.Histogram(
                    x=df_filtrado["Fuel used"],
                    nbinsx=30,
                    marker_color=gear1_colors[0],
                    opacity=0.75,
                    name="Consumo por Volta"
                ))
                fig_hist.update_layout(
                    title_text="Histograma do Consumo por Volta (Fuel Used)",
                    xaxis_title="Fuel Used",
                    yaxis_title="Frequência"
                )
                st.plotly_chart(fig_hist, use_container_width=True)

                # Calcular Q1, Q3 e IQR para cada piloto
                q1 = df_filtrado.groupby('Driver')['Fuel used'].quantile(0.25)
                q3 = df_filtrado.groupby('Driver')['Fuel used'].quantile(0.75)
                iqr = q3 - q1

                # Limites sem outliers
                lower_bounds = q1 - 1.5 * iqr
                upper_bounds = q3 + 1.5 * iqr

                # Intervalo de Y para zoom
                min_fuel = lower_bounds.min()
                max_fuel = upper_bounds.max()

                # Criar boxplot vertical
                fig_box_vertical = go.Figure()
                fig_box_vertical.add_trace(go.Box(
                    y=df_filtrado["Fuel used"],
                    x=df_filtrado["Driver"],
                    boxpoints="outliers",
                    marker_color=gear1_colors[1],
                    orientation='v',
                    boxmean=True
                ))

                fig_box_vertical.update_layout(
                    title_text="Boxplot do Consumo de Combustível por Piloto",
                    xaxis_title="Piloto",
                    yaxis_title="Consumo (L)",
                    yaxis=dict(range=[min_fuel, max_fuel]),
                    margin=dict(l=40, r=40, t=60, b=120),
                    showlegend=False
                )

                st.plotly_chart(fig_box_vertical, use_container_width=True)

        # if 'Driver' in df_filtrado.columns and 'Clean' in df_filtrado.columns:
        #     st.subheader("Voltas Limpas vs Incidentes por Piloto")
        #     grouped = df_filtrado.groupby(['Driver', 'Clean']).size().reset_index(name='Counts')
        #     voltas_limpas = grouped[grouped['Clean'] == 1]
        #     voltas_com_incidentes = grouped[grouped['Clean'] == 0]

        #     fig = go.Figure()
        #     fig.add_trace(go.Bar(x=voltas_limpas['Driver'], y=voltas_limpas['Counts'], name='Voltas Limpas', marker_color='rgb(25, 128, 37)'))
        #     fig.add_trace(go.Bar(x=voltas_com_incidentes['Driver'], y=voltas_com_incidentes['Counts'], name='Voltas com Incidente', marker_color='rgb(255, 127, 0)'))
        #     fig.update_layout(title='Total de Voltas por Piloto', xaxis_title='Piloto', yaxis_title='Número de Voltas', barmode='stack', template='plotly_white')
        #     st.plotly_chart(fig)

        #     sequencias_df = calcular_sequencias_voltas_limpas(df_filtrado)
        #     maiores_sequencias = sequencias_df.groupby('Driver')['Sequence_Length'].max().reset_index()

        #     fig_bar = go.Figure(data=[go.Bar(x=maiores_sequencias['Driver'], y=maiores_sequencias['Sequence_Length'], marker_color='rgb(15, 114, 35)')])
        #     fig_bar.update_layout(title='Maior Sequência de Voltas Limpas por Piloto', xaxis_title='Piloto', yaxis_title='Número de Voltas Limpas Consecutivas', template='plotly_white')
        #     st.plotly_chart(fig_bar)

        #     for driver in sequencias_df['Driver'].unique():
        #         driver_data = sequencias_df[sequencias_df['Driver'] == driver]
        #         fig_hist = go.Figure(data=[go.Histogram(x=driver_data['Sequence_Length'], marker_color='rgb(15, 114, 35)', opacity=0.75)])
        #         fig_hist.update_layout(title=f'Distribuição das Sequências de Voltas Limpas - {driver}', xaxis_title='Tamanho da Sequência de Voltas Limpas', yaxis_title='Frequência', template='plotly_white')
        #         st.plotly_chart(fig_hist)

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
                            

                # Agrupar por piloto e status de volta limpa/incidente
                # grouped = final_df.groupby(['Driver', 'Clean']).size().unstack(fill_value=0) #pie

                # # Iterar sobre cada piloto e criar um gráfico de pizza
                # for driver in grouped.index:
                #     clean_counts = grouped.loc[driver]

                #     labels = ['Voltas Limpas','Voltas com Incidente']
                #     values = [clean_counts.get(1, 0), clean_counts.get(0, 0)]

                    
                #     fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=0.3, pull=[0.1,0], marker_colors=gear1_colors)])
                #     fig.update_layout(title_text=f'Análise de Voltas Limpas para {driver}')

                #     st.plotly_chart(fig)
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

            # Exibir o gráfico de barras no Streamlit
            st.plotly_chart(fig_bar)

if __name__ == "__main__":
    main()

