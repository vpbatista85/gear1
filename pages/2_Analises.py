import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def main():
    st.set_page_config(
        page_title="Gear 1 Post Race",
        page_icon="https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png",
        layout="wide",
        initial_sidebar_state="expanded",
        # menu_items={
        #     'Get Help': 'https://www.extremelycoolapp.com/help',
        #     'Report a bug': "https://www.extremelycoolapp.com/bug",
        #     'About': "# This is a header. This is an *extremely* cool app!"
        # }
    )
    st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)

    col1, col2 = st.columns(2)
    col1.title(":green[Análise Pós Corrida]")
    # col2.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)

    uploaded_file = st.file_uploader(":green[Escolha o arquivo Excel (Exportado do Garagem 61)]", type=["xlsx"])

    # # Carregar a aba 'Overview' do arquivo Excel
    # df_overview = pd.read_excel(uploaded_file, sheet_name='Overview')

    # # Extrair informações específicas
    # car = df_overview['Car'].iloc[0]  # Supondo que a informação do carro esteja na primeira linha
    # track = df_overview['Track'].iloc[0]  # Supondo que a informação da pista esteja na primeira linha
    # drivers = df_overview['Driver'].dropna().unique().tolist()  # Lista de drivers únicos, removendo valores nulos

    # # Criar o dicionário com as informações
    # info_dict = {
    #     'Car': car,
    #     'Track': track,
    #     'Drivers': drivers
    # }

    if uploaded_file is not None:
        # col1.title(f":green[Análise Pós Corrida - {track}]")
        # col1.write(f":green[Carro -{car}]")
        # col1.write(f":green[Pilotos -{drivers}]")
        xls = pd.ExcelFile(uploaded_file)
        sheet_names = [sheet for sheet in xls.sheet_names if sheet != 'Overview']

        # st.sidebar.image("https://gear1.gg/wp-content/uploads/2022/11/Cabecalho.png", width=128)
        st.sidebar.header(":green[Sessões]")
        
        selected_sheets = [sheet for sheet in sheet_names if st.sidebar.checkbox(sheet, value=True)]
        
        if selected_sheets:
            df_list = [pd.read_excel(xls, sheet_name=sheet).assign(SheetName=sheet) for sheet in selected_sheets]
            final_df = pd.concat(df_list, ignore_index=True)     
            required_columns = {'Lap time', 'Driver'}
            if not required_columns.issubset(final_df.columns):
                st.error(f"O arquivo deve conter as colunas: {', '.join(required_columns)}")
                return

            final_df["Lap time"] = pd.to_timedelta(final_df["Lap time"], errors='coerce')
            final_df = final_df.dropna(subset=["Lap time"])


        tab1, tab2 = st.tabs([":green[Lap Time]", ":green[Safety]"])
        # Define color sets of paintings
        gear1_colors = ['rgb(25, 128, 37)','rgb(255, 127, 0)']#verde, laranja
       
            

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

            fig_all = go.Figure()
            fig_all.add_trace(go.Histogram(
                x=filtered_df["Lap time"].dt.total_seconds(),
                nbinsx=num_bins,
                marker_color=gear1_colors[0],
                opacity=0.75
            ))

            tick_vals = np.arange(min_lap_time, max_lap_time + bin_width, bin_width)
            tick_texts = [f"{int(t // 60):02}:{int(t % 60):02}.{int((t * 1000) % 1000):03}" for t in tick_vals]

            fig_all.update_layout(
                title="Histograma Geral para Todos os Pilotos",
                xaxis_title="Tempo de Volta (MM:SS.mmm)",
                yaxis_title="Frequência (Nº Voltas)",
                xaxis=dict(
                    tickmode='array',
                    tickvals=tick_vals,
                    ticktext=tick_texts,
                    range=[min_lap_time, max_lap_time]
                )
            )

            fig_all.update_layout(bargap=0.02)#test
            st.plotly_chart(fig_all, use_container_width=True)

            drivers = filtered_df["Driver"].unique()

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
                    barmode='stack',  # Empilhar as barras
                    template='plotly_white'  # Estilo do gráfico
                )

                # Exibir o gráfico no Streamlit
                st.plotly_chart(fig)

            

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

if __name__ == "__main__":
    main()
