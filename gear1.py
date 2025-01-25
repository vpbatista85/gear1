import streamlit as st
import pandas as pd
import irsdk
import time

# Inicializar irsdk
ir = irsdk.IRSDK()
ir.startup()

# Função para coletar dados da sessão atual
def get_session_data():
    if not ir.is_connected:  # Corrigido: agora sem parênteses
        ir.startup()
    
    if ir.is_connected:  # Corrigido: agora sem parênteses
        # Coleta de dados básicos da sessão
        driver_idx = ir['DriverInfo']['DriverCarIdx']
        data = {
            'Nome da Equipe': ir['TeamName'],
            'Piloto': ir['DriverInfo']['Drivers'][driver_idx]['UserName'],
            'Stint Nº': ir['CarIdxLapCompleted'][driver_idx],
            'Volta Nº': ir['Lap'],
            'Melhor Volta': ir['BestLapTime'],
            'Tempo da última volta': ir['LapLastLapTime'],
            'Tempo médio últimas 5 voltas': pd.Series(ir['LapBestNLapTime']).tail(5).mean(),
            'Tempo médio últimas 10 voltas': pd.Series(ir['LapBestNLapTime']).tail(10).mean(),
            'Tempo médio últimas 20 voltas': pd.Series(ir['LapBestNLapTime']).tail(20).mean(),
            'Nº de incidentes do piloto': ir['CarIdxIncidentCount'][driver_idx],
            'Nº de incidentes da equipe': ir['TeamIncidentCount'],
            'Tempo restante': ir['SessionTimeRemain'],
            'Previsão de Nº de Voltas': ir['SessionLapsRemain'],
        }
        return data
    return None

# Função para cálculo de undercut
def calculate_undercut(car_data, car_idx):
    try:
        current_lap_time = ir['LapLastLapTime'][car_idx]
        best_lap_time = ir['BestLapTime']
        
        if current_lap_time > best_lap_time * 1.05:
            return "Oportunidade de Undercut!"
        return "Sem oportunidade de Undercut no momento"
    
    except KeyError:
        return "Dados indisponíveis para o cálculo de undercut"

# Função para cálculo de overcut
def calculate_overcut(car_data, car_idx):
    try:
        last_lap_time = ir['LapLastLapTime'][car_idx]
        avg_last_laps = pd.Series(ir['LapBestNLapTime']).tail(5).mean()
        
        if last_lap_time > avg_last_laps * 1.03:
            return "Oportunidade de Overcut!"
        return "Sem oportunidade de Overcut no momento"
    
    except KeyError:
        return "Dados indisponíveis para o cálculo de overcut"

# Função para fuel saving
def calculate_fuel_saving(car_data):
    try:
        avg_fuel_per_lap = ir['FuelLevel'] / ir['Lap']
        fuel_left = ir['FuelLevel']
        laps_left = ir['SessionLapsRemain']
        
        if avg_fuel_per_lap * laps_left > fuel_left * 1.05:
            return "Reduzir ritmo para maximizar o combustível"
        return "Ritmo estável, sem necessidade de economia de combustível"
    
    except KeyError:
        return "Dados indisponíveis para cálculo de fuel saving"

# Exibir dados na tela
def display_data():
    st.title("iRacing Estratégia de Corrida")
    st.write("Atualizando a cada 0.5s...")
    
    # Dataframe inicial
    df = pd.DataFrame()
    
    while True:
        session_data = get_session_data()
        if session_data:
            # Adiciona os novos dados ao dataframe
            df = df.append(session_data, ignore_index=True)
            
            # Exibir apenas a última atualização na tela
            st.dataframe(df.tail(1))
            
            # Mostrar estratégias
            car_idx = ir['DriverInfo']['DriverCarIdx']
            st.write(f"Undercut: {calculate_undercut(df, car_idx)}")
            st.write(f"Overcut: {calculate_overcut(df, car_idx)}")
            st.write(f"Fuel Saving: {calculate_fuel_saving(df)}")
        
        # Atualiza a cada 0.5s
        time.sleep(0.5)

if __name__ == "__main__":
    display_data()
