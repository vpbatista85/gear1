import irsdk
import polars as pl
import time
import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import datetime
import unicodedata
import re
import psutil

# === Configurações ===
SERVICE_ACCOUNT_FILE = 'C:/Users/vpb85/Documents/Gear1/gear1-ir-36de8419de96.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']
PARQUET_FILENAME = 'telemetria_iracing.parquet'
LOCAL_DIR = os.path.expanduser("~/Documents/iracing_parquet")
LOCAL_PATH = os.path.join(LOCAL_DIR, PARQUET_FILENAME)
UPLOAD_INTERVAL = 10  # frames (cada 300 coletas = 1 envio)

def is_iracing_running():
    for proc in psutil.process_iter(attrs=['name']):
        if proc.info['name'] == 'iRacingSim64DX11.exe':
            return True
    return False

def remover_acentos(texto):
    if not isinstance(texto, str):
        return "desconhecido"
    nfkd = unicodedata.normalize('NFKD', texto)
    texto_sem_acentos = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return re.sub(r'[^a-zA-Z0-9_ ]', '', texto_sem_acentos)

import datetime

def gerar_nome_arquivo(df_list, iracing_ativo=True):
    """
    df_list: lista de dicionários, onde cada dicionário é uma linha dos dados coletados
    iracing_ativo: se True, inclui sufixo _Live, caso contrário não inclui
    """

    try:
        if not isinstance(df_list, list) or len(df_list) == 0:
            raise ValueError("Objeto df não é uma lista com dados")

        first_row = df_list[0]

        driver_info = first_row.get('DriverInfo')
        weekend_info = first_row.get('WeekendInfo')

        if driver_info is None or weekend_info is None:
            raise ValueError("Dados DriverInfo ou WeekendInfo não encontrados na primeira linha")

        driver_user_id = driver_info.get('DriverUserID')
        drivers = driver_info.get('Drivers', [])
        user_name = "Desconhecido"
        car_name = "Desconhecido"

        for piloto in drivers:
            if piloto.get('UserID') == driver_user_id:
                user_name = piloto.get('UserName', 'Desconhecido').replace(" ", "_")
                car_name = piloto.get('CarScreenName', 'Desconhecido').replace(" ", "_")
                break

        track_name = weekend_info.get('TrackDisplayName', 'Desconhecido').replace(" ", "_")

        session_id = str(weekend_info.get('SessionID', 'UnknownSession'))
        subsession_id = str(weekend_info.get('SubSessionID', 'UnknownSubSession'))

        sufixo = "_Live" if iracing_ativo else ""
        nome = f"{user_name}_{car_name}_{track_name}_{session_id}_{subsession_id}{sufixo}.parquet"
        return nome

    except Exception as e:
        print("Erro ao gerar nome do arquivo:", e)
        return "telemetria_iracing.parquet"

def remover_sufixo_live(caminho_arquivo):
    if "_Live.parquet" in caminho_arquivo:
        novo_nome = caminho_arquivo.replace("_Live.parquet", ".parquet")
        os.rename(caminho_arquivo, novo_nome)
        print(f"Arquivo renomeado para: {novo_nome}")
        return novo_nome
    return caminho_arquivo


# === Autenticação com Google Drive ===
def authenticate_google_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

# === Envio do arquivo ao Google Drive ===
DRIVE_FOLDER_ID = '1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ'  # <-- Substitua pelo seu ID real
pasta_drive_id = DRIVE_FOLDER_ID

def upload_to_drive(service, file_path, file_name):
    print(f"Iniciando envio do arquivo: {file_name}")
    print(f"Caminho do arquivo: {file_path}")

    file_metadata = {
        'name': file_name,
        'parents': [DRIVE_FOLDER_ID]
    }

    media = MediaFileUpload(file_path, mimetype='application/octet-stream', resumable=True)

    query = f"name='{file_name}' and '{DRIVE_FOLDER_ID}' in parents"
    print(f"Executando consulta no Drive: {query}")
    
    response = service.files().list(q=query, spaces='drive').execute()
    print(f"Resposta da API (list): {response}")

    if response['files']:
        file_id = response['files'][0]['id']
        print(f"Arquivo já existe, atualizando. ID: {file_id}")
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Arquivo atualizado.")
    else:
        print(f"Arquivo não encontrado, criando novo.")
        created = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Arquivo criado com ID: {created.get('id')}")
        print(f"Link direto: https://drive.google.com/file/d/{created.get('id')}/view")

# === Salva os dados no arquivo Parquet local ===
def salvar_incremental(data_batch, iracing_ativo=True):
    df = pl.DataFrame(data_batch)
    nome_arquivo = gerar_nome_arquivo(df.to_dicts(), iracing_ativo)
    caminho_completo = os.path.join(LOCAL_DIR, nome_arquivo)

    if os.path.exists(caminho_completo):
        existing_df = pl.read_parquet(caminho_completo)
        df = existing_df.vstack(df)

    df.write_parquet(caminho_completo)
    return caminho_completo, nome_arquivo

def wait_for_iracing(ir):
    print("Aguardando iRacing iniciar...")

    while True:
        ir.shutdown()
        time.sleep(1)
        ir.startup()

        if ir.is_initialized and ir.is_connected:
            print("Conectado ao iRacing.")
            break
        else:
            print("Aguardando sessão iRacing...")
            time.sleep(1)

def delete_file_from_drive_by_name(service, file_name, folder_id):
    print(f"Tentando excluir arquivo do Drive com nome: {file_name}")
    
    # Consulta para encontrar o arquivo
    query = f"name='{file_name}' and '{folder_id}' in parents"
    try:
        response = service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
        files = response.get('files', [])
        print(f"Resultado da busca por '{file_name}': {files}")

        if not files:
            print(f"[!] Nenhum arquivo chamado '{file_name}' encontrado no Google Drive.")
        else:
            for file in files:
                file_id = file['id']
                try:
                    service.files().delete(fileId=file_id).execute()
                    print(f"[✓] Arquivo '{file_name}' excluído do Google Drive. ID: {file_id}")
                except Exception as delete_error:
                    print(f"[x] Falha ao excluir o arquivo '{file_name}' do Drive. Erro: {delete_error}")
    except Exception as e:
        print(f"[x] Erro ao buscar arquivo '{file_name}' no Drive: {e}")

# var_list=['AirDensity', 'AirPressure', 'AirTemp', 'Brake', 'BrakeABSactive', 'BrakeRaw', 'CamCameraNumber', 'CamCameraState', 'CamCarIdx', 'CamGroupNumber', 'CarDistAhead', 'CarDistBehind', 'CarIdxBestLapNum', 'CarIdxBestLapTime', 'CarIdxClass', 'CarIdxClassPosition', 'CarIdxEstTime', 'CarIdxF2Time', 'CarIdxFastRepairsUsed', 'CarIdxGear', 'CarIdxLap', 'CarIdxLapCompleted', 'CarIdxLapDistPct', 'CarIdxLastLapTime', 'CarIdxOnPitRoad', 'CarIdxP2P_Count', 'CarIdxP2P_Status', 'CarIdxPaceFlags', 'CarIdxPaceLine', 'CarIdxPaceRow', 'CarIdxPosition', 'CarIdxQualTireCompound', 'CarIdxQualTireCompoundLocked', 'CarIdxRPM', 'CarIdxSessionFlags', 'CarIdxSteer', 'CarIdxTireCompound', 'CarIdxTrackSurface', 'CarIdxTrackSurfaceMaterial', 'CarLeftRight', 'ChanAvgLatency', 'ChanClockSkew', 'ChanLatency', 'ChanPartnerQuality', 'ChanQuality', 'Clutch', 'ClutchRaw', 'CpuUsageBG', 'CpuUsageFG', 'DCDriversSoFar', 'DCLapStatus', 'dcPitSpeedLimiterToggle', 'dcStarter', 'dcToggleWindshieldWipers', 'dcTriggerWindshieldWipers', 'DisplayUnits', 'dpFastRepair', 'dpFuelAddKg', 'dpFuelAutoFillActive', 'dpFuelAutoFillEnabled', 'dpFuelFill', 'dpLFTireChange', 'dpLFTireColdPress', 'dpLRTireChange', 'dpLRTireColdPress', 'dpRFTireChange', 'dpRFTireColdPress', 'dpRRTireChange', 'dpRRTireColdPress', 'dpWindshieldTearoff', 'DriverMarker','DriverInfo', 'Engine0_RPM', 'EngineWarnings', 'EnterExitReset', 'FastRepairAvailable', 'FastRepairUsed', 'FogLevel', 'FrameRate', 'FrontTireSetsAvailable', 'FrontTireSetsUsed', 'FuelLevel', 'FuelLevelPct', 'FuelPress', 'FuelUsePerHour', 'Gear', 'GpuUsage', 'HandbrakeRaw', 'IsDiskLoggingActive', 'IsDiskLoggingEnabled', 'IsGarageVisible', 'IsInGarage', 'IsOnTrack', 'IsOnTrackCar', 'IsReplayPlaying', 'Lap', 'LapBestLap', 'LapBestLapTime', 'LapBestNLapLap', 'LapBestNLapTime', 'LapCompleted', 'LapCurrentLapTime', 'LapDeltaToBestLap', 'LapDeltaToBestLap_DD', 'LapDeltaToBestLap_OK', 'LapDeltaToOptimalLap', 'LapDeltaToOptimalLap_DD', 'LapDeltaToOptimalLap_OK', 'LapDeltaToSessionBestLap', 'LapDeltaToSessionBestLap_DD', 'LapDeltaToSessionBestLap_OK', 'LapDeltaToSessionLastlLap', 'LapDeltaToSessionLastlLap_DD', 'LapDeltaToSessionLastlLap_OK', 'LapDeltaToSessionOptimalLap', 'LapDeltaToSessionOptimalLap_DD', 'LapDeltaToSessionOptimalLap_OK', 'LapDist', 'LapDistPct', 'LapLasNLapSeq', 'LapLastLapTime', 'LapLastNLapTime', 'LatAccel', 'LatAccel_ST', 'LeftTireSetsAvailable', 'LeftTireSetsUsed', 'LFbrakeLinePress', 'LFcoldPressure', 'LFodometer', 'LFshockDefl', 'LFshockDefl_ST', 'LFshockVel', 'LFshockVel_ST', 'LFtempCL', 'LFtempCM', 'LFtempCR', 'LFTiresAvailable', 'LFTiresUsed', 'LFwearL', 'LFwearM', 'LFwearR', 'LoadNumTextures', 'LongAccel', 'LongAccel_ST', 'LRbrakeLinePress', 'LRcoldPressure', 'LRodometer', 'LRshockDefl', 'LRshockDefl_ST', 'LRshockVel', 'LRshockVel_ST', 'LRtempCL', 'LRtempCM', 'LRtempCR', 'LRTiresAvailable', 'LRTiresUsed', 'LRwearL', 'LRwearM', 'LRwearR', 'ManifoldPress', 'ManualBoost', 'ManualNoBoost', 'MemPageFaultSec', 'MemSoftPageFaultSec', 'OilLevel', 'OilPress', 'OilTemp', 'OkToReloadTextures', 'OnPitRoad', 'P2P_Count', 'P2P_Status', 'PaceMode', 'Pitch', 'PitchRate', 'PitchRate_ST', 'PitOptRepairLeft', 'PitRepairLeft', 'PitsOpen', 'PitstopActive', 'PitSvFlags', 'PitSvFuel', 'PitSvLFP', 'PitSvLRP', 'PitSvRFP', 'PitSvRRP', 'PitSvTireCompound', 'PlayerCarClass', 'PlayerCarClassPosition', 'PlayerCarDriverIncidentCount', 'PlayerCarDryTireSetLimit', 'PlayerCarIdx', 'PlayerCarInPitStall', 'PlayerCarMyIncidentCount', 'PlayerCarPitSvStatus', 'PlayerCarPosition', 'PlayerCarPowerAdjust', 'PlayerCarSLBlinkRPM', 'PlayerCarSLFirstRPM', 'PlayerCarSLLastRPM', 'PlayerCarSLShiftRPM', 'PlayerCarTeamIncidentCount', 'PlayerCarTowTime', 'PlayerCarWeightPenalty', 'PlayerFastRepairsUsed', 'PlayerIncidents', 'PlayerTireCompound', 'PlayerTrackSurface', 'PlayerTrackSurfaceMaterial', 'Precipitation', 'PushToPass', 'PushToTalk', 'RaceLaps', 'RadioTransmitCarIdx', 'RadioTransmitFrequencyIdx', 'RadioTransmitRadioIdx', 'RearTireSetsAvailable', 'RearTireSetsUsed', 'RelativeHumidity', 'RFbrakeLinePress', 'RFcoldPressure', 'RFodometer', 'RFshockDefl', 'RFshockDefl_ST', 'RFshockVel', 'RFshockVel_ST', 'RFtempCL', 'RFtempCM', 'RFtempCR', 'RFTiresAvailable', 'RFTiresUsed', 'RFwearL', 'RFwearM', 'RFwearR', 'RightTireSetsAvailable', 'RightTireSetsUsed', 'Roll', 'RollRate', 'RollRate_ST', 'RPM', 'RRbrakeLinePress', 'RRcoldPressure', 'RRodometer', 'RRshockDefl', 'RRshockDefl_ST', 'RRshockVel', 'RRshockVel_ST', 'RRtempCL', 'RRtempCM', 'RRtempCR', 'RRTiresAvailable', 'RRTiresUsed', 'RRwearL', 'RRwearM', 'RRwearR','SessionInfo', 'SessionFlags', 'SessionJokerLapsRemain', 'SessionLapsRemain', 'SessionLapsRemainEx', 'SessionLapsTotal', 'SessionNum', 'SessionOnJokerLap', 'SessionState', 'SessionTick', 'SessionTime', 'SessionTimeOfDay', 'SessionTimeRemain', 'SessionTimeTotal', 'SessionUniqueID', 'Shifter', 'ShiftGrindRPM', 'ShiftPowerPct', 'Skies', 'SolarAltitude', 'SolarAzimuth', 'Speed', 'SteeringFFBEnabled', 'SteeringWheelAngle', 'SteeringWheelAngleMax', 'SteeringWheelLimiter', 'SteeringWheelMaxForceNm', 'SteeringWheelPctDamper', 'SteeringWheelPctIntensity', 'SteeringWheelPctSmoothing', 'SteeringWheelPctTorque', 'SteeringWheelPctTorqueSign', 'SteeringWheelPctTorqueSignStops', 'SteeringWheelPeakForceNm', 'SteeringWheelTorque', 'SteeringWheelTorque_ST', 'SteeringWheelUseLinear','SplitTimeInfo', 'Throttle', 'ThrottleRaw', 'TireLF_RumblePitch', 'TireLR_RumblePitch', 'TireRF_RumblePitch', 'TireRR_RumblePitch', 'TireSetsAvailable', 'TireSetsUsed', 'TrackTempCrew', 'TrackWetness', 'VelocityX', 'VelocityX_ST', 'VelocityY', 'VelocityY_ST', 'VelocityZ', 'VelocityZ_ST', 'VertAccel', 'VertAccel_ST', 'VidCapActive', 'VidCapEnabled', 'Voltage', 'WaterLevel', 'WaterTemp', 'WeatherDeclaredWet','WeekendInfo', 'WindDir', 'WindVel', 'Yaw', 'YawNorth', 'YawRate', 'YawRate_ST']
# === Coleta de dados do iRacing e envio ===
def main():
    ir = irsdk.IRSDK()
    os.makedirs(LOCAL_DIR, exist_ok=True)

    service = authenticate_google_drive()
    data_batch = []
    frame_count = 0
    nome_arquivo_live = None
    caminho_arquivo_live = None

    print("Aguardando o iRacing iniciar...")

    while not is_iracing_running():
        time.sleep(1)

    print("iRacing iniciado. Conectando ao iRSDK...")
    wait_for_iracing(ir)

    start_timestamp = time.time()  # <-- aqui define o timestamp de início

    var_list=['AirDensity', 'AirPressure', 'AirTemp', 'Brake', 'BrakeABSactive', 'BrakeRaw', 'CamCameraNumber', 'CamCameraState', 'CamCarIdx', 'CamGroupNumber', 'CarDistAhead', 'CarDistBehind', 'CarIdxBestLapNum', 'CarIdxBestLapTime', 'CarIdxClass', 'CarIdxClassPosition', 'CarIdxEstTime', 'CarIdxF2Time', 'CarIdxFastRepairsUsed', 'CarIdxGear', 'CarIdxLap', 'CarIdxLapCompleted', 'CarIdxLapDistPct', 'CarIdxLastLapTime', 'CarIdxOnPitRoad', 'CarIdxP2P_Count', 'CarIdxP2P_Status', 'CarIdxPaceFlags', 'CarIdxPaceLine', 'CarIdxPaceRow', 'CarIdxPosition', 'CarIdxQualTireCompound', 'CarIdxQualTireCompoundLocked', 'CarIdxRPM', 'CarIdxSessionFlags', 'CarIdxSteer', 'CarIdxTireCompound', 'CarIdxTrackSurface', 'CarIdxTrackSurfaceMaterial', 'CarLeftRight', 'ChanAvgLatency', 'ChanClockSkew', 'ChanLatency', 'ChanPartnerQuality', 'ChanQuality', 'Clutch', 'ClutchRaw', 'CpuUsageBG', 'CpuUsageFG', 'DCDriversSoFar', 'DCLapStatus', 'dcPitSpeedLimiterToggle', 'dcStarter', 'dcToggleWindshieldWipers', 'dcTriggerWindshieldWipers', 'DisplayUnits', 'dpFastRepair', 'dpFuelAddKg', 'dpFuelAutoFillActive', 'dpFuelAutoFillEnabled', 'dpFuelFill', 'dpLFTireChange', 'dpLFTireColdPress', 'dpLRTireChange', 'dpLRTireColdPress', 'dpRFTireChange', 'dpRFTireColdPress', 'dpRRTireChange', 'dpRRTireColdPress', 'dpWindshieldTearoff', 'DriverMarker','DriverInfo', 'Engine0_RPM', 'EngineWarnings', 'EnterExitReset', 'FastRepairAvailable', 'FastRepairUsed', 'FogLevel', 'FrameRate', 'FrontTireSetsAvailable', 'FrontTireSetsUsed', 'FuelLevel', 'FuelLevelPct', 'FuelPress', 'FuelUsePerHour', 'Gear', 'GpuUsage', 'HandbrakeRaw', 'IsDiskLoggingActive', 'IsDiskLoggingEnabled', 'IsGarageVisible', 'IsInGarage', 'IsOnTrack', 'IsOnTrackCar', 'IsReplayPlaying', 'Lap', 'LapBestLap', 'LapBestLapTime', 'LapBestNLapLap', 'LapBestNLapTime', 'LapCompleted', 'LapCurrentLapTime', 'LapDeltaToBestLap', 'LapDeltaToBestLap_DD', 'LapDeltaToBestLap_OK', 'LapDeltaToOptimalLap', 'LapDeltaToOptimalLap_DD', 'LapDeltaToOptimalLap_OK', 'LapDeltaToSessionBestLap', 'LapDeltaToSessionBestLap_DD', 'LapDeltaToSessionBestLap_OK', 'LapDeltaToSessionLastlLap', 'LapDeltaToSessionLastlLap_DD', 'LapDeltaToSessionLastlLap_OK', 'LapDeltaToSessionOptimalLap', 'LapDeltaToSessionOptimalLap_DD', 'LapDeltaToSessionOptimalLap_OK', 'LapDist', 'LapDistPct', 'LapLasNLapSeq', 'LapLastLapTime', 'LapLastNLapTime', 'LatAccel', 'LatAccel_ST', 'LeftTireSetsAvailable', 'LeftTireSetsUsed', 'LFbrakeLinePress', 'LFcoldPressure', 'LFodometer', 'LFshockDefl', 'LFshockDefl_ST', 'LFshockVel', 'LFshockVel_ST', 'LFtempCL', 'LFtempCM', 'LFtempCR', 'LFTiresAvailable', 'LFTiresUsed', 'LFwearL', 'LFwearM', 'LFwearR', 'LoadNumTextures', 'LongAccel', 'LongAccel_ST', 'LRbrakeLinePress', 'LRcoldPressure', 'LRodometer', 'LRshockDefl', 'LRshockDefl_ST', 'LRshockVel', 'LRshockVel_ST', 'LRtempCL', 'LRtempCM', 'LRtempCR', 'LRTiresAvailable', 'LRTiresUsed', 'LRwearL', 'LRwearM', 'LRwearR', 'ManifoldPress', 'ManualBoost', 'ManualNoBoost', 'MemPageFaultSec', 'MemSoftPageFaultSec', 'OilLevel', 'OilPress', 'OilTemp', 'OkToReloadTextures', 'OnPitRoad', 'P2P_Count', 'P2P_Status', 'PaceMode', 'Pitch', 'PitchRate', 'PitchRate_ST', 'PitOptRepairLeft', 'PitRepairLeft', 'PitsOpen', 'PitstopActive', 'PitSvFlags', 'PitSvFuel', 'PitSvLFP', 'PitSvLRP', 'PitSvRFP', 'PitSvRRP', 'PitSvTireCompound', 'PlayerCarClass', 'PlayerCarClassPosition', 'PlayerCarDriverIncidentCount', 'PlayerCarDryTireSetLimit', 'PlayerCarIdx', 'PlayerCarInPitStall', 'PlayerCarMyIncidentCount', 'PlayerCarPitSvStatus', 'PlayerCarPosition', 'PlayerCarPowerAdjust', 'PlayerCarSLBlinkRPM', 'PlayerCarSLFirstRPM', 'PlayerCarSLLastRPM', 'PlayerCarSLShiftRPM', 'PlayerCarTeamIncidentCount', 'PlayerCarTowTime', 'PlayerCarWeightPenalty', 'PlayerFastRepairsUsed', 'PlayerIncidents', 'PlayerTireCompound', 'PlayerTrackSurface', 'PlayerTrackSurfaceMaterial', 'Precipitation', 'PushToPass', 'PushToTalk', 'RaceLaps', 'RadioTransmitCarIdx', 'RadioTransmitFrequencyIdx', 'RadioTransmitRadioIdx', 'RearTireSetsAvailable', 'RearTireSetsUsed', 'RelativeHumidity', 'RFbrakeLinePress', 'RFcoldPressure', 'RFodometer', 'RFshockDefl', 'RFshockDefl_ST', 'RFshockVel', 'RFshockVel_ST', 'RFtempCL', 'RFtempCM', 'RFtempCR', 'RFTiresAvailable', 'RFTiresUsed', 'RFwearL', 'RFwearM', 'RFwearR', 'RightTireSetsAvailable', 'RightTireSetsUsed', 'Roll', 'RollRate', 'RollRate_ST', 'RPM', 'RRbrakeLinePress', 'RRcoldPressure', 'RRodometer', 'RRshockDefl', 'RRshockDefl_ST', 'RRshockVel', 'RRshockVel_ST', 'RRtempCL', 'RRtempCM', 'RRtempCR', 'RRTiresAvailable', 'RRTiresUsed', 'RRwearL', 'RRwearM', 'RRwearR','SessionInfo', 'SessionFlags', 'SessionJokerLapsRemain', 'SessionLapsRemain', 'SessionLapsRemainEx', 'SessionLapsTotal', 'SessionNum', 'SessionOnJokerLap', 'SessionState', 'SessionTick', 'SessionTime', 'SessionTimeOfDay', 'SessionTimeRemain', 'SessionTimeTotal', 'SessionUniqueID', 'Shifter', 'ShiftGrindRPM', 'ShiftPowerPct', 'Skies', 'SolarAltitude', 'SolarAzimuth', 'Speed', 'SteeringFFBEnabled', 'SteeringWheelAngle', 'SteeringWheelAngleMax', 'SteeringWheelLimiter', 'SteeringWheelMaxForceNm', 'SteeringWheelPctDamper', 'SteeringWheelPctIntensity', 'SteeringWheelPctSmoothing', 'SteeringWheelPctTorque', 'SteeringWheelPctTorqueSign', 'SteeringWheelPctTorqueSignStops', 'SteeringWheelPeakForceNm', 'SteeringWheelTorque', 'SteeringWheelTorque_ST', 'SteeringWheelUseLinear','SplitTimeInfo', 'Throttle', 'ThrottleRaw', 'TireLF_RumblePitch', 'TireLR_RumblePitch', 'TireRF_RumblePitch', 'TireRR_RumblePitch', 'TireSetsAvailable', 'TireSetsUsed', 'TrackTempCrew', 'TrackWetness', 'VelocityX', 'VelocityX_ST', 'VelocityY', 'VelocityY_ST', 'VelocityZ', 'VelocityZ_ST', 'VertAccel', 'VertAccel_ST', 'VidCapActive', 'VidCapEnabled', 'Voltage', 'WaterLevel', 'WaterTemp', 'WeatherDeclaredWet','WeekendInfo', 'WindDir', 'WindVel', 'Yaw', 'YawNorth', 'YawRate', 'YawRate_ST']

    try:
        while is_iracing_running():
            if ir.is_initialized and ir.is_connected:
                ir.freeze_var_buffer_latest()
                frame_data = {"timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()}

                for var_name in var_list:
                    try:
                        frame_data[var_name] = ir[var_name]
                    except:
                        frame_data[var_name] = None

                data_batch.append(frame_data)
                frame_count += 1

                if frame_count % UPLOAD_INTERVAL == 0:
                    if not nome_arquivo_live:
                        df_temp = pl.DataFrame(data_batch).to_dicts()
                        nome_arquivo_live = gerar_nome_arquivo(df_temp, iracing_ativo=True)
                        caminho_arquivo_live = os.path.join(LOCAL_DIR, nome_arquivo_live)

                    df_batch = pl.DataFrame(data_batch)
                    if os.path.exists(caminho_arquivo_live):
                        df_existente = pl.read_parquet(caminho_arquivo_live)
                        df_batch = df_existente.vstack(df_batch)

                    df_batch.write_parquet(caminho_arquivo_live)
                    upload_to_drive(service, caminho_arquivo_live, nome_arquivo_live)
                    print(f"[{frame_count}] Dados salvos e enviados ao Google Drive.")
                    data_batch.clear()

                time.sleep(0.2)
            else:
                print("Perda de conexão com iRSDK. Aguardando reconexão...")
                time.sleep(1)
                ir.shutdown()
                ir.startup()

    except KeyboardInterrupt:
        print("Interrompido manualmente.")
    finally:
        print("iRacing finalizado. Salvando dados...")

        df_final = None

        if data_batch:
            print(f"Total de registros em memória: {len(data_batch)}")

            if not nome_arquivo_live:
                nome_arquivo_live = gerar_nome_arquivo(data_batch, iracing_ativo=True)
                caminho_arquivo_live = os.path.join(LOCAL_DIR, nome_arquivo_live)
                print(f"Nome de arquivo gerado (live): {nome_arquivo_live}")
                print(f"Caminho completo do arquivo live: {caminho_arquivo_live}")

            df_batch = pl.DataFrame(data_batch)

            if caminho_arquivo_live and os.path.exists(caminho_arquivo_live):
                print(f"Arquivo temporário encontrado: {caminho_arquivo_live}")
                try:
                    df_existente = pl.read_parquet(caminho_arquivo_live)
                    df_final = df_existente.vstack(df_batch)
                except Exception as e:
                    print(f"Erro ao ler/parquet temporário: {e}")
                    df_final = df_batch
            else:
                print("Nenhum arquivo temporário encontrado. Será criado um novo arquivo.")
                df_final = df_batch
        else:
            print("Nenhum dado novo na memória. Verificando arquivo temporário existente...")
            if caminho_arquivo_live and os.path.exists(caminho_arquivo_live):
                print(f"Lendo arquivo temporário: {caminho_arquivo_live}")
                try:
                    df_final = pl.read_parquet(caminho_arquivo_live)
                except Exception as e:
                    print(f"Erro ao ler/parquet temporário: {e}")
                    df_final = None
            else:
                print("Nenhum dado disponível para salvar.")

        if df_final is not None and df_final.height > 0:
            if start_timestamp is not None:
                timestamp_inicio = datetime.datetime.fromtimestamp(start_timestamp).strftime("%Y%m%d_%H%M%S")
            else:
                timestamp_inicio = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                print("Aviso: start_timestamp não definido. Usando horário atual.")

            nome_final = nome_arquivo_live.replace("_Live.parquet", f"_{timestamp_inicio}.parquet")
            caminho_final = os.path.join(LOCAL_DIR, nome_final)

            print(f"Salvando arquivo final: {nome_final}")
            print(f"Caminho completo: {caminho_final}")

            try:
                df_final.write_parquet(caminho_final)
                print("Arquivo final salvo com sucesso.")
            except Exception as e:
                print(f"Erro ao salvar o arquivo final: {e}")

            # Remove temporário (local)
            if caminho_arquivo_live and os.path.exists(caminho_arquivo_live):
                try:
                    os.remove(caminho_arquivo_live)
                    print(f"Arquivo temporário removido: {caminho_arquivo_live}")
                except Exception as e:
                    print(f"Erro ao remover o arquivo temporário: {e}")
            else:
                print("Arquivo temporário já havia sido removido ou não encontrado.")

            # Remove _Live.parquet do Google Drive
            try:
                print(f"Solicitando remoção do arquivo _Live do Drive: {nome_arquivo_live}")
                delete_file_from_drive_by_name(service, nome_arquivo_live, pasta_drive_id)
            except Exception as e:
                print(f"Erro ao tentar excluir o arquivo _Live do Drive: {e}")

            # Envia ao Google Drive
            try:
                upload_to_drive(service, caminho_final, nome_final)
                print("Arquivo enviado ao Google Drive com sucesso.")
            except Exception as e:
                print(f"Erro ao enviar para o Google Drive: {e}")
        else:
            print("Nenhum dado consolidado para salvar.")

        ir.shutdown()

if __name__ == "__main__":
    main()

