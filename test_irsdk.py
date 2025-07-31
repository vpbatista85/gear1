import irsdk
import polars as pl
import time
import os
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# === Configurações ===
SERVICE_ACCOUNT_FILE = 'C:/Users/vpb85/Documents/Gear1/gear1-ir-36de8419de96.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']
PARQUET_FILENAME = 'telemetria_iracing.parquet'
LOCAL_DIR = os.path.expanduser("~/Documents/iracing_parquet")
LOCAL_PATH = os.path.join(LOCAL_DIR, PARQUET_FILENAME)
UPLOAD_INTERVAL = 300  # frames (cada 300 coletas = 1 envio)

# === Autenticação com Google Drive ===
def authenticate_google_drive():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=creds)

# === Envio do arquivo ao Google Drive ===
DRIVE_FOLDER_ID = '1Ix44ranjPTYSPMN6W9YhwXqQSCC94uRZ'  # <-- Substitua pelo seu ID real

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
def salvar_incremental(data_batch):
    df = pl.DataFrame(data_batch)
    if os.path.exists(LOCAL_PATH):
        existing_df = pl.read_parquet(LOCAL_PATH)
        df = existing_df.vstack(df)
    df.write_parquet(LOCAL_PATH)


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


# === Coleta de dados do iRacing e envio ===
def main():
    ir = irsdk.IRSDK()
    wait_for_iracing(ir)  # <-- ESSENCIAL

    os.makedirs(LOCAL_DIR, exist_ok=True)
    data_batch = []
    frame_count = 0
    service = authenticate_google_drive()
    var_list=['AirDensity', 'AirPressure', 'AirTemp', 'Brake', 'BrakeABSactive', 'BrakeRaw', 'CamCameraNumber', 'CamCameraState', 'CamCarIdx', 'CamGroupNumber', 'CarDistAhead', 'CarDistBehind', 'CarIdxBestLapNum', 'CarIdxBestLapTime', 'CarIdxClass', 'CarIdxClassPosition', 'CarIdxEstTime', 'CarIdxF2Time', 'CarIdxFastRepairsUsed', 'CarIdxGear', 'CarIdxLap', 'CarIdxLapCompleted', 'CarIdxLapDistPct', 'CarIdxLastLapTime', 'CarIdxOnPitRoad', 'CarIdxP2P_Count', 'CarIdxP2P_Status', 'CarIdxPaceFlags', 'CarIdxPaceLine', 'CarIdxPaceRow', 'CarIdxPosition', 'CarIdxQualTireCompound', 'CarIdxQualTireCompoundLocked', 'CarIdxRPM', 'CarIdxSessionFlags', 'CarIdxSteer', 'CarIdxTireCompound', 'CarIdxTrackSurface', 'CarIdxTrackSurfaceMaterial', 'CarLeftRight', 'ChanAvgLatency', 'ChanClockSkew', 'ChanLatency', 'ChanPartnerQuality', 'ChanQuality', 'Clutch', 'ClutchRaw', 'CpuUsageBG', 'CpuUsageFG', 'DCDriversSoFar', 'DCLapStatus', 'dcPitSpeedLimiterToggle', 'dcStarter', 'dcToggleWindshieldWipers', 'dcTriggerWindshieldWipers', 'DisplayUnits', 'dpFastRepair', 'dpFuelAddKg', 'dpFuelAutoFillActive', 'dpFuelAutoFillEnabled', 'dpFuelFill', 'dpLFTireChange', 'dpLFTireColdPress', 'dpLRTireChange', 'dpLRTireColdPress', 'dpRFTireChange', 'dpRFTireColdPress', 'dpRRTireChange', 'dpRRTireColdPress', 'dpWindshieldTearoff', 'DriverMarker', 'Engine0_RPM', 'EngineWarnings', 'EnterExitReset', 'FastRepairAvailable', 'FastRepairUsed', 'FogLevel', 'FrameRate', 'FrontTireSetsAvailable', 'FrontTireSetsUsed', 'FuelLevel', 'FuelLevelPct', 'FuelPress', 'FuelUsePerHour', 'Gear', 'GpuUsage', 'HandbrakeRaw', 'IsDiskLoggingActive', 'IsDiskLoggingEnabled', 'IsGarageVisible', 'IsInGarage', 'IsOnTrack', 'IsOnTrackCar', 'IsReplayPlaying', 'Lap', 'LapBestLap', 'LapBestLapTime', 'LapBestNLapLap', 'LapBestNLapTime', 'LapCompleted', 'LapCurrentLapTime', 'LapDeltaToBestLap', 'LapDeltaToBestLap_DD', 'LapDeltaToBestLap_OK', 'LapDeltaToOptimalLap', 'LapDeltaToOptimalLap_DD', 'LapDeltaToOptimalLap_OK', 'LapDeltaToSessionBestLap', 'LapDeltaToSessionBestLap_DD', 'LapDeltaToSessionBestLap_OK', 'LapDeltaToSessionLastlLap', 'LapDeltaToSessionLastlLap_DD', 'LapDeltaToSessionLastlLap_OK', 'LapDeltaToSessionOptimalLap', 'LapDeltaToSessionOptimalLap_DD', 'LapDeltaToSessionOptimalLap_OK', 'LapDist', 'LapDistPct', 'LapLasNLapSeq', 'LapLastLapTime', 'LapLastNLapTime', 'LatAccel', 'LatAccel_ST', 'LeftTireSetsAvailable', 'LeftTireSetsUsed', 'LFbrakeLinePress', 'LFcoldPressure', 'LFodometer', 'LFshockDefl', 'LFshockDefl_ST', 'LFshockVel', 'LFshockVel_ST', 'LFtempCL', 'LFtempCM', 'LFtempCR', 'LFTiresAvailable', 'LFTiresUsed', 'LFwearL', 'LFwearM', 'LFwearR', 'LoadNumTextures', 'LongAccel', 'LongAccel_ST', 'LRbrakeLinePress', 'LRcoldPressure', 'LRodometer', 'LRshockDefl', 'LRshockDefl_ST', 'LRshockVel', 'LRshockVel_ST', 'LRtempCL', 'LRtempCM', 'LRtempCR', 'LRTiresAvailable', 'LRTiresUsed', 'LRwearL', 'LRwearM', 'LRwearR', 'ManifoldPress', 'ManualBoost', 'ManualNoBoost', 'MemPageFaultSec', 'MemSoftPageFaultSec', 'OilLevel', 'OilPress', 'OilTemp', 'OkToReloadTextures', 'OnPitRoad', 'P2P_Count', 'P2P_Status', 'PaceMode', 'Pitch', 'PitchRate', 'PitchRate_ST', 'PitOptRepairLeft', 'PitRepairLeft', 'PitsOpen', 'PitstopActive', 'PitSvFlags', 'PitSvFuel', 'PitSvLFP', 'PitSvLRP', 'PitSvRFP', 'PitSvRRP', 'PitSvTireCompound', 'PlayerCarClass', 'PlayerCarClassPosition', 'PlayerCarDriverIncidentCount', 'PlayerCarDryTireSetLimit', 'PlayerCarIdx', 'PlayerCarInPitStall', 'PlayerCarMyIncidentCount', 'PlayerCarPitSvStatus', 'PlayerCarPosition', 'PlayerCarPowerAdjust', 'PlayerCarSLBlinkRPM', 'PlayerCarSLFirstRPM', 'PlayerCarSLLastRPM', 'PlayerCarSLShiftRPM', 'PlayerCarTeamIncidentCount', 'PlayerCarTowTime', 'PlayerCarWeightPenalty', 'PlayerFastRepairsUsed', 'PlayerIncidents', 'PlayerTireCompound', 'PlayerTrackSurface', 'PlayerTrackSurfaceMaterial', 'Precipitation', 'PushToPass', 'PushToTalk', 'RaceLaps', 'RadioTransmitCarIdx', 'RadioTransmitFrequencyIdx', 'RadioTransmitRadioIdx', 'RearTireSetsAvailable', 'RearTireSetsUsed', 'RelativeHumidity', 'RFbrakeLinePress', 'RFcoldPressure', 'RFodometer', 'RFshockDefl', 'RFshockDefl_ST', 'RFshockVel', 'RFshockVel_ST', 'RFtempCL', 'RFtempCM', 'RFtempCR', 'RFTiresAvailable', 'RFTiresUsed', 'RFwearL', 'RFwearM', 'RFwearR', 'RightTireSetsAvailable', 'RightTireSetsUsed', 'Roll', 'RollRate', 'RollRate_ST', 'RPM', 'RRbrakeLinePress', 'RRcoldPressure', 'RRodometer', 'RRshockDefl', 'RRshockDefl_ST', 'RRshockVel', 'RRshockVel_ST', 'RRtempCL', 'RRtempCM', 'RRtempCR', 'RRTiresAvailable', 'RRTiresUsed', 'RRwearL', 'RRwearM', 'RRwearR', 'SessionFlags', 'SessionJokerLapsRemain', 'SessionLapsRemain', 'SessionLapsRemainEx', 'SessionLapsTotal', 'SessionNum', 'SessionOnJokerLap', 'SessionState', 'SessionTick', 'SessionTime', 'SessionTimeOfDay', 'SessionTimeRemain', 'SessionTimeTotal', 'SessionUniqueID', 'Shifter', 'ShiftGrindRPM', 'ShiftPowerPct', 'Skies', 'SolarAltitude', 'SolarAzimuth', 'Speed', 'SteeringFFBEnabled', 'SteeringWheelAngle', 'SteeringWheelAngleMax', 'SteeringWheelLimiter', 'SteeringWheelMaxForceNm', 'SteeringWheelPctDamper', 'SteeringWheelPctIntensity', 'SteeringWheelPctSmoothing', 'SteeringWheelPctTorque', 'SteeringWheelPctTorqueSign', 'SteeringWheelPctTorqueSignStops', 'SteeringWheelPeakForceNm', 'SteeringWheelTorque', 'SteeringWheelTorque_ST', 'SteeringWheelUseLinear', 'Throttle', 'ThrottleRaw', 'TireLF_RumblePitch', 'TireLR_RumblePitch', 'TireRF_RumblePitch', 'TireRR_RumblePitch', 'TireSetsAvailable', 'TireSetsUsed', 'TrackTempCrew', 'TrackWetness', 'VelocityX', 'VelocityX_ST', 'VelocityY', 'VelocityY_ST', 'VelocityZ', 'VelocityZ_ST', 'VertAccel', 'VertAccel_ST', 'VidCapActive', 'VidCapEnabled', 'Voltage', 'WaterLevel', 'WaterTemp', 'WeatherDeclaredWet', 'WindDir', 'WindVel', 'Yaw', 'YawNorth', 'YawRate', 'YawRate_ST']

    try:
        while True:
            if ir.is_initialized and ir.is_connected:
                ir.freeze_var_buffer_latest()
                frame_data = {"timestamp": datetime.utcnow().isoformat()}
                for var_name in var_list:
                    try:
                        frame_data[var_name] = ir[var_name]
                    except:
                        frame_data[var_name] = None

                data_batch.append(frame_data)
                frame_count += 1

                if frame_count % UPLOAD_INTERVAL == 0:
                    salvar_incremental(data_batch)
                    upload_to_drive(service, LOCAL_PATH, PARQUET_FILENAME)
                    print(f"[{frame_count}] Dados salvos e enviados ao Google Drive.")
                    data_batch.clear()

                time.sleep(0.2)
            else:
                print("Aguardando sessão iRacing...")
                time.sleep(1)

    except KeyboardInterrupt:
        print("Encerrando...")
        if data_batch:
            salvar_incremental(data_batch)
            upload_to_drive(service, LOCAL_PATH, PARQUET_FILENAME)
            print("Dados finais salvos e enviados ao Google Drive.")
        ir.shutdown()

if __name__ == "__main__":
    main()

