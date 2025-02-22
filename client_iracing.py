from flask import Flask, request, jsonify
import os
import win32com.client
import socket
import winreg  # Para acessar o Registro do Windows

app = Flask(__name__)

def obter_ip_local():
    """Obtém o IP local da máquina automaticamente."""
    hostname = socket.gethostname()
    ip_local = socket.gethostbyname(hostname)
    return ip_local

@app.route('/get_ip', methods=['GET'])
def get_ip():
    """Endpoint que retorna o IP local do client."""
    ip = obter_ip_local()
    return jsonify({"ip": ip})

@app.route('/dados', methods=['GET'])
def get_dados():
    """Simulação de um endpoint que retorna dados do iRacing."""
    dados = {
        "piloto": "Nome Exemplo",
        "tempo_volta": "1:35.678",
        "combustivel": "10.5L"
    }
    return jsonify(dados)

def obter_caminho_iracing():
    """
    Tenta obter automaticamente o caminho do iRacing verificando:
    1. Diretamente no Registro do Windows
    2. No caminho padrão da Steam
    """
    try:
        # Verifica no Registro do Windows (instalação padrão do iRacing)
        chave_iracing = r"SOFTWARE\WOW6432Node\iRacing.com"
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, chave_iracing) as chave:
            caminho, _ = winreg.QueryValueEx(chave, "InstallDir")
            return os.path.join(caminho, "iRacingUI.exe")
    
    except Exception:
        print("⚠️ Caminho do iRacing não encontrado no Registro do Windows.")

    # Caminho padrão da Steam (se instalado via Steam)
    caminhos_steam = [
        r"C:\Program Files (x86)\Steam\steamapps\common\iRacing\ui\iRacingUI.exe",
        r"C:\Program Files\Steam\steamapps\common\iRacing\ui\iRacingUI.exe",
        os.path.expanduser(r"~\AppData\Local\Steam\steamapps\common\iRacing\ui\iRacingUI.exe")
    ]
    
    for caminho in caminhos_steam:
        if os.path.exists(caminho):
            return caminho

    return None  # Se não encontrar, retorna None

def abrir_iracing_local():
    """
    Abre o iRacing automaticamente, independente de onde ele esteja instalado.
    """
    caminho_iracing = obter_caminho_iracing()
    
    if caminho_iracing and os.path.exists(caminho_iracing):
        try:
            shell = win32com.client.Dispatch("WScript.Shell")
            shell.Run(f'"{caminho_iracing}"')
            return True
        except Exception as e:
            print(f"Erro ao abrir o iRacing: {e}")
            return False
    else:
        print("❌ Caminho do iRacing não encontrado!")
        return False

@app.route('/abrir_iracing', methods=['GET'])
def abrir_iracing():
    """Endpoint para abrir o iRacing remotamente."""
    if abrir_iracing_local():
        return jsonify({"mensagem": "iRacing sendo aberto via pywin32..."}), 200
    else:
        return jsonify({"erro": "Falha ao abrir o iRacing. Verifique a instalação."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
