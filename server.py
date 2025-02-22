from flask import Flask, jsonify
import os
import win32com.client


def abrir_iracing_local():
    try:
        caminho_iracing = r"C:\Program Files (x86)\iRacing\iRacingLauncher.exe"  # Ajuste se necessário
        shell = win32com.client.Dispatch("WScript.Shell")
        shell.Run(f'"{caminho_iracing}"')
        return True
    except Exception as e:
        print(f"Erro ao abrir o iRacing: {e}")
        return False


app = Flask(__name__)

@app.route('/dados', methods=['GET'])
def get_dados():
    dados = [
        {"piloto": "Max Verstappen", "voltas": 58, "melhor_tempo": "1:30.123"},
        {"piloto": "Lewis Hamilton", "voltas": 58, "melhor_tempo": "1:30.456"}
    ]
    return jsonify(dados)


@app.route('/abrir_iracing', methods=['GET'])
def abrir_iracing():
    if abrir_iracing_local():
        return jsonify({"mensagem": "iRacing sendo aberto via pywin32..."}), 200
    else:
        return jsonify({"erro": "Falha ao abrir o iRacing"}), 500
# @app.route('/abrir_iracing', methods=['GET'])
# def abrir_iracing():
#     try:
#         url = "https://iracing.link/"
#         webbrowser.open(url)  # Abre o iRacing
#         return jsonify({"mensagem": "iRacing sendo aberto..."}), 200
#     except Exception as e:
#         return jsonify({"erro": str(e)}), 500



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
