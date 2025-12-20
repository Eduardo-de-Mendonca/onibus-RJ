from flask import Flask, jsonify, request
from flask_cors import CORS
import collector
import database_mysql as database
from datetime import datetime
import schedule
import time
import threading
import sys

# ========== ESPERA ATIVA PELO BANCO ==========
print("=" * 50)
print("AGUARDANDO MYSQL FICAR DISPONÍVEL...")
print("=" * 50)

MAX_TENTATIVAS = 30
tentativa = 0
banco_ok = False

while tentativa < MAX_TENTATIVAS:
    try:
        if database.init_database():
            banco_ok = True
            print("Banco de dados conectado e pronto!")
            break
    except Exception as e:
        tentativa += 1
        print(f"Tentativa {tentativa}/{MAX_TENTATIVAS} - MySQL indisponível: {e}")
        time.sleep(5)

if not banco_ok:
    print("ERRO CRÍTICO: Banco de dados não iniciou.")
    print("Encerrando API para evitar funcionamento inconsistente.")
    sys.exit(1)

print("=" * 50)

# ========== INICIALIZAÇÃO DA API ==========
app = Flask(__name__)
CORS(app, origins="*", supports_credentials=True)

@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response

# ========== COLETOR AUTOMÁTICO CONTROLADO ==========
def coletor_automatico():
    """Coleta dados automaticamente a cada 10 minutos com prevenção de sobreposição"""
    print("Coletor automático INICIADO (intervalo: 10 minutos)")
    
    coletando = False
    
    def coletar_e_salvar():
        nonlocal coletando

        if coletando:
            print(f"Coleta já em andamento, ignorando... {datetime.now().strftime('%H:%M:%S')}")
            return

        try:
            coletando = True
            print(f"Coletando dados automáticos... {datetime.now().strftime('%H:%M:%S')}")
            dados = collector.search_bus_data()
            if dados:
                database.save_bus_data(dados)
                print(f"Coleta automática concluída: {len(dados)} ônibus")
            else:
                print("Coleta automática: nenhum dado retornado")
        except Exception as e:
            print(f"Erro na coleta automática: {e}")
        finally:
            coletando = False

    schedule.every(10).minutes.do(coletar_e_salvar)

    print("Coletor automático configurado: 1 coleta a cada 10 minutos")
    print("Estimativa: ~1.500 registros por coleta | ~9.000 registros/hora")

    while True:
        schedule.run_pending()
        time.sleep(30)

# Inicia coletor automático SOMENTE com banco disponível
threading.Thread(target=coletor_automatico, daemon=True).start()

# ========== ENDPOINTS ==========
@app.route('/api/onibus/atual')
def get_onibus_atual():
    dados = database.get_recent_data(200)
    return jsonify({
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "total_onibus": len(dados),
        "data": dados
    })

@app.route('/api/onibus/salvar', methods=['POST'])
def salvar_dados():
    try:
        dados = collector.search_bus_data()
        if not dados:
            return jsonify({"success": False, "error": "Sem dados para salvar"}), 400

        resultado = database.save_bus_data(dados)
        if resultado["success"]:
            return jsonify({
                "success": True,
                "message": f"Inserção concluída: {resultado['inserted']} registros",
                "timestamp": datetime.now().isoformat(),
                "statistics": resultado
            })
        return jsonify({"success": False, "error": resultado["error"]}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/onibus/historico', methods=['GET'])
def get_historico():
    try:
        limit = request.args.get('limit', 1000, type=int)
        dados = database.get_recent_data(limit)
        return jsonify({"success": True, "total_registros": len(dados), "data": dados})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/analises/estatisticas', methods=['GET'])
def get_estatisticas():
    try:
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "estatisticas": database.get_bus_statistics()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/analises/estatisticas_hoje', methods=['GET'])
def get_estatisticas_hoje():
    try:
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "estatisticas_hoje": database.get_todays_statistics(),
            "estatisticas_gerais": database.get_bus_statistics()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    
@app.route('/api/analises/ultima_coleta', methods=['GET'])
def get_estatisticas_ultima_coleta():
    try:
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "estatisticas": database.get_last_collection_statistics()
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/api/analises/linhas_invalidas', methods=['GET'])
def get_linhas_invalidas():
    try:
        invalidas = database.get_invalid_lines()
        return jsonify({
            "success": True,
            "total_invalidas": len(invalidas),
            "linhas_invalidas": invalidas
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "online",
        "timestamp": datetime.now().isoformat(),
        "service": "API Jaé - Dados Ônibus Rio",
        "coleta_automatica": "ativa (10 minutos)"
    })


# ========== START ==========
if __name__ == '__main__':
    print("API Jaé iniciada com sucesso.")
    app.run(host='0.0.0.0', port=5000, debug=True)
