from flask import Flask, jsonify, request
from flask_cors import CORS
import collector 
import database_mysql as database 
from datetime import datetime
import schedule
import time
import threading

# Inicializa a API
app = Flask(__name__)
CORS(app)

# ========== COLETOR AUTOMÁTICO CONTROLADO ==========

def coletor_automatico():
    """Coleta dados automaticamente a cada 10 minutos de forma CONTROLADA"""
    print("Coletor automático INICIADO (intervalo: 10 minutos)")
    
    def coletar_e_salvar():
        """Função que coleta e salva dados uma vez"""
        try:
            print(f"Coletando dados automáticos... {datetime.now().strftime('%H:%M:%S')}")
            dados = collector.search_bus_data()
            if dados:
                database.save_bus_data(dados)
                print(f"Coleta automática concluída: {len(dados)} ônibus")
            else:
                print("Coleta automática: nenhum dado retornado")
        except Exception as e:
            print(f"Erro na coleta automática: {e}")
    
    # CONFIGURAÇÃO SEGURA: 10 MINUTOS
    schedule.every(10).minutes.do(coletar_e_salvar)
    
    print("Coletor automático configurado: 1 coleta a cada 10 minutos")
    print("Estimativa: ~1.500 registros por coleta | ~9.000 registros/hora")
    
    # Loop do agendador
    while True:
        schedule.run_pending()
        time.sleep(30)  # Verifica a cada 30 segundos (mais eficiente)

# Iniciar coletor automático em thread separada
threading.Thread(target=coletor_automatico, daemon=True).start()

# ========== ENDPOINTS PRINCIPAIS ==========

@app.route('/api/onibus/atual', methods=['GET'])
def get_onibus_atual():
    """Retorna dados em tempo real"""
    try:
        dados = collector.search_bus_data()
        if dados:
            return jsonify({
                "success": True,
                "timestamp": datetime.now().isoformat(),
                "total_onibus": len(dados),
                "data": dados
            })
        else:
            return jsonify({
                "success": False,
                "error": "Não foi possível coletar dados"
            }), 500
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/onibus/salvar', methods=['POST'])
def salvar_dados():
    """Salva dados atuais no banco - MANUAL"""
    try:
        dados = collector.search_bus_data()
        if dados:
            resultado = database.save_bus_data(dados)
            
            if resultado["success"]:
                return jsonify({
                    "success": True,
                    "message": f"Inserção concluída: {resultado['inserted']} novos registros, {resultado['duplicates']} duplicatas ignoradas",
                    "timestamp": datetime.now().isoformat(),
                    "statistics": resultado
                })
            else:
                return jsonify({
                    "success": False,
                    "error": resultado["error"]
                }), 500
        else:
            return jsonify({
                "success": False,
                "error": "Não há dados para salvar"
            }), 400
    except Exception as e:
        return jsonify({
            "success": False, 
            "error": str(e)
        }), 500

@app.route('/api/onibus/historico', methods=['GET'])
def get_historico():
    """Retorna dados históricos do banco"""
    try:
        limit = request.args.get('limit', 1000, type=int)
        dados = database.get_recent_data(limit)
        
        return jsonify({
            "success": True,
            "total_registros": len(dados),
            "data": dados
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/analises/estatisticas', methods=['GET'])
def get_estatisticas():
    """Retorna análises e estatísticas"""
    try:
        estatisticas = database.get_bus_statistics()
        
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "estatisticas": estatisticas
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint para verificar se a API está funcionando"""
    return jsonify({
        "status": "online",
        "timestamp": datetime.now().isoformat(),
        "service": "API Jaé - Dados Ônibus Rio",
        "coleta_automatica": "ativa (5 minutos)"
    })

# ========== INICIALIZAÇÃO ==========

if __name__ == '__main__':
    print("Iniciando API Jaé com Coleta Automática...")
    print("Endpoints disponíveis:")
    print("   GET  /api/onibus/atual        - Dados em tempo real")
    print("   POST /api/onibus/salvar       - Salva dados no banco (MANUAL)") 
    print("   GET  /api/onibus/historico    - Dados históricos")
    print("   GET  /api/analises/estatisticas - Estatísticas")
    print("   GET  /api/health              - Status da API")
    print("\nConfiguração do Coletor Automático:")
    print("   Intervalo: 10 minutos")
    print("   Estimado: ~1.500 registros/coleta")
    print("   Estimado: ~9.000 registros/hora")
    print("   Use POST /api/onibus/salvar para coleta manual extra")
    print("\n" + "="*50)
    
    app.run(debug=False, host='0.0.0.0', port=5000)