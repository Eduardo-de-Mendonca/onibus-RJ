from flask import Flask, jsonify, request
from flask_cors import CORS
import collector 
import database_mysql as database 
from datetime import datetime
import schedule
import time
import threading

# ========== INICIALIZAÇÃO DO BANCO ==========
print("=" * 50)
print("VERIFICANDO/CRIANDO BANCO DE DADOS...")
print("=" * 50)

try:
    if database.init_database():
        print("Banco de dados inicializado com sucesso!")
    else:
        print("Falha ao inicializar banco de dados")
        print("Continuando sem banco... (alguns endpoints não funcionarão)")
except Exception as e:
    print(f"Erro na inicialização do banco: {e}")
    print("Continuando...")

print("=" * 50)

# Inicializa a API
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
    
    coletando = False  # Flag para controlar execução única
    
    def coletar_e_salvar():
        """Função que coleta e salva dados uma vez"""
        nonlocal coletando
        
        # Se já estiver coletando, ignora
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
                    "message": f"Inserção concluída: {resultado['inserted']} novos registros",
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
    """Retorna análises e estatísticas GERAIS (todos os dados)"""
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

@app.route('/api/analises/estatisticas_hoje', methods=['GET'])
def get_estatisticas_hoje():
    """Retorna análises e estatísticas apenas dos dados de HOJE"""
    try:
        estatisticas_hoje = database.get_todays_statistics()
        estatisticas_gerais = database.get_bus_statistics()  # Mantém as gerais também
        
        return jsonify({
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "estatisticas_hoje": estatisticas_hoje,
            "estatisticas_gerais": estatisticas_gerais
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
        "coleta_automatica": "ativa (10 minutos)",
        "ultima_coleta": datetime.now().strftime('%H:%M:%S')
    })

@app.route('/api/analises/linhas_invalidas', methods=['GET'])
def get_linhas_invalidas():
    """Retorna linhas que parecem ser inválidas"""
    try:
        invalidas = database.get_invalid_lines()
        
        return jsonify({
            "success": True,
            "total_invalidas": len(invalidas),
            "linhas_invalidas": invalidas
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# ========== INICIALIZAÇÃO ==========

if __name__ == '__main__':
    print("Iniciando API Jaé com Coleta Automática...")
    print("Endpoints disponíveis:")
    print("   GET  /api/onibus/atual               - Dados em tempo real")
    print("   POST /api/onibus/salvar              - Salva dados no banco (MANUAL)") 
    print("   GET  /api/onibus/historico           - Dados históricos")
    print("   GET  /api/analises/estatisticas      - Estatísticas GERAIS")
    print("   GET  /api/analises/estatisticas_hoje - Estatísticas de HOJE")
    print("   GET  /api/health                     - Status da API")
    print("\nConfiguração do Coletor Automático:")
    print("   Intervalo: 10 minutos")
    print("   Proteção: Prevenção de execuções sobrepostas")
    print("   Estimado: ~1.500 registros/coleta")
    print("   Estimado: ~9.000 registros/hora")
    print("   Use POST /api/onibus/salvar para coleta manual extra")
    print("\n" + "="*50)
    
    app.run(host='0.0.0.0', port=5000, debug=True)

