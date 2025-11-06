from flask import Flask, jsonify, request
from flask_cors import CORS
import collector 
import database
from datetime import datetime

def popular_banco_inicial():
    """Salva alguns dados automaticamente ao iniciar"""
    try:
        print("Populando banco com dados iniciais...")
        dados = collector.search_bus_data()
        if dados:
            database.save_bus_data(dados)
            print(f"Dados iniciais salvos: {len(dados)} registros")
        else:
            print("Não foi possível coletar dados iniciais")
    except Exception as e:
        print(f"Erro ao popular banco: {e}")

# Chama a função para popular o banco quando o arquivo é executado
popular_banco_inicial()


# Inicializa a API
app = Flask(__name__)
CORS(app)  # Permite requests do frontend

# Inicializa o banco de dados
database.init_database()

# ========== ENDPOINTS PRINCIPAIS ==========

@app.route('/api/onibus/atual', methods=['GET'])
def get_onibus_atual():
    """Retorna dados em tempo real (usa o coletor)"""
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
    """Salva dados atuais no banco"""
    try:
        dados = collector.search_bus_data()
        if dados:
            database.save_bus_data(dados)
            return jsonify({
                "success": True,
                "message": f"Dados salvos com sucesso: {len(dados)} registros",
                "timestamp": datetime.now().isoformat()
            })
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
            "data": dados  # Já é uma lista de dicionários
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
        "service": "API Jaé - Dados Ônibus Rio"
    })

# ========== INICIALIZAÇÃO ==========

if __name__ == '__main__':
    print("Iniciando API Jaé...")
    print("Endpoints disponíveis:")
    print("   GET  /api/onibus/atual        - Dados em tempo real")
    print("   POST /api/onibus/salvar       - Salva dados no banco") 
    print("   GET  /api/onibus/historico    - Dados históricos")
    print("   GET  /api/analises/estatisticas - Estatísticas")
    print("   GET  /api/health              - Status da API")
    
    app.run(debug=True, host='0.0.0.0', port=5000)