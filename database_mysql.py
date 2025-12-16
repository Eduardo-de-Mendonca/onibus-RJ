from datetime import datetime
import mysql.connector
from mysql.connector import Error

def init_database():
    """Cria o banco e tabela se não existirem"""
    print("=" * 50)
    print("INICIALIZANDO BANCO DE DADOS...")
    
    try:
        # Primeiro conecta sem especificar o banco
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='rootroot'
        )
        cursor = conn.cursor()
        
        # 1. Cria banco se não existir
        cursor.execute("CREATE DATABASE IF NOT EXISTS jae_onibus")
        print("Banco 'jae_onibus' verificado/criado")
        
        cursor.execute("USE jae_onibus")
        
        # 2. Cria tabela
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS onibus (
                id INT AUTO_INCREMENT PRIMARY KEY,
                linha VARCHAR(20),
                ordem VARCHAR(50),
                velocidade FLOAT,
                latitude DECIMAL(10, 6),
                longitude DECIMAL(10, 6),
                timestamp DATETIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("Tabela 'onibus' criada/verificada")
        
        # 3. Remove índice único antigo se existir
        try:
            cursor.execute("DROP INDEX IF EXISTS idx_unique_bus ON onibus")
            print("Índice único antigo removido (se existia)")
        except:
            pass
        
        # 4. Cria índices para performance (NÃO únicos)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON onibus (timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_linha ON onibus (linha)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ordem ON onibus (ordem)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_linha_ordem ON onibus (linha, ordem)")
        print("Índices de performance criados")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("BANCO DE DADOS INICIALIZADO COM SUCESSO!")
        print("=" * 50)
        return True
        
    except Error as e:
        print(f"ERRO ao inicializar banco: {e}")
        print("Verifique:")
        print("  1. MySQL está rodando?")
        print("  2. Usuário/senha corretos?")
        print("  3. Permissões adequadas?")
        return False

def get_connection():
    """Cria conexão com o MySQL"""
    return mysql.connector.connect(
        host='localhost',
        user='root', 
        password='rootroot',
        database='jae_onibus'
    )

def save_bus_data(dados_onibus):
    """Salva dados - SEM IGNORE, sempre insere novos registros"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Timestamp base para esta coleta
    coleta_timestamp = datetime.now()
    
    try:
        inserts_realizados = 0
        
        for i, onibus in enumerate(dados_onibus):
            # Criar timestamp ÚNICO para cada ônibus
            timestamp_unico = coleta_timestamp.replace(microsecond=i % 1000000)
            
            # INSERT SEM IGNORE - sempre insere
            cursor.execute('''
                INSERT INTO onibus 
                (linha, ordem, velocidade, latitude, longitude, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                onibus.get('linha'),
                onibus.get('ordem'), 
                onibus.get('velocidade'),
                onibus.get('lat'),
                onibus.get('lon'),
                timestamp_unico
            ))
            inserts_realizados += cursor.rowcount
        
        conn.commit()
        
        total_recebido = len(dados_onibus)
        
        print(f"COLETA SALVA:")
        print(f"   Recebidos da API: {total_recebido} ônibus")
        print(f"   Inseridos no BD: {inserts_realizados} registros")
        
        return {
            "success": True,
            "inserted": inserts_realizados,
            "total_processed": total_recebido
        }
        
    except Exception as e:
        conn.rollback()
        print(f"Erro ao salvar: {e}")
        return {"success": False, "error": str(e)}
    finally:
        conn.close()

def get_recent_data(limit=1000):
    """Busca dados recentes do MySQL"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute('''
        SELECT linha, ordem, velocidade, latitude, longitude, timestamp
        FROM onibus 
        ORDER BY timestamp DESC 
        LIMIT %s
    ''', (limit,))
    
    dados = cursor.fetchall()
    conn.close()
    return dados

def get_bus_statistics():
    """Calcula estatísticas GERAIS de todos os dados no MySQL"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM onibus")
    total_registros = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT linha) FROM onibus")
    linhas_ativas = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(velocidade) FROM onibus WHERE velocidade IS NOT NULL")
    velocidade_media = cursor.fetchone()[0] or 0
    
    cursor.execute('''
        SELECT linha, COUNT(*) as quantidade 
        FROM onibus 
        GROUP BY linha 
        ORDER BY quantidade DESC 
        LIMIT 10
    ''')
    linhas_frequentes = []
    for linha, quantidade in cursor.fetchall():
        linhas_frequentes.append({
            'linha': linha,
            'quantidade': quantidade
        })
    
    conn.close()
    
    return {
        "total_registros": total_registros,
        "linhas_ativas": linhas_ativas,
        "velocidade_media_geral": round(velocidade_media, 2),
        "linhas_mais_frequentes": linhas_frequentes
    }

def get_todays_statistics():
    """Calcula estatísticas apenas dos dados de HOJE"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Conta registros de HOJE
    cursor.execute("""
        SELECT COUNT(*) 
        FROM onibus 
        WHERE DATE(timestamp) = CURDATE()
    """)
    registros_hoje = cursor.fetchone()[0]
    
    # Conta VEÍCULOS DISTINTOS de HOJE (pela ORDEM)
    cursor.execute("""
        SELECT COUNT(DISTINCT ordem)
        FROM onibus 
        WHERE DATE(timestamp) = CURDATE()
        AND ordem IS NOT NULL 
        AND ordem != ''
        AND ordem != '0'
    """)
    veiculos_distintos_hoje = cursor.fetchone()[0]
    
    # Velocidade média de HOJE
    cursor.execute("""
        SELECT AVG(velocidade) 
        FROM onibus 
        WHERE velocidade IS NOT NULL 
        AND DATE(timestamp) = CURDATE()
    """)
    velocidade_media_hoje = cursor.fetchone()[0] or 0
    
    # LINHAS DISTINTAS de HOJE
    cursor.execute("""
        SELECT COUNT(DISTINCT linha) 
        FROM onibus 
        WHERE DATE(timestamp) = CURDATE()
        AND linha IS NOT NULL 
        AND linha != ''
        AND linha != '0'
        AND linha != '000'
        AND linha != '00000'
    """)
    linhas_distintas_hoje = cursor.fetchone()[0]
    
    # Linhas mais frequentes HOJE
    cursor.execute('''
        SELECT linha, COUNT(*) as quantidade 
        FROM onibus 
        WHERE DATE(timestamp) = CURDATE()
        AND linha IS NOT NULL 
        AND linha != ''
        AND linha != '0'
        AND linha != '000'
        AND linha != '00000'
        GROUP BY linha 
        ORDER BY quantidade DESC 
        LIMIT 10
    ''')
    linhas_frequentes_hoje = []
    for linha, quantidade in cursor.fetchall():
        linhas_frequentes_hoje.append({
            'linha': linha,
            'quantidade': quantidade
        })
    
    conn.close()
    
    return {
        "registros_hoje": registros_hoje,
        "veiculos_distintos_hoje": veiculos_distintos_hoje,  # NOME ALTERADO
        "velocidade_media_hoje": round(velocidade_media_hoje, 2),
        "linhas_distintas_hoje": linhas_distintas_hoje,  # AGORA RETORNA CORRETAMENTE
        "linhas_frequentes_hoje": linhas_frequentes_hoje
    }

# Função para teste/debug
def test_connection():
    """Testa a conexão com o banco"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 'Conexão com MySQL OK' as status")
        result = cursor.fetchone()
        print(result[0])
        conn.close()
        return True
    except Error as e:
        print(f"Falha na conexão: {e}")
        return False
    

