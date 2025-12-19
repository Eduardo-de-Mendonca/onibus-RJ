from datetime import datetime
import mysql.connector
from mysql.connector import Error

def init_database():
    print("=" * 50)
    print("INICIALIZANDO BANCO DE DADOS...")
    
    try:
        conn = mysql.connector.connect(
            host='db',
            user='root',
            password='rootpassword'
        )
        cursor = conn.cursor()
        
        cursor.execute("CREATE DATABASE IF NOT EXISTS jae_onibus")
        print("Banco 'jae_onibus' verificado/criado")
        
        cursor.execute("USE jae_onibus")
        
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
        
        try:
            cursor.execute("SHOW INDEX FROM onibus WHERE Key_name = 'idx_unique_bus'")
            if cursor.fetchone():
                cursor.execute("DROP INDEX idx_unique_bus ON onibus")
                print("Índice único antigo removido")
        except:
            pass 
        
        indices = [
            ("idx_timestamp", "CREATE INDEX idx_timestamp ON onibus (timestamp)"),
            ("idx_linha", "CREATE INDEX idx_linha ON onibus (linha)"),
            ("idx_ordem", "CREATE INDEX idx_ordem ON onibus (ordem)"),
            ("idx_linha_ordem", "CREATE INDEX idx_linha_ordem ON onibus (linha, ordem)")
        ]
        
        for nome_indice, sql in indices:
            try:
                cursor.execute(sql)
                print(f"Índice '{nome_indice}' criado")
            except mysql.connector.Error as e:
                if "Duplicate key name" in str(e) or "1061" in str(e):
                    print(f"Índice '{nome_indice}' já existe")
                else:
                    print(f"Erro ao criar índice '{nome_indice}': {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("BANCO DE DADOS INICIALIZADO COM SUCESSO!")
        print("=" * 50)
        return True
        
    except Error as e:
        print(f"ERRO ao inicializar banco: {e}")
        return False
        
    except Error as e:
        print(f"ERRO ao inicializar banco: {e}")
        print("Verifique:")
        print("  1. MySQL está rodando?")
        print("  2. Usuário/senha corretos?")
        print("  3. Permissões adequadas?")
        return False

def get_connection():
    return mysql.connector.connect(
        host='db',
        user='root', 
        password='rootpassword',
        database='jae_onibus'
    )

def save_bus_data(dados_onibus):
    """Salva dados - Otimizado com INSERT em massa"""
    print(f"[DB] Iniciando salvamento de {len(dados_onibus):,} registros")
    
    if not dados_onibus:
        return {"success": True, "inserted": 0, "total_processed": 0}
    
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # 1. PREPARAR DADOS EM LOTE
        coleta_timestamp = datetime.now()
        batch_size = 5000  # Lotes de 5.000 registros
        total_inserts = 0
        
        print(f"[DB] Dividindo {len(dados_onibus):,} registros em lotes de {batch_size}...")
        
        # 2. PROCESSAR EM LOTES
        for i in range(0, len(dados_onibus), batch_size):
            batch = dados_onibus[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(dados_onibus) - 1) // batch_size + 1
            
            #print(f"[DB] Processando lote {batch_num}/{total_batches} ({len(batch):,} registros)...")
            
            # Preparar valores para INSERT em massa
            values = []
            for j, onibus in enumerate(batch):
                # Timestamp único para cada registro
                timestamp_unico = coleta_timestamp.replace(
                    microsecond=(i + j) % 1000000
                )
                
                values.append((
                    onibus.get('linha', '') or '',
                    onibus.get('ordem', '') or '',
                    float(onibus.get('velocidade', 0) or 0),
                    float(onibus.get('lat', 0) or 0),
                    float(onibus.get('lon', 0) or 0),
                    timestamp_unico
                ))
            
            # 3. INSERT EM MASSA (MUITO MAIS RÁPIDO)
            try:
                cursor.executemany('''
                    INSERT INTO onibus 
                    (linha, ordem, velocidade, latitude, longitude, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s)
                ''', values)
                
                inserts = cursor.rowcount
                total_inserts += inserts
                conn.commit()  # Commit após cada lote
                
                print(f"[DB] Lote {batch_num} salvo: {inserts:,} registros")
                
            except mysql.connector.Error as e:
                # Se falhar no lote, tentar registro por registro
                print(f"[DB AVISO] Erro no lote {batch_num}, tentando individualmente...")
                conn.rollback()
                
                inserts_individual = 0
                for value in values:
                    try:
                        cursor.execute('''
                            INSERT INTO onibus 
                            (linha, ordem, velocidade, latitude, longitude, timestamp)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        ''', value)
                        inserts_individual += 1
                    except:
                        continue  # Ignora erros individuais
                
                conn.commit()
                total_inserts += inserts_individual
                #print(f"[DB] Lote {batch_num} (individual): {inserts_individual:,}/{len(batch):,} salvos")
        
        # 4. RELATÓRIO FINAL
        total_recebido = len(dados_onibus)
        success_rate = (total_inserts / total_recebido * 100) if total_recebido > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"[DB] COLETA CONCLUÍDA COM SUCESSO!")
        print(f"{'='*50}")
        print(f"   Total recebido: {total_recebido:,} ônibus")
        print(f"   Total inserido: {total_inserts:,} registros")
        print(f"   Taxa de sucesso: {success_rate:.1f}%")
        print(f"{'='*50}")
        
        return {
            "success": True,
            "inserted": total_inserts,
            "total_processed": total_recebido,
            "success_rate": f"{success_rate:.1f}%"
        }
        
    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[DB ERRO] Erro crítico: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}
    finally:
        if conn:
            conn.close()
            print("[DB] Conexão MySQL fechada")

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
    row = cursor.fetchone()
    velocidade_media = float(row[0]) if row and row[0] is not None else 0.0
    
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
    row = cursor.fetchone()
    velocidade_media_hoje = float(row[0]) if row and row[0] is not None else 0.0
    
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
    

def get_invalid_lines():
    """Identifica linhas que parecem ser inválidas"""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute('''
        SELECT DISTINCT linha, COUNT(*) as total
        FROM onibus 
        WHERE linha IN ('0', '000', '00000', '') 
           OR linha IS NULL
           OR LENGTH(linha) < 2
        GROUP BY linha
        ORDER BY total DESC
    ''')
    
    invalid_lines = cursor.fetchall()
    conn.close()
    
    return invalid_lines