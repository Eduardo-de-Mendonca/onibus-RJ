from datetime import datetime
import mysql.connector
from mysql.connector import Error

import os

def get_connection():
    db_host = os.getenv('DB_HOST', 'localhost') 
    
    return mysql.connector.connect(
        host=db_host,
        user='root',
        password='rootroot',
        database='jae_onibus'
    )

def init_database():
    """Inicializa o banco - MySQL já deve estar criado"""
    print("MySQL conectado (banco já existe)")

def save_bus_data(dados_onibus):
    """Salva dados com timestamp ÚNICO para cada ônibus"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Timestamp base para esta coleta
    coleta_timestamp = datetime.now()
    
    try:
        inserts_realizados = 0
        
        for i, onibus in enumerate(dados_onibus):
            # Criar timestamp ÚNICO para cada ônibus
            # Adiciona microsegundos diferentes
            timestamp_unico = coleta_timestamp.replace(microsecond=i % 1000000)
            
            cursor.execute('''
                INSERT IGNORE INTO onibus 
                (linha, ordem, velocidade, latitude, longitude, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (
                onibus.get('linha'),
                onibus.get('ordem'), 
                onibus.get('velocidade'),
                onibus.get('lat'),
                onibus.get('lon'),
                timestamp_unico  # TIMESTAMP ÚNICO!
            ))
            inserts_realizados += cursor.rowcount
        
        conn.commit()
        
        total_recebido = len(dados_onibus)
        duplicatas_ignoradas = total_recebido - inserts_realizados
        
        print(f"COLETA CONCLUÍDA:")
        print(f"   Recebidos da API: {total_recebido} ônibus")
        print(f"   Inseridos no BD: {inserts_realizados} registros")
        print(f"   Duplicatas: {duplicatas_ignoradas}")
        
        return {
            "success": True,
            "inserted": inserts_realizados,
            "duplicates": duplicatas_ignoradas,
            "total_processed": total_recebido
        }
        
    except Exception as e:
        conn.rollback()
        print(f"Erro: {e}")
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
    """Calcula estatísticas dos dados no MySQL"""
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