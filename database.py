import sqlite3
import json
from datetime import datetime, timedelta

def init_database():
    """Inicializa o banco de dados SEM pandas"""
    conn = sqlite3.connect('data/onibus.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS onibus (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            linha TEXT NOT NULL,
            ordem TEXT NOT NULL,
            velocidade REAL,
            latitude REAL,
            longitude REAL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    print("✅ Banco de dados inicializado!")

def save_bus_data(dados_onibus):
    """Salva dados SEM pandas"""
    conn = sqlite3.connect('data/onibus.db')
    cursor = conn.cursor()
    
    for onibus in dados_onibus:
        cursor.execute('''
            INSERT INTO onibus (linha, ordem, velocidade, latitude, longitude)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            onibus.get('linha'),
            onibus.get('ordem'), 
            onibus.get('velocidade'),
            onibus.get('lat'),
            onibus.get('lon')
        ))
    
    conn.commit()
    conn.close()
    print(f"✅ Dados salvos: {len(dados_onibus)} registros")

def get_recent_data(limit=1000):
    """Busca dados recentes SEM pandas - VERSÃO CORRIGIDA"""
    conn = sqlite3.connect('data/onibus.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT linha, ordem, velocidade, latitude, longitude, timestamp
        FROM onibus 
        ORDER BY timestamp DESC 
        LIMIT ?
    ''', (limit,))
    
    resultados = cursor.fetchall()
    conn.close()
    
    # Converte para lista de dicionários 
    dados = []
    for row in resultados:
        dados.append({
            'linha': row[0],
            'ordem': row[1],
            'velocidade': row[2],
            'latitude': row[3],
            'longitude': row[4],
            'timestamp': row[5]
        })
    
    return dados  

def get_bus_statistics():
    """Calcula estatísticas SEM pandas"""
    conn = sqlite3.connect('data/onibus.db')
    cursor = conn.cursor()
    
    # Total de registros
    cursor.execute("SELECT COUNT(*) FROM onibus")
    total_registros = cursor.fetchone()[0]
    
    # Linhas ativas
    cursor.execute("SELECT COUNT(DISTINCT linha) FROM onibus")
    linhas_ativas = cursor.fetchone()[0]
    
    # Velocidade média
    cursor.execute("SELECT AVG(velocidade) FROM onibus WHERE velocidade IS NOT NULL")
    velocidade_media = cursor.fetchone()[0] or 0
    
    # Linhas mais frequentes
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