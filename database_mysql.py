from datetime import datetime
import mysql.connector
from mysql.connector import Error
import time

# ========== CONFIG ==========
DB_CONFIG = {
    "host": "db",
    "user": "jae_user",
    "password": "jae_password",
    "database": "jae_onibus",
    "port": 3306
}

# ========== INIT ==========
def init_database():
    tentativas_max = 10
    espera = 3  # segundos

    for tentativa in range(1, tentativas_max + 1):
        try:
            print(f"Tentando conectar ao banco (tentativa {tentativa}/{tentativas_max})...")

            conn = mysql.connector.connect(
                host="db",
                user="jae_user",
                password="jae_password",
                database="jae_onibus",
                port=3306
            )

            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS onibus (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    linha VARCHAR(20),
                    ordem VARCHAR(50),
                    velocidade FLOAT,
                    latitude DECIMAL(10, 6),
                    longitude DECIMAL(10, 6),
                    timestamp DATETIME,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            conn.commit()
            cursor.close()
            conn.close()

            print("Banco conectado e inicializado com sucesso!")
            return True

        except Exception as e:
            print(f"Banco ainda não disponível: {e}")
            time.sleep(espera)

    print("Falha ao conectar ao banco após várias tentativas.")
    return False


# ========== CONNECTION ==========
def get_connection(retries=5, delay=3):
    for tentativa in range(1, retries + 1):
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except Error as e:
            print(f"[DB] Tentativa {tentativa}/{retries} - MySQL indisponível")
            time.sleep(delay)

    raise ConnectionError("MySQL indisponível após múltiplas tentativas")


# ========== INSERT ==========
def save_bus_data(dados_onibus):
    print(f"[DB] Iniciando salvamento de {len(dados_onibus)} registros")

    if not dados_onibus:
        return {"success": True, "inserted": 0, "total_processed": 0}

    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        coleta_timestamp = datetime.now()
        values = []

        for i, onibus in enumerate(dados_onibus):
            values.append((
                onibus.get('linha', '') or '',
                onibus.get('ordem', '') or '',
                float(onibus.get('velocidade', 0) or 0),
                float(onibus.get('lat', 0) or 0),
                float(onibus.get('lon', 0) or 0),
                coleta_timestamp
            ))

        cursor.executemany('''
            INSERT INTO onibus 
            (linha, ordem, velocidade, latitude, longitude, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
        ''', values)

        conn.commit()

        return {
            "success": True,
            "inserted": cursor.rowcount,
            "total_processed": len(dados_onibus)
        }

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"[DB ERRO] {e}")
        return {"success": False, "error": str(e)}

    finally:
        if conn:
            conn.close()


# ========== READ ==========
def get_recent_data(limit=1000):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute('''
            SELECT linha, ordem, velocidade, latitude, longitude, timestamp
            FROM onibus
            ORDER BY timestamp DESC
            LIMIT %s
        ''', (limit,))

        return cursor.fetchall()

    finally:
        if conn:
            conn.close()


def get_bus_statistics():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM onibus")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT linha) FROM onibus")
        linhas = cursor.fetchone()[0]

        cursor.execute("SELECT AVG(velocidade) FROM onibus")
        vel = cursor.fetchone()[0] or 0

        return {
            "total_registros": total,
            "linhas_ativas": linhas,
            "velocidade_media_geral": round(float(vel), 2)
        }

    finally:
        if conn:
            conn.close()


def get_todays_statistics():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("""
        SELECT
            COUNT(*) AS registros_hoje,
            COUNT(DISTINCT linha) AS linhas_distintas_hoje,
            COUNT(DISTINCT ordem) AS veiculos_distintos_hoje,
            AVG(velocidade) AS velocidade_media_hoje
        FROM onibus
        WHERE DATE(timestamp)=CURDATE()
    """)
    stats = cursor.fetchone()

    if stats["velocidade_media_hoje"] is not None:
        stats["velocidade_media_hoje"] = round(float(stats["velocidade_media_hoje"]), 2)

    cursor.execute("""
        SELECT linha, COUNT(*) AS quantidade
        FROM onibus
        WHERE DATE(timestamp)=CURDATE()
        GROUP BY linha
        ORDER BY quantidade DESC
        LIMIT 5
    """)
    linhas = cursor.fetchall()

    conn.close()

    return {
        **stats,
        "linhas_frequentes_hoje": linhas
    }

def get_last_collection_statistics():
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # 1. Descobre o timestamp da última coleta
    cursor.execute("""
        SELECT MAX(timestamp) AS ultimo_timestamp
        FROM onibus
    """)
    ultimo = cursor.fetchone()["ultimo_timestamp"]

    if not ultimo:
        conn.close()
        return {
            "total_registros": 0,
            "velocidade_media": 0,
            "veiculos_ativos": 0,
            "linhas_ativas": 0,
            "linhas_frequentes": []
        }

    # 2. Estatísticas da última coleta
    cursor.execute("""
        SELECT
            COUNT(*) AS total_registros,
            COUNT(DISTINCT linha) AS linhas_ativas,
            COUNT(DISTINCT ordem) AS veiculos_ativos,
            AVG(velocidade) AS velocidade_media
        FROM onibus
        WHERE timestamp = %s
    """, (ultimo,))

    stats = cursor.fetchone()

    # 3. Top 5 linhas da última coleta
    cursor.execute("""
        SELECT linha, COUNT(*) AS quantidade
        FROM onibus
        WHERE timestamp = %s
        GROUP BY linha
        ORDER BY quantidade DESC
        LIMIT 5
    """, (ultimo,))

    linhas = cursor.fetchall()
    conn.close()

    return {
        "total_registros": stats["total_registros"],
        "velocidade_media": round(float(stats["velocidade_media"] or 0), 2),
        "onibus_ativos": stats["veiculos_ativos"],
        "linhas_ativas": stats["linhas_ativas"],
        "linhas_frequentes": linhas
    }


def get_invalid_lines():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute('''
            SELECT linha, COUNT(*) as total
            FROM onibus
            WHERE linha IS NULL OR linha IN ('', '0', '000', '00000')
            GROUP BY linha
        ''')

        return cursor.fetchall()

    finally:
        if conn:
            conn.close()
