import os
import sys
import requests
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool 
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from functools import wraps
import logging

# Logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Carrega .env
load_dotenv() 

app = Flask(__name__)

# --- Configuração ---
DATABASE_URL = os.getenv("DATABASE_URL")
AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL")

if not DATABASE_URL or not AUTH_SERVICE_URL:
    log.critical("Erro: DATABASE_URL e AUTH_SERVICE_URL devem ser definidos.")
    sys.exit(1)

# --- Pool de Conexão ---
try:
    pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5)
    log.info("Pool de conexões com o PostgreSQL inicializado.")
except Exception as e:
    log.critical(f"Erro fatal ao conectar ao PostgreSQL: {e}")
    sys.exit(1)

# --- Middleware de Autenticação ---
def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            return jsonify({"error": "Authorization header obrigatório"}), 401
        
        try:
            response = requests.get(
                f"{AUTH_SERVICE_URL}/validate",
                headers={"Authorization": auth_header},
                timeout=3
            )

            if response.status_code != 200:
                return jsonify({"error": "Chave de API inválida"}), 401

        except requests.exceptions.Timeout:
            return jsonify({"error": "Serviço de autenticação indisponível (timeout)"}), 504  
        except requests.exceptions.RequestException:
            return jsonify({"error": "Serviço de autenticação indisponível"}), 503  

        return f(*args, **kwargs)
    return decorated

# --- Endpoints ---

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

@app.route('/flags', methods=['POST'])
@require_auth
def create_flag():
    data = request.get_json()
    if not data or 'name' not in data:
        return jsonify({"error": "'name' é obrigatório"}), 400
    
    name = data['name']
    description = data.get('description', '')
    is_enabled = data.get('is_enabled', False)
    
    conn = None
    cur = None
    try:
        conn = pool.getconn()
        cur = conn.cursor(row_factory=dict_row)

        cur.execute(
            "INSERT INTO flags (name, description, is_enabled, created_at, updated_at) "
            "VALUES (%s, %s, %s, NOW(), NOW()) RETURNING *",
            (name, description, is_enabled)
        )

        new_flag = cur.fetchone()
        conn.commit()
        return jsonify(new_flag), 201

    except psycopg.errors.UniqueViolation:
        conn.rollback()
        return jsonify({"error": f"Flag '{name}' já existe"}), 409

    except Exception as e:
        if conn:
            conn.rollback()
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

    finally:
        if cur: cur.close()
        if conn: pool.putconn(conn)

@app.route('/flags', methods=['GET'])
@require_auth
def get_flags():
    try:
        conn = pool.getconn()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute("SELECT * FROM flags ORDER BY name")
        flags = cur.fetchall()
        return jsonify(flags)

    except Exception as e:
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

    finally:
        if cur: cur.close()
        if conn: pool.putconn(conn)

@app.route('/flags/<string:name>', methods=['GET'])
@require_auth
def get_flag(name):
    try:
        conn = pool.getconn()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute("SELECT * FROM flags WHERE name = %s", (name,))
        flag = cur.fetchone()
        if not flag:
            return jsonify({"error": "Flag não encontrada"}), 404
        return jsonify(flag)

    except Exception as e:
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

    finally:
        if cur: cur.close()
        if conn: pool.putconn(conn)

@app.route('/flags/<string:name>', methods=['PUT'])
@require_auth
def update_flag(name):
    data = request.get_json()
    if not data:
        return jsonify({"error": "Corpo da requisição obrigatório"}), 400

    fields = []
    values = []
    
    if 'description' in data:
        fields.append("description = %s")
        values.append(data['description'])
    if 'is_enabled' in data:
        fields.append("is_enabled = %s")
        values.append(data['is_enabled'])
    
    if not fields:
        return jsonify({"error": "Pelo menos um campo ('description', 'is_enabled') é obrigatório"}), 400
    
    values.append(name)
    query = f"UPDATE flags SET {', '.join(fields)}, updated_at = NOW() WHERE name = %s RETURNING *"
    
    try:
        conn = pool.getconn()
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(query, tuple(values))

        if cur.rowcount == 0:
            return jsonify({"error": "Flag não encontrada"}), 404

        updated_flag = cur.fetchone()
        conn.commit()
        return jsonify(updated_flag)

    except Exception as e:
        conn.rollback()
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

    finally:
        if cur: cur.close()
        if conn: pool.putconn(conn)

@app.route('/flags/<string:name>', methods=['DELETE'])
@require_auth
def delete_flag(name):
    try:
        conn = pool.getconn()
        cur = conn.cursor()
        cur.execute("DELETE FROM flags WHERE name = %s", (name,))

        if cur.rowcount == 0:
            return jsonify({"error": "Flag não encontrada"}), 404

        conn.commit()
        return "", 204

    except Exception as e:
        conn.rollback()
        return jsonify({"error": "Erro interno do servidor", "details": str(e)}), 500

    finally:
        if cur: cur.close()
        if conn: pool.putconn(conn)

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8002))
    app.run(host='0.0.0.0', port=port, debug=False)
