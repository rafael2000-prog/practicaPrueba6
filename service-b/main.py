from fastapi import FastAPI
import psycopg2
import time
import os
import requests

app = FastAPI()

DB_HOST = os.getenv("DB_HOST", "db-b")
DB_NAME = os.getenv("POSTGRES_DB", "serviceb")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
SERVICE_NEXT = os.getenv("SERVICE_NEXT_URL")

def get_connection():
    for i in range(10):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=5432
            )
        except psycopg2.OperationalError:
            print("⏳ Esperando a la base de datos...")
            time.sleep(2)
    raise Exception("❌ No se pudo conectar a la base de datos")

@app.post("/process")
def process(data: dict):
    value = data.get("value", 0) + 1

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO values_table(value) VALUES (%s)", (value,))
    conn.commit()
    cur.close()
    conn.close()

    if SERVICE_NEXT:
        try:
            resp = requests.post(SERVICE_NEXT, json={"value": value}, timeout=5)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return {"service": "B", "value": value, "error_downstream": str(e)}

    return {"service": "B", "value": value}
