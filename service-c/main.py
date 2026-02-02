from fastapi import FastAPI
import psycopg2
import time
import os

app = FastAPI()

DB_HOST = os.getenv("DB_HOST", "db-c")
DB_NAME = os.getenv("POSTGRES_DB", "servicec")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

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

    return {
        "service": "C",
        "value": value
    }
