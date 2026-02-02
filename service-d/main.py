from fastapi import FastAPI
import psycopg2
import time
import os
import requests

app = FastAPI()

DB_HOST = os.getenv("DB_HOST", "db-d")
DB_NAME = os.getenv("POSTGRES_DB", "serviced")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

SERVICE_A_URL = os.getenv("SERVICE_A_URL", "http://service-a:8000/process")


def get_connection():
    for _ in range(10):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=5432
            )
        except psycopg2.OperationalError:
            print("⏳ service-d esperando DB...")
            time.sleep(2)
    raise Exception("❌ service-d sin DB")


@app.post("/process")
def process(data: dict):
    value = data.get("value", 0) + 1

    # Guardar en DB-D
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO values_table(value) VALUES (%s)", (value,))
    conn.commit()
    cur.close()
    conn.close()

    # Llamar a A para continuar la cadena
    response = requests.post(
        SERVICE_A_URL,
        json={"value": value},
        timeout=5
    )
    response.raise_for_status()
    downstream = response.json()

    # El valor final es el que devuelve la cadena (por ejemplo, servicio C)
    final_value = downstream.get("value", value)

    return {
        "service": "D",
        "final_value": final_value,
        "status": "Proceso completado",
        "returned_to": "service-a",
        "service_a_response": downstream
    }
