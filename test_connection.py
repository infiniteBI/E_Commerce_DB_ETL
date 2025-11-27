from dotenv import load_dotenv
import os
import psycopg2

# Load .env variables
from pathlib import Path

env_path = Path(__file__).parent / 'rdsAuthenticator.env'
load_dotenv(env_path)
load_dotenv('rdsAuthenticator.env')

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT"))
    )

# Test connection
if __name__ == "__main__":
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT current_date;")
        print("Connected! Today:", cursor.fetchone()[0])
        cursor.close()
        conn.close()
    except Exception as e:
        print("Database connection failed:", e)
