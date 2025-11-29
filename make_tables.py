import os
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

def main():
    load_dotenv()

    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")

    # Path to the SQL file same directory as this script
    sql_path = "table_def.sql"

    with open(sql_path, "r") as f:
        sql_commands = f.read()

    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )

    cur = conn.cursor()
    cur.execute(sql_commands)
    conn.commit()

    cur.close()
    conn.close()

    print("Tables created successfully.")

if __name__ == "__main__":
    main()
