import os
import psycopg2
from dotenv import load_dotenv

# Load database details from .env file
load_dotenv()

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

try:
    connection = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()

    print("Connection successful!")
    print("PostgreSQL version:")
    print(db_version[0])

    cursor.close()
    connection.close()

except Exception as error:
    print("Connection failed!")
    print(error)