import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "2020")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    database = os.getenv("DB_NAME", "Clientes")

    connection_string = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)