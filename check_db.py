from database import get_engine
from sqlalchemy import text

engine = get_engine()

with engine.connect() as conn:
    result = conn.execute(text("SELECT cod_cliente, dni, nombre FROM clients LIMIT 10"))
    rows = result.fetchall()
    print("Clientes en la base de datos:")
    for row in rows:
        print(f"  cod_cliente: {row[0]}, dni: {row[1]}, nombre: {row[2]}")
