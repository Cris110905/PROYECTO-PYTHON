from database import get_engine
from sqlalchemy import text

def drop_and_recreate_tables():
    engine = get_engine()
    
    with engine.begin() as conn:
        # Drop tables if they exist (in correct order due to foreign key)
        conn.execute(text("DROP TABLE IF EXISTS tarjetas CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS clients CASCADE"))
        
        # Create clients table
        conn.execute(text("""
            CREATE TABLE clients (
                cod_cliente VARCHAR(20) UNIQUE,
                nombre VARCHAR(100),
                apellido1 VARCHAR(100),
                apellido2 VARCHAR(100),
                dni VARCHAR(20) PRIMARY KEY,
                correo VARCHAR(150),
                telefono VARCHAR(20)
            )
        """))
        
        # Create tarjetas table with correct column names
        conn.execute(text("""
            CREATE TABLE tarjetas (
                cod_cliente VARCHAR(20),
                numero_tarjeta VARCHAR(20) PRIMARY KEY,
                fecha_exp VARCHAR(10),
                cvv VARCHAR(5),
                FOREIGN KEY (cod_cliente) REFERENCES clients(cod_cliente)
            )
        """))
        
        print("[OK] Tablas eliminadas y recreadas correctamente")

if __name__ == "__main__":
    drop_and_recreate_tables()
