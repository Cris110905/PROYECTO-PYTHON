from database import get_engine
from sqlalchemy import text

def create_tables():
    engine = get_engine()
    
    with engine.connect() as conn:
        # Create clients table if not exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS clients (
                nombre VARCHAR(100),
                apellido1 VARCHAR(100),
                apellido2 VARCHAR(100),
                dni VARCHAR(20) PRIMARY KEY,
                correo VARCHAR(150),
                telefono VARCHAR(20)
            )
        """))
        
        # Create tarjetas table if not exists
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tarjetas (
                cod_cliente VARCHAR(20),
                numero_tarjeta VARCHAR(20) PRIMARY KEY,
                fecha_exp VARCHAR(10),
                cvv VARCHAR(5),
                FOREIGN KEY (cod_cliente) REFERENCES clients(dni)
            )
        """))
        
        conn.commit()
        print("[OK] Tablas creadas correctamente")

if __name__ == "__main__":
    create_tables()
