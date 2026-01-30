from database import get_engine
from sqlalchemy import text

def verify_data():
    engine = get_engine()
    
    with engine.connect() as conn:
        # Count clients
        result = conn.execute(text("SELECT COUNT(*) FROM clients"))
        client_count = result.fetchone()[0]
        
        # Count tarjetas
        result = conn.execute(text("SELECT COUNT(*) FROM tarjetas"))
        tarjeta_count = result.fetchone()[0]
        
        # Show sample data
        print("=" * 60)
        print("VERIFICACION DE DATOS EN POSTGRESQL")
        print("=" * 60)
        print(f"\nTotal de clientes: {client_count}")
        print(f"Total de tarjetas: {tarjeta_count}")
        
        print("\n--- Muestra de Clientes ---")
        result = conn.execute(text("SELECT cod_cliente, nombre, apellido1, dni, correo FROM clients LIMIT 5"))
        for row in result:
            print(f"  {row[0]} | {row[1]} {row[2]} | {row[3]} | {row[4]}")
        
        print("\n--- Muestra de Tarjetas ---")
        result = conn.execute(text("SELECT cod_cliente, numero_tarjeta, fecha_exp FROM tarjetas LIMIT 5"))
        for row in result:
            print(f"  Cliente: {row[0]} | Tarjeta: {row[1]} | Expira: {row[2]}")
        
        print("\n" + "=" * 60)
        print("[OK] Verificacion completada exitosamente!")
        print("=" * 60)

if __name__ == "__main__":
    verify_data()
