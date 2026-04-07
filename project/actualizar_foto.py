import pyodbc
import requests
import sys

def main():
    # 1) Conexión ODBC a SQL Server
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.10.105,1433;'
        'DATABASE=data_exe1;'
        'UID=sa;'
        'PWD=Perulinux,.12345;'
        'Encrypt=no;'
        'TrustServerCertificate=yes;'
    )
    try:
        conn = pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print("Error al conectar a la base de datos:", e)
        sys.exit(1)
    cursor = conn.cursor()

    # 2) Leer todos los DNIs
    cursor.execute("SELECT dni FROM reniec_data")
    dnis = [row[0] for row in cursor.fetchall()]
    if not dnis:
        print("No se encontraron DNIs en reniec_data.")
        conn.close()
        return

    # 3) Preparar UPDATE
    sql_update = "UPDATE reniec_data SET imagen = ? WHERE dni = ?"
    base_url = 'http://161.132.51.34:1500/Nueva/api/fotodb/v1.2'

    # 4) Procesar cada DNI
    for dni in dnis:
        b64 = None
        try:
            url = f"{base_url}/{dni}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get('coRespuesta') == '0000':
                b64 = data.get('image_base64')
        except Exception as e:
            print(f"[Error API DNI={dni}]: {e}")

        # 5) Ejecutar UPDATE y mostrar en consola
        cursor.execute(sql_update, b64, dni)
        if b64:
            print(f"DNI {dni}: imagen actualizada ({len(b64)} caracteres)")
        else:
            print(f"DNI {dni}: sin foto, campo imagen = NULL")

    # 6) Commit y cerrar
    conn.commit()
    print("\n✅ Todas las actualizaciones han sido aplicadas.")
    conn.close()

if __name__ == '__main__':
    main()
