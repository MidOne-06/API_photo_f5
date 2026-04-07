import pyodbc
import requests
import sys
import os

def main():
    # 0) Ruta al fichero con DNIs pendientes
    txt_path = 'dnis.txt'
    if not os.path.isfile(txt_path):
        print(f"❌ No encontré el fichero '{txt_path}'. Ponlo en el mismo directorio que este script.")
        sys.exit(1)

    # Leer DNIs del fichero (uno por línea)
    with open(txt_path, encoding='utf-8') as f:
        dnis = [line.strip() for line in f if line.strip()]
    if not dnis:
        print(f"❌ El fichero '{txt_path}' está vacío o no tiene DNIs válidos.")
        sys.exit(1)

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
        print("❌ Error al conectar a la base de datos:", e)
        sys.exit(1)
    cursor = conn.cursor()

    # 2) Preparar API y SQL
    base_url = 'http://161.132.51.34:1500/Nueva/api/fotodb/v1.2'
    sql_update = "UPDATE reniec_data SET imagen = ? WHERE dni = ?"

    print(f"⏳ Procesando {len(dnis)} DNIs desde '{txt_path}'…\n")

    # 3) Por cada DNI, llamar API, validar y actualizar
    for dni in dnis:
        b64 = None
        motivo = ""
        try:
            resp = requests.get(f"{base_url}/{dni}", timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            motivo = f"Error HTTP: {e}"
        else:
            # 3.1) validar coRespuesta
            if data.get('coRespuesta') != '0000':
                motivo = f"coRespuesta={data.get('coRespuesta')!r}"
            else:
                raw = data.get('image_base64')
                # 3.2) validar que sea string no vacío
                if isinstance(raw, str) and raw.strip():
                    b64 = raw.strip()
                else:
                    motivo = "image_base64 ausente o vacío"

        # 4) actualizar en la base y mostrar resultado
        cursor.execute(sql_update, b64, dni)
        if b64:
            print(f"✅ DNI {dni}: imagen guardada ({len(b64)} caracteres).")
        else:
            print(f"⚠️ DNI {dni}: queda NULL → {motivo}")

    # 5) confirmar y cerrar
    conn.commit()
    print("\n✅ Todas las actualizaciones aplicadas.")
    conn.close()

if __name__ == '__main__':
    main()

