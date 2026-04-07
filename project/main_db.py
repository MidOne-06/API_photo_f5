import csv
import base64
import binascii
import pyodbc

def main():
    # Parámetros de conexión
    conn_str = (
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=192.168.10.105,1433;'
        'DATABASE=data_exe1;'
        'UID=sa;'
        'PWD=Perulinux,.12345;'
        'Encrypt=no;'
        'TrustServerCertificate=yes;'
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Abrimos el CSV (llámalo "resultados.cvs" si ese es el nombre)
    with open('resultados.csv', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dni = row.get('dni', '').strip()
            imagen_b64 = row.get('imagen', '').strip()

            if not dni:
                # Sin DNI no actualizamos
                continue

            if imagen_b64:
                try:
                    # Validamos que sea base64 correcto
                    base64.b64decode(imagen_b64, validate=True)
                    # Si no lanza excepción, actualizamos con la cadena base64
                    cursor.execute(
                        "UPDATE reniec_data SET imagen = ? WHERE dni = ?",
                        imagen_b64, dni
                    )
                except (binascii.Error, ValueError):
                    # No es base64 válido → NULL
                    cursor.execute(
                        "UPDATE reniec_data SET imagen = NULL WHERE dni = ?",
                        dni
                    )
            else:
                # Campo vacío → NULL
                cursor.execute(
                    "UPDATE reniec_data SET imagen = NULL WHERE dni = ?",
                    dni
                )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == '__main__':
    main()
