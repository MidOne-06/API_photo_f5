# leer_db.py

from cache import list_cache
import json

def main():
    registros = list_cache()
    if not registros:
        print("No hay entradas en la caché.")
        return

    for entrada in registros:
        print(f"RUC:       {entrada['ruc']}")
        print(f"Timestamp: {entrada['timestamp']}")
        print("Data:")
        print(json.dumps(entrada['data'], ensure_ascii=False, indent=2))
        print("-" * 60)

if __name__ == "__main__":
    main()
