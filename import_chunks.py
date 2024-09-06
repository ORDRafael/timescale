import os
import psycopg2
import subprocess

# Configuración de la conexión a PostgreSQL
DB_NAME = "test"
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_PASSWORD = "12345"

# Directorio donde están los archivos CSV comprimidos
OUTPUT_DIR = "/usr/proyectos"

# Función para importar un archivo CSV a una tabla
def import_csv_to_table(file_path, table_name):
    try:
        # Conectar a la base de datos PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # Comando COPY para importar datos
        copy_sql = f"COPY {table_name} FROM STDIN WITH CSV HEADER"

        # Abrir el archivo CSV
        with open(file_path, 'r') as f:
            cur.copy_expert(copy_sql, f)

        conn.commit()
        print(f"Archivo {file_path} importado con éxito a la tabla {table_name}.")

        # Comprimir el archivo CSV después de la importación exitosa
        compress_file(file_path)

    except Exception as e:
        print(f"Error al importar {file_path}: {e}")
    finally:
        cur.close()
        conn.close()

# Función para descomprimir archivos .gz
def decompress_files(output_dir):
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".csv.gz"):
                file_path = os.path.join(root, file)
                print(f"Descomprimiendo {file_path}...")
                try:
                    subprocess.run(["gunzip", "-f", file_path], check=True)
                except subprocess.CalledProcessError as e:
                    print(f"Error al descomprimir {file_path}: {e}")

# Función para comprimir archivos CSV
def compress_file(file_path):
    try:
        print(f"Comprimiendo {file_path}...")
        subprocess.run(["pigz", "-f", file_path], check=True)
        print(f"Archivo {file_path} comprimido con éxito.")
    except subprocess.CalledProcessError as e:
        print(f"Error al comprimir {file_path}: {e}")

# Descomprimir archivos CSV comprimidos
decompress_files(OUTPUT_DIR)

# Iterar sobre cada archivo CSV en el directorio descomprimido
for root, dirs, files in os.walk(OUTPUT_DIR):
    for file in files:
        if file.endswith(".csv"):
            file_path = os.path.join(root, file)

            # Determinar el nombre de la tabla a partir del nombre del archivo
            table_name = os.path.basename(root)  # Usar el nombre del directorio como nombre de tabla
            import_csv_to_table(file_path, table_name)
