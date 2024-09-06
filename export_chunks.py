import os
import psycopg2
from psycopg2 import sql
import subprocess  

# Configuración de la conexión a postgres
DB_NAME = "zabbix"
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_PASSWORD = "12345"

TABLE_NAMES = ["history", "history_uint", "history_bin", "history_str",
               "history_log", "history_text", "auditlog", "trends_uint", "trends"]

OUTPUT_DIR = "/usr/proyectos"
EXPORT_LOG_FILE = "exported_chunks.log"

# Opcion para habiltiar o deshabilitar la limpieza del archivo log y la cantidad de chunks que va a procesar para que sea por bloque
CLEAR_LOG = False
PAGE_SIZE = 1000

# Opcion para habilitar o deshabilitar el backup del resto de las tablas con pg_dump
ENABLE_PG_DUMP = False  
PG_DUMP_FILE = "tablas_zabbix.sql"  # Archivo de salida para pg_dump

# Funcion para limpiar el archivo de log
def clear_export_log(log_file):
    open(log_file, 'w').close()
    print(f"Archivo de log {log_file} limpiado.")

    # Función para borrar los archivos de salida
def clear_output_files(output_dir):
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith(".csv") or file.endswith(".csv.gz"):
                file_path = os.path.join(root, file)
                try:
                    os.remove(file_path)
                    print(f"Archivo eliminado: {file_path}")
                except Exception as e:
                    print(f"Error al eliminar el archivo {file_path}: {e}")

# Funcion para cargar los chunks exportados previamente
def load_exported_chunks(log_file):
    try:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                return set(line.strip() for line in f if line.strip())
        else:
            return set()
    except Exception as e:
        print(f"Error al cargar el archivo de registro: {e}")
        return set()

# Funcion para guardar los chunks exportados
def save_exported_chunk(log_file, chunk_name):
    with open(log_file, 'a') as f:
        f.write(chunk_name + "\n")

# Funcion para ejecutar pg_dump
def execute_pg_dump():
    try:
        # realiza el backup excluyendo las tablas que contienen los chunks
        command = [
            "pg_dump",
            "-U", DB_USER,
            "-h", DB_HOST,
            "-p", DB_PORT,
            "-f", PG_DUMP_FILE,
            "-T", "history_uint",
            "-T", "history",
            "-T", "history_bin",
            "-T", "history_str",
            "-T", "history_log",
            "-T", "history_text",
            "-T", "trends_uint",
            "-T", "trends",
            "-T", "_timescaledb_internal.*",  
            
            DB_NAME
        ]

        # Ejecutar pg_dump
        result = subprocess.run(command, check=True, text=True)
        print(f"Backup realizado con éxito y guardado en {PG_DUMP_FILE}")

    except subprocess.CalledProcessError as e:
        print(f"Error al ejecutar pg_dump: {e}")

# Conectar a la base de datos poatgres
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    print("Conexión a la base de datos exitosa.")
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")
    exit(1)

# funcion para limpiar el archivo de log si está habilitado
if CLEAR_LOG:
    clear_export_log(EXPORT_LOG_FILE)
    clear_output_files(OUTPUT_DIR)


cur = conn.cursor()

# Cargar chunks exportados previamente
exported_chunks = load_exported_chunks(EXPORT_LOG_FILE)

# Iterar sobre cada tabla en la lista
for table_name in TABLE_NAMES:
    print(f"Procesando tabla: {table_name}")

    # Crear un subdirectorio para cada tabla
    table_output_dir = os.path.join(OUTPUT_DIR, table_name)
    os.makedirs(table_output_dir, exist_ok=True)

    # Configurar paginación
    offset = 0
    while True:

        # Obtener un bloque de chunks para la tabla actual
        cur.execute(
            sql.SQL("""
                SELECT chunk_schema, chunk_name
                FROM timescaledb_information.chunks 
                WHERE hypertable_name = %s
                LIMIT %s OFFSET %s;
            """),
            [table_name, PAGE_SIZE, offset]
        )
        chunks = cur.fetchall()

        # Si no hay mas chunks, salir del bucle
        if not chunks:
            break

        # Exportar cada chunk a CSV si no ha sido exportado antes
        for chunk in chunks:
            chunk_schema, chunk_name = chunk
            chunk_full_name = f"{chunk_schema}.{chunk_name}"

            if chunk_full_name in exported_chunks:
                print(f"El chunk {chunk_full_name} ya ha sido exportado")
                continue

            output_file = os.path.join(table_output_dir, chunk_name.replace('.', '_') + ".csv")

            try:
                with open(output_file, 'w') as f:
                    copy_sql = sql.SQL("COPY (SELECT * FROM {} ) TO STDOUT WITH CSV HEADER").format(sql.Identifier(chunk_schema, chunk_name))
                    cur.copy_expert(copy_sql, f)
                print(f"Exportado: {chunk_full_name} a {output_file}")

                # Comprimir el archivo exportado
                subprocess.run(["pigz", output_file], check=True)
                print(f"Archivo comprimido: {output_file}.gz")

                # Registrar el chunk como exportado
                save_exported_chunk(EXPORT_LOG_FILE, chunk_full_name)

            except Exception as e:
                print(f"Error al exportar {chunk_full_name}: {e}")
                conn.rollback()

        # Incrementar offset para la siguiente página de chunks
        offset += PAGE_SIZE

cur.close()
conn.close()
print("Exportación completada.")

# Ejecutar pg_dump si está habilitado
if ENABLE_PG_DUMP:
    execute_pg_dump()
