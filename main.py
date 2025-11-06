from google.cloud import bigquery, storage
from datetime import datetime, time
import pandas as pd
import tempfile
import os
import re
import hashlib
import base64

# -------------------------------------------------------------------------------------------------------------------
# Parámetros globales
PROJECT_ID = "integra-bbdd-adduntia" #Completar: ID del proyecto.
LOCATION = "us"                      #Completar: Zona de ubicación del proyecto.
BUCKET_NAME = "rls-ddm-bucket"       #Completar: Nombre del bucket de Cloud Storage.
SHEET_PATH = "masking_policies.csv"  #Completar: Nombre del archivo CSV.
BQ_DATASET = "Data_Adduntia_Sheets"  #Completar: Dataset en donde se alocara la tabla de auditoria.
BQ_TABLE = "masking_reglas"          #Completar: Nombre de la tabla que contendra las reglas de Masking en Bigquery.
BQ_AUDIT_TABLE = "masking_auditoria" #Completar: Nombre de la tabla de auditoría en BigQuery.

# -------------------------------------------------------------------------------------------------------------------
# Funciones auxiliares
def sanitize_identifier(s: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_]', '_', s)

def verificar_y_convertir():
    """Descarga archivo del bucket y asegura que sea CSV"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(SOURCE_FILE)

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, os.path.basename(SOURCE_FILE))
        blob.download_to_filename(local_path)
        ext = os.path.splitext(local_path)[-1].lower()

        if ext not in [".csv", ".xlsx", ".xls"]:
            raise ValueError(f"Formato no soportado: {ext}")

        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(local_path)
            csv_path = local_path.replace(ext, ".csv")
            df.to_csv(csv_path, index=False)
            new_blob_name = SOURCE_FILE.replace(ext, ".csv")
            bucket.blob(new_blob_name).upload_from_filename(csv_path)
            return new_blob_name

        return SOURCE_FILE

# -------------------------------------------------------------------------------------------------------------------
# Creacion/actualizacion de la tabla de reglas RLS en BigQuery       
def cargar_a_bigquery():
    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = client.dataset(DATASET)
    table_ref = dataset_ref.table(TABLA)

    # Definicion del esquema del sheet con las reglas RLS
    schema_fields = [
        bigquery.SchemaField("Proyecto", "STRING"),
        bigquery.SchemaField("Dataset", "STRING"),
        bigquery.SchemaField("Tabla", "STRING"),
        bigquery.SchemaField("Usuario", "STRING"),
        bigquery.SchemaField("Dimension_1", "STRING"),
        bigquery.SchemaField("Condicion_1", "STRING"),
        bigquery.SchemaField("Dimension_2", "STRING"),
        bigquery.SchemaField("Condicion_2", "STRING"),
    ]

    # Creacion de la tabla si no existe
    try:
        client.get_table(table_ref)
        print(f"Tabla {TABLA} ya existe.")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema_fields)
        client.create_table(table)
        print(f"Tabla {TABLA} creada.")

    # Configuración del job de carga
    job_config = bigquery.LoadJobConfig(
        schema=schema_fields,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition="WRITE_TRUNCATE",
    )

    uri = f"gs://{BUCKET}/{SOURCE_FILE}"

    print(f"Cargando datos desde {uri} a {DATASET}.{TABLA} ...")
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  
    print(f"Datos cargados correctamente en {DATASET}.{TABLA}")

# -------------------------------------------------------------------------------------------------------------------
# Eliminacion de las RLS pre existentes   
def eliminar_rls_existentes():
    client = bigquery.Client()
    df = client.query(f"SELECT * FROM `{TABLE_REGLAS}`").to_dataframe()

    for _, row in df.iterrows():
        proyecto, dataset, tabla = row["Proyecto"], row["Dataset"], row["Tabla"]
        if not (proyecto and dataset and tabla):
            continue

        sql_list = f"""
            SELECT policy_name
            FROM `{proyecto}.{dataset}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
            WHERE table_name = '{tabla}';
        """
        try:
            policies_df = client.query(sql_list).to_dataframe()
            for policy_name in policies_df.get("policy_name", []):
                drop_sql = f"DROP ROW ACCESS POLICY `{policy_name}` ON `{proyecto}.{dataset}.{tabla}`;"
                client.query(drop_sql).result()
                print(f"Eliminada política {policy_name} en {tabla}")
        except Exception as e:
            print(f"Error eliminando políticas en {tabla}: {e}")

# -------------------------------------------------------------------------------------------------------------------
# Generacion dinamica y aplicacion de las RLS. Creacion/actualizacion de la tabla de auditoria   
def generar_y_aplicar_rls():
    client = bigquery.Client()
    df = client.query(f"SELECT * FROM `{TABLE_REGLAS}`").to_dataframe()

    auditoria = []
    batch_id = 1
    try:
        result = client.query(f"SELECT COALESCE(MAX(batch_id), 0) AS max_batch FROM `{TABLE_AUDITORIA}`").to_dataframe()
        batch_id = int(result["max_batch"].iloc[0]) + 1
    except Exception:
        pass

    grouped = df.groupby(["Proyecto", "Dataset", "Tabla", "Usuario"], dropna=False)

    for (proyecto, dataset, tabla, usuario), group in grouped:
        if not all([proyecto, dataset, tabla, usuario]):
            continue
        if not usuario.startswith("user:"):
            usuario = f"user:{usuario}"

        try:
            client.get_table(f"{proyecto}.{dataset}.{tabla}")
        except Exception:
            continue

        condiciones_list = []
        for _, row in group.iterrows():
            condiciones = []
            for col in df.columns:
                if col.lower().startswith("dimension_"):
                    idx = col.split("_")[-1]
                    dim = row[col]
                    cond = row.get(f"Condicion_{idx}")
                    if pd.notna(dim) and pd.notna(cond):
                        condiciones.append(f"{dim} {cond}")
            if condiciones:
                condiciones_list.append("(" + " AND ".join(condiciones) + ")")

        if not condiciones_list:
            continue

        filtro = " OR ".join(condiciones_list)
        policy_name = sanitize_identifier(f"rls_{proyecto}_{dataset}_{tabla}_{usuario}")
        sql = f"""
        CREATE OR REPLACE ROW ACCESS POLICY {policy_name}
        ON `{proyecto}.{dataset}.{tabla}`
        GRANT TO ('{usuario}')
        FILTER USING ({filtro});
        """
        client.query(sql).result()

        cadena = f"{proyecto}|{dataset}|{usuario}|{filtro}"
        hash_id = hashlib.sha256(cadena.encode()).hexdigest()
        auditoria.append({
            "proyecto": proyecto,
            "dataset": dataset,
            "tabla": tabla,
            "usuario": usuario,
            "policy_name": policy_name,
            "condiciones": filtro,
            "fecha": datetime.utcnow(),
            "batch_id": batch_id,
            "hash_id": hash_id,
            "estado": "VIGENTE"
        })

    if auditoria:
       df_auditoria = pd.DataFrame(auditoria)
       df_auditoria["estado"] = "VIGENTE"  

       job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
       client.load_table_from_dataframe(df_auditoria, TABLE_AUDITORIA, job_config=job_config).result()
       print(f"Se registraron {len(auditoria)} políticas en auditoría (batch_id={batch_id}).")

# -------------------------------------------------------------------------------------------------------------------
# Actualizacion del estado de las politicas en la tabla de auditoria        
def actualizacion_registros_auditoria():        
    try:
        client = bigquery.Client()
        update_sql = f"""
        UPDATE `{TABLE_AUDITORIA}`
        SET estado = 'DEPRECADO'
        WHERE batch_id < (SELECT MAX(batch_id) FROM `{TABLE_AUDITORIA}`)
        """
        job = client.query(update_sql)
        job.result()
      
        print(f"UPDATE para marcar deprecados ejecutado correctamente")
    except Exception as e:
        print(f"Error ejecutando UPDATE para marcar DEPRECADO: {e}")

# -------------------------------------------------------------------------------------------------------------------
# Cloud Function principal
def run_rls_process(request):
    try:
        verificar_y_convertir()
        cargar_a_bigquery()
        eliminar_rls_existentes()
        generar_y_aplicar_rls()
        actualizacion_registros_auditoria()
        print("Proceso completado exitosamente.")
        return ("Proceso RLS ejecutado correctamente", 200)
    except Exception as e:
        print(f"Error durante la ejecución: {e}")
        return (f"Error: {e}", 500)
