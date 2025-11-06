from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime
import uuid
from datetime import datetime
import pandas as pd
import re
import os
import tempfile
import hashlib

# -------------------------------------------------------------------------------------------------------------------
# Sección declarativa

PROJECT_ID = "integra-bbdd-adduntia"                      #Completar: ID del proyecto en donde se aloja el Dataset con la tabla de reglas de RLS.
DATASET = "Data_Adduntia_Sheets"                          #Completar: Dataset en donde se alojara la tabla con las reglas de RLS.
TABLA = "reglas_rls_test"                                 #Completar: Tabla que contendra las reglas de RLS.
TABLE_REGLAS = f"{PROJECT_ID}.{DATASET}.{TABLA}"               
TABLE_AUDITORIA = f"{PROJECT_ID}.{DATASET}.auditoria_rls" #Completar: Nombre de la tabla para auditoría de cambios en RLS.
BUCKET = "us-central1-test-96d39955-bucket"               #Completar: Nombre del bucket de Google Cloud Storage.
SOURCE_FILE = "data/rls_policies.csv"                     #Completar: Carpeta y nombre del archivo en Google Cloud Storage.

# -------------------------------------------------------------------------------------------------------------------
# Funciones auxiliares

def sanitize_identifier(s: str) -> str:
    """Convierte un string en un identificador válido de BigQuery."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', s)

def verificar_y_convertir(bucket, source_object, **kwargs):
    """
    Verifica si el archivo es CSV, si no lo convierte a CSV y lo sube a GCS.
    Guarda en XCom el nombre final del CSV.
    """
    gcs_hook = GCSHook()

    with tempfile.TemporaryDirectory() as tmp_dir:
        local_file = os.path.join(tmp_dir, os.path.basename(source_object))
        gcs_hook.download(bucket_name=bucket, object_name=source_object, filename=local_file)

        ext = os.path.splitext(local_file)[-1].lower()
        if ext == ".csv":
            kwargs["ti"].xcom_push(key="csv_file", value=source_object)
            return

        if ext in [".xlsx", ".xls"]:
            df = pd.read_excel(local_file)
            csv_file = local_file.replace(ext, ".csv")
            df.to_csv(csv_file, index=False)

            csv_object = source_object.replace(ext, ".csv")
            gcs_hook.upload(bucket_name=bucket, object_name=csv_object, filename=csv_file)

            kwargs["ti"].xcom_push(key="csv_file", value=csv_object)
        else:
            raise ValueError(f"Formato no soportado: {ext}")

def eliminar_rls_existentes(**kwargs):
    """
    Elimina todas las políticas RLS existentes de las tablas listadas en TABLE_REGLAS.
    """
    client = bigquery.Client()
    df = client.query(f"SELECT * FROM `{TABLE_REGLAS}`").to_dataframe()

    for _, row in df.iterrows():
        proyecto = row.get("Proyecto")   
        dataset = row.get("Dataset")
        tabla = row.get("Tabla")

        if not proyecto or not dataset or not tabla:
            continue

        sql_list_policies = f"""
            SELECT policy_name
            FROM `{proyecto}.{dataset}.INFORMATION_SCHEMA.ROW_ACCESS_POLICIES`
            WHERE table_name = '{tabla}';
        """
        try:
            policies_df = client.query(sql_list_policies).to_dataframe()
        except Exception as e:
            print(f"No se pudieron listar políticas en {tabla}: {e}")
            continue

        for policy_name in policies_df.get("policy_name", []):
            sql_drop = f"DROP ROW ACCESS POLICY `{policy_name}` ON `{proyecto}.{dataset}.{tabla}`;"
            try:
                client.query(sql_drop).result()
                print(f"Eliminada política {policy_name} en {tabla}")
            except Exception as e:
                print(f"No se pudo eliminar {policy_name} en {tabla}: {e}")

def generar_rls_dinamico(**kwargs):
    client = bigquery.Client()
    df = client.query(f"SELECT * FROM `{TABLE_REGLAS}`").to_dataframe()

    queries = []
    auditoria = []
    
    # Generacion de los campos batch_id y hash_id para la tabla de auditoria
    # 1. Buscar el último batch_id en la tabla de auditoría
    try:
        query = f"SELECT COALESCE(MAX(batch_id), 0) AS max_batch FROM `{TABLE_AUDITORIA}`"
        result = client.query(query).to_dataframe()
        last_batch = int(result["max_batch"].iloc[0])
    except Exception:
        last_batch = 0

    batch_id = last_batch + 1
   
    grouped = df.groupby(["Proyecto", "Dataset", "Tabla", "Usuario"], dropna=False)

    for (proyecto, dataset, tabla, usuario), group in grouped:
        if not proyecto or not dataset or not tabla or not usuario:
            continue

        if not usuario.startswith("user:"):
            usuario = f"user:{usuario}"

        # Validar que la tabla exista en BQ
        try:
            client.get_table(f"{proyecto}.{dataset}.{tabla}")
        except Exception:
            continue

        condiciones_list = []
        for _, row in group.iterrows():
            condiciones = []

            # Recorrer todas las columnas Dimension_X y Condicion_X
            for col in df.columns:
                if col.lower().startswith("dimension_"):
                    idx = col.split("_")[-1]  # ej: "1", "2", ...
                    dim = row[col]
                    cond = row.get(f"Condicion_{idx}")
                    if pd.notna(dim) and pd.notna(cond):
                        condiciones.append(f"{dim} {cond}")

            if condiciones:
                condiciones_list.append("(" + " AND ".join(condiciones) + ")")

        if not condiciones_list:
            continue

        filter_sql = " OR ".join(condiciones_list)
        policy_name = sanitize_identifier(f"rls_{proyecto}_{dataset}_{tabla}_{usuario}")

        sql = f"""
        CREATE OR REPLACE ROW ACCESS POLICY {policy_name}
        ON `{proyecto}.{dataset}.{tabla}`
        GRANT TO ('{usuario}')
        FILTER USING ({filter_sql});
        """
        queries.append(sql.strip())

        cadena = f"{proyecto}|{dataset}|{usuario}|{condiciones}"
        hash_id = hashlib.sha256(cadena.encode('utf-8')).hexdigest()
               
        auditoria.append({
            "proyecto": proyecto,
            "dataset": dataset,
            "tabla": tabla,
            "usuario": usuario,
            "policy_name": policy_name,
            "condiciones": filter_sql,
            "fecha": datetime.utcnow(),
            "batch_id": batch_id,
            "hash_id": hash_id
        })

    kwargs["ti"].xcom_push(key="queries", value=queries)
    kwargs["ti"].xcom_push(key="auditoria", value=auditoria)


def aplicar_rls_dinamico(**context):
    """
    Aplica las políticas RLS generadas por generar_rls_dinamico y registra auditoría.
    """
    queries = context["ti"].xcom_pull(task_ids="generar_rls_dinamico", key="queries") or []
    auditoria = context["ti"].xcom_pull(task_ids="generar_rls_dinamico", key="auditoria") or []

    if not queries:
        print("No hay queries para aplicar.")
        return

    client = bigquery.Client(project=PROJECT_ID)

    # Ejecutar todas las queries directamente
    for stmt in queries:
        client.query(stmt).result()

    if auditoria:
        df_auditoria = pd.DataFrame(auditoria)
        client_audit = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        client_audit.load_table_from_dataframe(df_auditoria, TABLE_AUDITORIA, job_config=job_config).result()

def detectar_cambios_rls(**kwargs):
    client = bigquery.Client(project=PROJECT_ID)

    TABLE_CAMBIOS = f"{PROJECT_ID}.{DATASET}.auditoria_rls_cambios"

    # 1. Verificar si la tabla existe (si no, crearla)
    try:
        client.get_table(TABLE_CAMBIOS)
        print(f"Tabla {TABLE_CAMBIOS} ya existe ✅")
    except Exception:
        print(f"Tabla {TABLE_CAMBIOS} no existe, creando...")
        schema = [
            bigquery.SchemaField("batch_id", "INT64"),
            bigquery.SchemaField("hash_id", "STRING"),
            bigquery.SchemaField("estado", "STRING"),
            bigquery.SchemaField("fecha", "TIMESTAMP"),
            bigquery.SchemaField("proyecto", "STRING"),
            bigquery.SchemaField("dataset", "STRING"),
            bigquery.SchemaField("tabla", "STRING"),
            bigquery.SchemaField("usuario", "STRING"),
            bigquery.SchemaField("policy_name", "STRING"),
            bigquery.SchemaField("condiciones", "STRING"),
        ]
        table = bigquery.Table(TABLE_CAMBIOS, schema=schema)
        client.create_table(table)
        print(f"Tabla {TABLE_CAMBIOS} creada ✅")

    # 2. Obtener batch actual y anterior
    query_batches = f"""
        SELECT 
          MAX(batch_id) AS batch_actual,
          MAX(batch_id) - 1 AS batch_anterior
        FROM `{TABLE_AUDITORIA}`
    """
    result = client.query(query_batches).to_dataframe()
    batch_actual = int(result["batch_actual"].iloc[0])
    batch_anterior = int(result["batch_anterior"].iloc[0])

    # 3. Comparar y detectar cambios incluyendo metadata
    query_cambios = f"""
    WITH anterior AS (
      SELECT 
        hash_id, proyecto, dataset, tabla, usuario, policy_name, condiciones
      FROM `{TABLE_AUDITORIA}`
      WHERE batch_id = {batch_anterior}
    ),
    actual AS (
      SELECT 
        hash_id, proyecto, dataset, tabla, usuario, policy_name, condiciones
      FROM `{TABLE_AUDITORIA}`
      WHERE batch_id = {batch_actual}
    )
    SELECT 
      {batch_actual} AS batch_id,
      COALESCE(a.hash_id, b.hash_id) AS hash_id,
      CASE 
        WHEN a.hash_id IS NULL THEN "ELIMINADA"
        WHEN b.hash_id IS NULL THEN "NUEVA"
        ELSE "EXISTENTE"
      END AS estado,
      CURRENT_TIMESTAMP() AS fecha,
      COALESCE(a.proyecto, b.proyecto) AS proyecto,
      COALESCE(a.dataset, b.dataset) AS dataset,
      COALESCE(a.tabla, b.tabla) AS tabla,
      COALESCE(a.usuario, b.usuario) AS usuario,
      COALESCE(a.policy_name, b.policy_name) AS policy_name,
      COALESCE(a.condiciones, b.condiciones) AS condiciones
    FROM actual a
    FULL OUTER JOIN anterior b
    ON a.hash_id = b.hash_id
    """
    cambios_df = client.query(query_cambios).to_dataframe()

    if not cambios_df.empty:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        client.load_table_from_dataframe(cambios_df, TABLE_CAMBIOS, job_config=job_config).result()
        print(f"Cambios detectados y tabla {TABLE_CAMBIOS} reemplazada")
    else:
        print("No se detectaron cambios entre lotes.")



# -------------------------------------------------------------------------------------------------------------------
# DAG

with DAG(
    dag_id="gcs_sheet_to_bq_rls_dinamico",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0},
    tags=["gcs", "bigquery", "rls"],
) as dag:

    schema_fields = [
        {"name": "Proyecto", "type": "STRING", "mode": "NULLABLE"},    # A completar: Nombre del campo del sheet en el que se detallan los proyectos sobre los que se aplicara el RLS. 
        {"name": "Dataset", "type": "STRING", "mode": "NULLABLE"},     # A completar: Nombre del campo del sheet en el que se detallan los datasets sobre los que se aplicara el RLS.
        {"name": "Tabla", "type": "STRING", "mode": "NULLABLE"},       # A completar: Nombre del campo del sheet en el que se detallan las tablas sobre las que se aplicara el RLS.
        {"name": "Usuario", "type": "STRING", "mode": "NULLABLE"},     # A completar: Nombre del campo del sheet en el que se detallan los usuarios sobre los que se aplicara el RLS.
        {"name": "Dimension_1", "type": "STRING", "mode": "NULLABLE"}, # A completar: Nombre del campo del sheet en el que se detallan los usuarios sobre los que se aplicara el RLS.
        {"name": "Condicion_1", "type": "STRING", "mode": "NULLABLE"}, # A completar: Nombre del campo del sheet en el que se detallan los usuarios sobre los que se aplicara el RLS.
        {"name": "Dimension_2", "type": "STRING", "mode": "NULLABLE"}, # A completar: Nombre del campo del sheet en el que se detallan los usuarios sobre los que se aplicara el RLS.
        {"name": "Condicion_2", "type": "STRING", "mode": "NULLABLE"}, # A completar: Nombre del campo del sheet en el que se detallan los usuarios sobre los que se aplicara el RLS.
    
    ]

    verificar_archivo = PythonOperator(
        task_id="verificar_archivo",
        python_callable=verificar_y_convertir,
        provide_context=True,
        op_kwargs={"bucket": BUCKET, "source_object": SOURCE_FILE},
    )

    gcs_to_bq = GCSToBigQueryOperator(
       task_id="gcs_to_bq",
       bucket=BUCKET,
       source_objects=["{{ ti.xcom_pull(task_ids='verificar_archivo', key='csv_file') }}"],
       destination_project_dataset_table=TABLE_REGLAS,
       source_format="csv",
       skip_leading_rows=1,
       write_disposition="WRITE_TRUNCATE",
       create_disposition="CREATE_IF_NEEDED",
       schema_fields=schema_fields,
       location="US",
    )


    eliminar_rls_task = PythonOperator(
        task_id="eliminar_rls_existentes",
        python_callable=eliminar_rls_existentes,
    )

    generar_rls_dinamico_task = PythonOperator(
        task_id="generar_rls_dinamico",
        python_callable=generar_rls_dinamico,
    )

    aplicar_rls_dinamico_task = PythonOperator(
        task_id="aplicar_rls_dinamico",
        python_callable=aplicar_rls_dinamico,
    )

    detectar_cambios_rls_task = PythonOperator(
    task_id="detectar_cambios_rls",
    python_callable=detectar_cambios_rls,
    )

    # Secuencia de tareas
    verificar_archivo >> gcs_to_bq >> eliminar_rls_task >> generar_rls_dinamico_task >> aplicar_rls_dinamico_task >> detectar_cambios_rls_task
