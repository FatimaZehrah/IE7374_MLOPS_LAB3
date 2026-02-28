import os
from datetime import datetime, timezone

import yaml
from google.cloud import bigquery
from google.cloud import storage

CONFIG_PATH = "config.yaml"
SCHEMA_PATH = "schemas.yaml"

bq = bigquery.Client()
gcs = storage.Client()


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def now_ts():
    return datetime.now(timezone.utc).isoformat()


def get_extension(file_name: str) -> str:
    dot = file_name.rfind(".")
    return file_name[dot:] if dot != -1 else ""


def route_table(object_name: str, config: dict):
    incoming_prefix = config["incoming_prefix"]

    if not object_name.startswith(incoming_prefix):
        return None

    relative_path = object_name[len(incoming_prefix):]
    parts = relative_path.split("/", 1)

    if len(parts) < 2:
        return None

    folder = parts[0].strip()
    return config["routing"].get(folder)


def move_blob(bucket_name: str, source_blob_name: str, destination_blob_name: str):
    bucket = gcs.bucket(bucket_name)
    source_blob = bucket.blob(source_blob_name)

    bucket.copy_blob(source_blob, bucket, destination_blob_name)
    source_blob.delete()


def ensure_table_exists(project_id: str, dataset_id: str, table_id: str, schemas: dict, config: dict):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        bq.get_table(table_ref)
        print(f"Table already exists: {table_ref}")
        return
    except Exception:
        print(f"Table does not exist yet. Creating: {table_ref}")

    schema_fields = []
    for field in schemas[table_id]["schema"]:
        schema_fields.append(
            bigquery.SchemaField(
                field["name"],
                field["type"],
                mode=field.get("mode", "NULLABLE")
            )
        )

    table = bigquery.Table(table_ref, schema=schema_fields)

    partition_field = config["tables"][table_id].get("partition_field")
    clustering_fields = config["tables"][table_id].get("clustering_fields", [])

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        )

    if clustering_fields:
        table.clustering_fields = clustering_fields

    bq.create_table(table)
    print(f"Created table: {table_ref}")


def insert_audit_row(project_id: str, dataset_id: str, row: dict):
    audit_table = f"{project_id}.{dataset_id}.load_audit"
    errors = bq.insert_rows_json(audit_table, [row])
    if errors:
        print("Audit insert errors:", errors)
    else:
        print("Audit row inserted successfully.")


def hello_gcs(request):
    config = load_yaml(CONFIG_PATH)
    schemas = load_yaml(SCHEMA_PATH)

    project_id = config["project_id"]
    dataset_id = config["dataset_id"]

    data = request.get_json(silent=True)

    if data is None:
        raw_data = request.data
        if raw_data:
            import json
            data = json.loads(raw_data)
        else:
            raise ValueError("Request body is empty or not valid JSON.")

    bucket_name = data["bucket"]
    object_name = data["name"]
    gcs_uri = f"gs://{bucket_name}/{object_name}"

    if object_name.startswith(config["archive_prefix"]) or object_name.startswith(config["rejected_prefix"]):
        print(f"Ignoring non-incoming file: {object_name}")
        return "Ignored", 200

    print(f"Triggered by file: {object_name}")

    file_extension = get_extension(object_name)
    if file_extension not in config["allowed_extensions"]:
        rejected_reason = f"Invalid file extension: {file_extension}"
        rejected_name = f"{config['rejected_prefix']}{os.path.basename(object_name)}"

        move_blob(bucket_name, object_name, rejected_name)

        insert_audit_row(project_id, dataset_id, {
            "run_ts": now_ts(),
            "file_name": object_name,
            "gcs_uri": gcs_uri,
            "target_table": None,
            "status": "REJECTED",
            "rejected_reason": rejected_reason,
            "rows_loaded": 0,
            "bq_job_id": None
        })

        print(rejected_reason)
        return "Rejected: invalid extension", 200

    table_id = route_table(object_name, config)
    if not table_id:
        rejected_reason = "File path does not match expected incoming/<folder>/<file> pattern"
        rejected_name = f"{config['rejected_prefix']}{os.path.basename(object_name)}"

        move_blob(bucket_name, object_name, rejected_name)

        insert_audit_row(project_id, dataset_id, {
            "run_ts": now_ts(),
            "file_name": object_name,
            "gcs_uri": gcs_uri,
            "target_table": None,
            "status": "REJECTED",
            "rejected_reason": rejected_reason,
            "rows_loaded": 0,
            "bq_job_id": None
        })

        print(rejected_reason)
        return "Rejected: invalid path", 200

    ensure_table_exists(project_id, dataset_id, table_id, schemas, config)

    target_table = f"{project_id}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = bq.load_table_from_uri(gcs_uri, target_table, job_config=job_config)
    load_job.result()

    destination_table = bq.get_table(target_table)

    insert_audit_row(project_id, dataset_id, {
        "run_ts": now_ts(),
        "file_name": object_name,
        "gcs_uri": gcs_uri,
        "target_table": target_table,
        "status": "SUCCESS",
        "rejected_reason": None,
        "rows_loaded": destination_table.num_rows,
        "bq_job_id": load_job.job_id
    })

    archived_name = f"{config['archive_prefix']}{os.path.basename(object_name)}"
    move_blob(bucket_name, object_name, archived_name)

    print(f"Loaded file {object_name} into {target_table} and archived it.")
    return "Success", 200
