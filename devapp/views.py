import os
import json
import pandas as pd
import clickhouse_connect
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile


def index(request):
    return render(request, "devapp/index.html")


@csrf_exempt
def connect(request):
    if request.method == "POST":
        data = json.loads(request.body)
        try:
            client = clickhouse_connect.get_client(
                host=data["host"],
                # port=int(data["port"]),  # Optional if using default or secure
                username=data["user"],
                password=data["jwt"],
                database=data["database"],
                secure=True,
            )
            # Test connection
            client.query("SELECT 1")
            # Fetch and return list of tables
            tables = client.query("SHOW TABLES").result_rows
            return JsonResponse(
                {
                    "status": "success",
                    "tables": [t[0] for t in tables],  # Flatten tuples
                }
            )
        except Exception as e:
            print("Connection error:", e)
            return JsonResponse({"status": "error", "message": str(e)})


@csrf_exempt
def get_schema(request):
    if request.method == "POST":
        data = json.loads(request.body)
        try:
            if data["source_type"] == "clickhouse":
                client = clickhouse_connect.get_client(
                    host=data["host"],
                    port=int(data["port"]),
                    username=data["user"],
                    password=data["jwt"],
                    database=data["database"],
                    secure=True,
                )
                if data.get("table"):
                    schema = client.query(f"DESCRIBE TABLE {data['table']}").result_rows
                    return JsonResponse({"columns": [col[0] for col in schema]})
                else:
                    tables = client.query("SHOW TABLES").result_rows
                    return JsonResponse({"tables": [t[0] for t in tables]})
            else:
                df = pd.read_csv(data["filepath"], delimiter=data.get("delimiter", ","))
                return JsonResponse({"columns": df.columns.tolist()})
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)})


@csrf_exempt
def preview_data(request):
    if request.method == "POST":
        data = json.loads(request.body)
        try:
            client = clickhouse_connect.get_client(
                host=data["host"],
                port=8443,
                username=data["user"],
                password=data["jwt"],
                database=data["database"],
                secure=True,
            )
            columns = ", ".join(data["columns"])
            result = client.query(f"SELECT {columns} FROM {data['table']} LIMIT 100")
            return JsonResponse({"preview": result.result_rows})
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)})


@csrf_exempt
def ingest_data(request):
    if request.method == "POST":
        data = json.loads(request.body)
        try:
            if data["direction"] == "clickhouse_to_file":
                client = clickhouse_connect.get_client(
                    host=data["host"],
                    port=int(data["port"]),
                    username=data["user"],
                    password=data["jwt"],
                    database=data["database"],
                    secure=True,
                    compression=False,
                )

                columns = ", ".join(data["columns"])
                query = f"SELECT {columns} FROM {data['table']}"

                df = client.query_df(query)
                df.to_csv(data["output_path"], index=False)

                return JsonResponse(
                    {
                        "status": "success",
                        "records": len(df),
                        "columns": df.columns.tolist(),
                        "message": f"Your table has been downloaded to CSV with {len(df)} rows.",
                    }
                )
            else:
                df = pd.read_csv(data["filepath"], delimiter=data.get("delimiter", ","))
                df = df[data["columns"]]
                client = clickhouse_connect.get_client(
                    host=data["host"],
                    port=int(data["port"]),
                    username=data["user"],
                    password=data["jwt"],
                    database=data["database"],
                    secure=True,
                )
                client.insert_df(data["table_name"], df)
                return JsonResponse(
                    {
                        "status": "success",
                        "records": len(df),
                        "message": f"Ingested {len(df)} rows from file to ClickHouse table `{data['table_name']}`.",
                    }
                )
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)})


@csrf_exempt
def upload_file(request):
    if request.method == "POST" and request.FILES.get("file"):
        try:
            uploaded_file = request.FILES["file"]
            path = default_storage.save(
                f"uploads/{uploaded_file.name}", ContentFile(uploaded_file.read())
            )
            return JsonResponse({"status": "success", "filepath": path})
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)})
    return HttpResponseBadRequest("Invalid request")


@csrf_exempt
def preview_flatfile(request):
    if request.method == "POST":
        data = json.loads(request.body)
        try:
            df = pd.read_csv(data["filepath"], delimiter=data.get("delimiter", ","))
            return JsonResponse(
                {"preview": df[data["columns"]].head(100).values.tolist()}
            )
        except Exception as e:
            return JsonResponse({"status": "error", "message": str(e)})
