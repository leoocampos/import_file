import os
import io
import pandas as pd
import logging
from flask import Flask, jsonify
from google.cloud import bigquery, storage
from google.auth import default

# =====================
# CONFIGURAÇÕES
# =====================
BUCKET_NAME = "sample-track-files"
PASTA_DESTINO = "import/" # Pasta dentro do bucket
DATASET_ID = "SILVER"
TABLE_ID = "vendas_diarias"

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# =====================
# CLIENTES GCP
# =====================
def get_clients():
    creds, project = default()
    bq_client = bigquery.Client(credentials=creds, project=project)
    storage_client = storage.Client(credentials=creds, project=project)
    return bq_client, storage_client

# =====================
# LÓGICA DE EXPORTAÇÃO
# =====================
def exportar_silver_para_bucket():
    try:
        bq_client, storage_client = get_clients()
        
        # 1️⃣ Query no BigQuery (Captura a última carga)
        query = f"""
            SELECT * FROM `{bq_client.project}.{DATASET_ID}.{TABLE_ID}`
            WHERE dat_ref_carga = (SELECT MAX(dat_ref_carga) FROM `{bq_client.project}.{DATASET_ID}.{TABLE_ID}`)
        """
        logging.info("Executando query no BigQuery...")
        df = bq_client.query(query).to_dataframe()

        if df.empty:
            logging.warning("Nenhum dado encontrado para a última data de carga.")
            return {"status": "ok", "message": "Tabela vazia, nada a exportar."}, 200

        # 2️⃣ Montar arquivo Excel em memória
        output = io.BytesIO()
        file_name = f"relatorio_vendas_ultima_carga.xlsx"
        # O caminho final dentro do bucket inclui a "pasta"
        caminho_final_blob = f"{PASTA_DESTINO}{file_name}"
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Vendas silver')
        
        output.seek(0)

        # 3️⃣ Salvar no Cloud Storage (Bucket)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(caminho_final_blob)

        logging.info(f"Fazendo upload para gs://{BUCKET_NAME}/{caminho_final_blob}...")
        blob.upload_from_file(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        return {
            "status": "success",
            "message": f"Arquivo salvo no bucket em: {caminho_final_blob}",
            "rows_exported": len(df)
        }, 200

    except Exception as e:
        logging.error(f"Erro no processo de exportação para bucket: {e}")
        return {
            "status": "error",
            "message": "Falha ao exportar dados da silver para o Storage.",
            "details": str(e)
        }, 500

# =====================
# ENDPOINT
# =====================
@app.post("/import_file")
def import_file():
    resultado, status_code = exportar_silver_para_bucket()
    return jsonify(resultado), status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)