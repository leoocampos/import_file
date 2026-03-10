import os
import io
import pandas as pd
import logging
from flask import Flask, jsonify
from google.cloud import bigquery
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth import default

# =====================
# CONFIGURAÇÕES
# =====================
# ID da pasta onde o relatório será salvo
FOLDER_DESTINO_ID = "1qaT6UKBdjH2V-iCF1D65mehG8C7Kdldj"
DATASET_ID = "GOLD"
TABLE_ID = "vendas"

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# =====================
# CLIENTES GCP
# =====================
def get_clients():
    # Escopos necessários para ler o BQ e escrever no Drive
    SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/cloud-platform"]
    
    creds, project = default(scopes=SCOPES)
    drive_service = build("drive", "v3", credentials=creds)
    bq_client = bigquery.Client(credentials=creds, project=project)
    
    return drive_service, bq_client

# =====================
# LÓGICA DE EXPORTAÇÃO
# =====================
def exportar_gold_para_drive():
    try:
        drive_service, bq_client = get_clients()
        
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
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Vendas Gold')
        
        output.seek(0) # Volta para o início do arquivo em memória

        # 3️⃣ Salvar no Google Drive
        file_metadata = {
            'name': file_name,
            'parents': [FOLDER_DESTINO_ID]
        }
        media = MediaIoBaseUpload(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

        logging.info(f"Fazendo upload do arquivo {file_name} para o Drive...")
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        return {
            "status": "success",
            "message": f"Arquivo {file_name} gerado e salvo no Drive.",
            "rows_exported": len(df)
        }, 200

    except Exception as e:
        logging.error(f"Erro no processo de exportação: {e}")
        return {
            "status": "error",
            "message": "Falha ao exportar dados da GOLD para o Drive.",
            "details": str(e)
        }, 500

# =====================
# ENDPOINT CLOUD RUN
# =====================
@app.post("/import_file")
def import_file():
    resultado, status_code = exportar_gold_para_drive()
    return jsonify(resultado), status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)