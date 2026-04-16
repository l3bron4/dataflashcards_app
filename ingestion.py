import os
import json
import datetime
from google.cloud import firestore
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. CONFIGURATION CROSS-PROJECT
SOURCE_PROJECT_ID = "dataflashcards"
DEST_PROJECT_ID = "my-project-florent-bq"
DATASET_ID = "raw_data_dataflashcards"
TABLE_ID = "analytics_promo_raw"
COLLECTION_NAME = "analytics_promo"

def run_ingestion():
    # 2. AUTHENTIFICATION
    info = json.loads(os.environ.get("GCP_SA_KEY"))
    creds = service_account.Credentials.from_service_account_info(info)

    db = firestore.Client(credentials=creds, project=SOURCE_PROJECT_ID)
    bq_client = bigquery.Client(credentials=creds, project=DEST_PROJECT_ID)

    print(f"--- Extraction de Firestore ({SOURCE_PROJECT_ID}) ---")
    
    docs = db.collection(COLLECTION_NAME).stream()
    rows_to_insert = []

    # On utilise datetime.datetime pour le formatage
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    now_timestamp = now_dt.isoformat()

    for doc in docs:
        data = doc.to_dict()
        data["document_id"] = doc.id
        data["ingested_at"] = now_timestamp
        
        for key, value in data.items():
            if hasattr(value, 'isoformat'):
                data[key] = value.isoformat()
        
        rows_to_insert.append(data)

    print(f"Extraction réussie : {len(rows_to_insert)} documents.")

    # 3. CHARGEMENT VERS BIGQUERY (CORRIGÉ POUR LE SANDBOX)
    if rows_to_insert:
        table_ref = f"{DEST_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            # AJOUT INDISPENSABLE POUR LE MODE GRATUIT :
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingested_at",  # Utilise ta colonne de timestamp
                expiration_ms=30 * 24 * 60 * 60 * 1000  # Expire après 30 jours (< 60j)
            ),
        )

        print(f"Envoi des données vers {table_ref}...")
        job = bq_client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )
        job.result() 

        print(f"--- Succès ! Données disponibles dans {DEST_PROJECT_ID} ---")
    else:
        print("Aucune donnée trouvée.")

if __name__ == "__main__":
    run_ingestion()
