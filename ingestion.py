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
    # Vérification de la présence de la clé dans les variables d'environnement
    info_json = os.environ.get("GCP_SA_KEY")
    if not info_json:
        print("Erreur : La variable GCP_SA_KEY est manquante.")
        return

    info = json.loads(info_json)
    creds = service_account.Credentials.from_service_account_info(info)

    # Initialisation des clients
    db = firestore.Client(credentials=creds, project=SOURCE_PROJECT_ID)
    bq_client = bigquery.Client(credentials=creds, project=DEST_PROJECT_ID)

    print(f"--- Extraction de Firestore ({SOURCE_PROJECT_ID}) ---")
    
    docs = db.collection(COLLECTION_NAME).stream()
    rows_to_insert = []

    # Heure actuelle pour le traçage
    now_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for doc in docs:
        data = doc.to_dict()
        data["document_id"] = doc.id
        data["ingested_at"] = now_timestamp
        
        # Nettoyage des formats pour BigQuery (Timestamps Firestore -> ISO Strings)
        for key, value in data.items():
            if hasattr(value, 'isoformat'):
                data[key] = value.isoformat()
        
        rows_to_insert.append(data)

    print(f"Extraction réussie : {len(rows_to_insert)} documents.")

    # 3. CHARGEMENT VERS BIGQUERY
    if rows_to_insert:
        table_ref = f"{DEST_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Plus de contraintes Sandbox ici, on utilise la config standard
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        print(f"Envoi des données vers {table_ref}...")
        job = bq_client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )
        job.result()  # Attend la fin du job

        print(f"--- Succès ! Données injectées dans {DEST_PROJECT_ID} ---")
    else:
        print("Aucune donnée trouvée dans Firestore.")

if __name__ == "__main__":
    run_ingestion()
