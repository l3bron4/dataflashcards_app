import os
import json
from google.cloud import firestore
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. CONFIGURATION DES VARIABLES
# Modifie ces noms selon tes besoins
PROJECT_ID = "my-project-florent-bq"
DATASET_ID = "raw_data_dataflashcards"
TABLE_ID = "analytics_promo_raw"
COLLECTION_NAME = "analytics_promo"

def run_ingestion():
    # 2. AUTHENTIFICATION
    # On récupère la clé JSON depuis la variable d'environnement (configurée dans GitHub Actions)
    info = json.loads(os.environ.get("GCP_SA_KEY"))
    creds = service_account.Credentials.from_service_account_info(info)

    # Initialisation des clients
    db = firestore.Client(credentials=creds, project=PROJECT_ID)
    bq_client = bigquery.Client(credentials=creds, project=PROJECT_ID)

    print(f"--- Début de l'extraction de Firestore ({COLLECTION_NAME}) ---")
    
    # 3. EXTRACTION DE FIRESTORE
    docs = db.collection(COLLECTION_NAME).stream()
    rows_to_insert = []

    for doc in docs:
        data = doc.to_dict()
        # On ajoute l'ID du document pour le suivi
        data["document_id"] = doc.id
        
        # Formatage des dates pour BigQuery (Firebase renvoie des objets datetime)
        for key, value in data.items():
            if hasattr(value, 'isoformat'):
                data[key] = value.isoformat()
        
        rows_to_insert.append(data)

    print(f"Extraction réussie : {len(rows_to_insert)} documents trouvés.")

    # 4. CHARGEMENT VERS BIGQUERY
    if rows_to_insert:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        # Configuration de l'ingestion : on remplace la table à chaque fois (Write Truncate)
        # Idéal pour la pratique d'ingestion quotidienne
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True, # BigQuery va créer le schéma tout seul
        )

        job = bq_client.load_table_from_json(
            rows_to_insert, table_ref, job_config=job_config
        )
        job.result()  # Attend la fin de l'insertion

        print(f"--- Succès ! Données chargées dans {table_ref} ---")
    else:
        print("Aucune donnée à importer.")

if __name__ == "__main__":
    run_ingestion()
