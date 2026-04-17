import os

import json

import datetime

from google.cloud import firestore

from google.cloud import bigquery

from google.oauth2 import service_account



# 1. CONFIGURATION CROSS-PROJECT

SOURCE_PROJECT_ID = "dataflashcards"       # Où est ton Firestore

DEST_PROJECT_ID = "my-project-florent-bq"  # Où est ton BigQuery

DATASET_ID = "raw_data_dataflashcards"

TABLE_ID = "analytics_promo_raw"

COLLECTION_NAME = "analytics_promo"



def run_ingestion():

    # 2. AUTHENTIFICATION

    # On récupère la clé JSON depuis les secrets GitHub

    info = json.loads(os.environ.get("GCP_SA_KEY"))

    creds = service_account.Credentials.from_service_account_info(info)



    # Initialisation des clients

    db = firestore.Client(credentials=creds, project=SOURCE_PROJECT_ID)

    bq_client = bigquery.Client(credentials=creds, project=DEST_PROJECT_ID)



    print(f"--- Extraction de Firestore ({SOURCE_PROJECT_ID}) ---")

    

    docs = db.collection(COLLECTION_NAME).stream()

    rows_to_insert = []



    # On récupère l'heure actuelle une seule fois pour tout le lot

    now_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()



    for doc in docs:

        data = doc.to_dict()

        data["document_id"] = doc.id

        

        # AJOUT DE LA COLONNE TECHNIQUE

        data["ingested_at"] = now_timestamp

        

        # Nettoyage des formats de date pour BigQuery

        for key, value in data.items():

            if hasattr(value, 'isoformat'):

                data[key] = value.isoformat()

        

        rows_to_insert.append(data)



    print(f"Extraction réussie : {len(rows_to_insert)} documents.")



    # 3. CHARGEMENT VERS BIGQUERY

    if rows_to_insert:

        table_ref = f"{DEST_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

        

        # WRITE_TRUNCATE : vide la table et remplace par les nouvelles données

        job_config = bigquery.LoadJobConfig(

            write_disposition="WRITE_TRUNCATE",

            autodetect=True,

        )



        print(f"Envoi des données vers {table_ref}...")

        job = bq_client.load_table_from_json(

            rows_to_insert, table_ref, job_config=job_config

        )

        job.result() # Attend la fin du job



        print(f"--- Succès ! Données disponibles dans {DEST_PROJECT_ID} ---")

    else:

        print("Aucune donnée trouvée.")



if __name__ == "__main__":

    run_ingestion()""
