import os
import yaml
import requests
import snowflake.connector
import shutil
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta

# --- CONFIGURATION ---
DBT_PROFILE_NAME = 'nyc_taxi_analysis'
DBT_TARGET_NAME = 'dev'
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
DOWNLOAD_DIR = "temp_data"

def get_dbt_credentials():
    """
    Récupère les identifiants depuis les variables d'environnement (GitHub Actions)
    OU depuis le fichier profiles.yml (Local).
    """
    # 1. Priorité : Variables d'environnement (GitHub Actions)
    if os.getenv('SNOWFLAKE_ACCOUNT'):
        print("🔑 Utilisation des variables d'environnement (Mode Cloud/CI)")
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN'),
        }

    # 2. Fallback : Lecture du profiles.yml (Mode Local)
    print("🏠 Recherche du profiles.yml (Mode Local)")
    home = str(Path.home())
    
    paths_to_check = [
        os.path.join(os.getcwd(), 'profiles.yml'), # Racine du projet
        os.path.join(home, '.dbt', 'profiles.yml') # Dossier utilisateur
    ]
    
    profile_path = None
    for p in paths_to_check:
        if os.path.exists(p):
            profile_path = p
            break
            
    if not profile_path:
        raise FileNotFoundError(f"❌ profiles.yml introuvable ici : {paths_to_check}")
    
    with open(profile_path, 'r') as f:
        profiles = yaml.safe_load(f)
    
    try:
        if DBT_PROFILE_NAME not in profiles:
                raise ValueError(f"Le profil '{DBT_PROFILE_NAME}' n'existe pas.")
        return profiles[DBT_PROFILE_NAME]['outputs'][DBT_TARGET_NAME]
    except KeyError as e:
        raise ValueError(f"❌ Erreur de lecture du profil : {e}")

def load_data():
    creds = get_dbt_credentials()
    
    # Calcul des mois (Jan 2024 -> Aujourd'hui)
    today = datetime.today()
    months_to_load = []
    # On commence en 2024 (Brief: 2024 + 2025 + 2026)
    current_date = datetime(2024, 1, 1) 
    
    while current_date <= today:
        months_to_load.append(current_date.strftime("%Y-%m"))
        current_date += relativedelta(months=1)

    print(f"📅 Période calculée : {months_to_load[0]} -> {months_to_load[-1]}")
    
    # Création du dossier temporaire
    if os.path.exists(DOWNLOAD_DIR):
        shutil.rmtree(DOWNLOAD_DIR) # Nettoyage préventif
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    print("🔌 Connexion à Snowflake...")
    ctx = snowflake.connector.connect(
        user=creds['user'],
        password=creds['password'],
        account=creds['account'],
        warehouse=creds['warehouse'],
        database=creds['database'],
        schema=creds['schema'],
        role=creds.get('role', 'ACCOUNTADMIN')
    )
    cs = ctx.cursor()

    try:
        for month in months_to_load:
            filename = f"yellow_tripdata_{month}.parquet"
            url = BASE_URL.format(month)
            local_path = os.path.join(DOWNLOAD_DIR, filename)
            
            print(f"⬇️  Traitement de {month}...", end=" ", flush=True)
            
            # A. Téléchargement
            try:
                response = requests.get(url, stream=True)
                if response.status_code == 200:
                    with open(local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print("✅ Téléchargé.", end=" ")
                else:
                    print(f"⚠️  Pas encore publié (Ignoré).")
                    break # On arrête si on atteint le futur
            except Exception as e:
                print(f"❌ Erreur réseau : {e}")
                continue

            # B. Upload vers Snowflake (Internal Stage)
            try:
                print("⬆️ Upload...", end=" ")
                safe_path = local_path.replace('\\', '/')
                put_query = f"PUT file://{os.path.abspath(safe_path)} @RAW.MY_INTERNAL_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                cs.execute(put_query)

                # C. Copy into Table
                print("💾 SQL...", end=" ")
                copy_query = f"""
                COPY INTO RAW.yellow_taxi_trips
                FROM @RAW.MY_INTERNAL_STAGE/{filename}
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = 'CONTINUE'
                """
                cs.execute(copy_query)
                print("🆗 Terminé !")
                
                # Nettoyage fichier unitaire
                os.remove(local_path)
                
            except Exception as e:
                print(f"\n❌ Erreur Snowflake sur {month} : {e}")

    finally:
        cs.close()
        ctx.close()
        if os.path.exists(DOWNLOAD_DIR):
            shutil.rmtree(DOWNLOAD_DIR)
        print("\n👋 Pipeline terminé.")

if __name__ == "__main__":
    load_data()
