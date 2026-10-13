import pendulum
import pandas as pd
from io import StringIO
from airflow.sdk import dag, task
from pathlib import Path

# Importación de los módulos reales de tu proyecto
from src.extract_grammy import extract_grammys
from src.extract_spotify import extraer_datos_spotify
from src.transform import transform
from gx.validacion_transform import validations_prepared
from gx.validation_extract import validations_spotify, grammy_validations
from src.load import load_dw  # Tu función de carga a PostgreSQL

STAGING_DIR = Path("/opt/airflow/data/staging")
STAGING_DIR.mkdir(parents=True, exist_ok=True)

@dag(
    dag_id="reliable_music_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "spotify", "grammy", "reliable"],
)
def reliable_music_pipeline():

    @task
    def start():
        print("Iniciando Pipeline ETL Spotify-Grammys.")

    # -------------------------------------------------------------------------
    # RAMA SPOTIFY
    # -------------------------------------------------------------------------
    @task
    def extract_data_spotify():
        print("Extrayendo datos de Spotify...")
        ruta_spotify = Path("/opt/airflow/data/spotify_dataset.csv")
        df_spotify = extraer_datos_spotify(ruta_spotify)
        return df_spotify.to_json(orient="records")

    @task
    def validate_data_extract_spotify(spotify_json):
        print("Validando dataset de Spotify...")
        validations_spotify()  # Se llama sin argumentos según la firma de gx
        print("¡Validación de extracción de Spotify exitosa!")
        return spotify_json

    # -------------------------------------------------------------------------
    # RAMA GRAMMYS
    # -------------------------------------------------------------------------
    @task
    def extract_data_grammys():
        print("Extrayendo datos de Grammys...")
        df_grammys = extract_grammys()
        return df_grammys.to_json(orient="records")  # Corregido: to_json

    @task 
    def validate_data_extract_grammys(grammys_json):
        print("Validando dataset de Grammys...")
        grammy_validations()  # Se llama sin argumentos
        print("¡Validación de extracción de Grammys exitosa!")
        return grammys_json

    # -------------------------------------------------------------------------
    # TRANSFORMACIÓN Y CARGA
    # -------------------------------------------------------------------------
    @task
    def transform_data(spotify_json, grammys_json):
        print("Iniciando transformación y cruce de datos...")
        df_spotify = pd.read_json(StringIO(spotify_json))
        df_grammys = pd.read_json(StringIO(grammys_json))
        
        # 1. Ejecutar transformación
        df_transformed = transform(df_spotify, df_grammys)
        
        # 2. Guardar en disco en la carpeta staging
        output_path = STAGING_DIR / "transformed_dataset.csv"
        df_transformed.to_csv(output_path, index=False)
        
        print(f"Transformación guardada exitosamente en {output_path}")
        # Retornamos únicamente la ruta en texto (XCom ligero)
        return str(output_path)

    @task
    def validate_data_transform(file_path: str):
        print("Iniciando validación de datos transformados...")
        # Leemos el archivo guardado en la tarea anterior
        df_transformed = pd.read_csv(file_path)
        
        # Validación con Great Expectations
        validations_prepared(df_transformed)
        
        print("¡Validación de transformación exitosa!")
        return file_path

    @task
    def load_data(file_path: str):
        print("Iniciando carga hacia el Data Warehouse...")
        df_transformed = pd.read_csv(file_path)
        
        # Carga a PostgreSQL
        load_dw(df_transformed)
        
        print("¡Carga a Data Warehouse completada exitosamente!")

    # -------------------------------------------------------------------------
    # Definición del flujo y dependencias paralelas
    # -------------------------------------------------------------------------
    start_node = start()

    # Ejecución paralela de las ramas de Spotify y Grammys
    spotify_json = extract_data_spotify()
    spotify_validated = validate_data_extract_spotify(spotify_json)

    grammys_json = extract_data_grammys()
    grammys_validated = validate_data_extract_grammys(grammys_json)

    # Cruce y etapas finales
    transformed_file_path = transform_data(spotify_validated, grammys_validated)
    validated_file_path = validate_data_transform(transformed_file_path)
    load_node = load_data(validated_file_path)

    # Conexión inicial hacia ambas ramas
    start_node >> [spotify_json, grammys_json]

# Instanciación del DAG
reliable_music_pipeline()