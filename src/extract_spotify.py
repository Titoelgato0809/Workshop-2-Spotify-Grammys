from pathlib import Path
import pandas as pd

# ============================================================
# CONFIGURACIÓN DE RUTAS
# ============================================================
# Se asume la estructura: Proyecto/data/spotify_dataset.csv
RUTA_RAW = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "spotify_dataset.csv"
)


# ============================================================
# EXTRACCIÓN
# ============================================================
def extraer_datos_spotify(ruta: Path) -> pd.DataFrame:
    """Extrae los datos del dataset de Spotify desde un archivo CSV."""

    if not ruta.exists():
        raise FileNotFoundError(f"No se encontró el archivo fuente en: {ruta}")

    print("Iniciando extracción del Dataset 1 (Spotify)...")
    print(f"Archivo de origen: {ruta}")

    # Carga limpia del CSV usando Pandas
    df = pd.read_csv(ruta)

    print("\nExtracción completada con éxito.")
    print(f"Filas extraídas: {df.shape[0]:,}")
    print(f"Columnas extraídas: {df.shape[1]}")

    return df


# ============================================================
# EJECUCIÓN LOCAL DE PRUEBA
# ============================================================
if __name__ == "__main__":

    df_spotify = extraer_datos_spotify(RUTA_RAW)

    print("\nVista previa de las primeras 5 filas:")
    print(df_spotify.head())