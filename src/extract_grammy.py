import os
import pathlib
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


def extract_grammys() -> pd.DataFrame:
    # 1. Ubicar la raíz del proyecto y cargar el archivo .env
    PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
    load_dotenv(PROJECT_ROOT / ".env")

    # 2. Leer las variables de entorno (con valores de respaldo por si no existen)
    postgres_user = os.getenv("POSTGRES_USER", "postgres")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "postgres_password")
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_db = os.getenv("POSTGRES_DB", "postgres")

    # 3. Construir la URL de conexión de SQLAlchemy
    connection_string = (
        f"postgresql://{postgres_user}:{postgres_password}@"
        f"{postgres_host}:{postgres_port}/{postgres_db}"
    )
    engine = create_engine(connection_string)

    # 4. Extracción vía SQL
    query = "SELECT * FROM source_db.grammys;"
    df_grammys = pd.read_sql(query, con=engine)

    print(f"✅ Extracción exitosa. Se obtuvieron {len(df_grammys):,} filas.")

    return df_grammys


if __name__ == "__main__":
    extract_grammys()