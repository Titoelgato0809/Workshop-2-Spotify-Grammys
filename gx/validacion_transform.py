import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as gx


# ---------------------------------------------------------
# Umbrales (decisiones de ingeniería: justificar cada uno en la tabla de reglas)
# ---------------------------------------------------------

MIN_YEAR = 1958
MAX_YEAR = 2019
MIN_ARTIST_KNOWN = 0.60       # el perfilamiento muestra ~69% de artistas conocidos
MIN_NOT_OTHER_GROUP = 0.85    # el perfilamiento muestra ~93% de categorías agrupadas
MIN_SONG_MATCH = 0.05         # AJUSTAR con tu corrida real (match de canciones)
MIN_ALBUM_MATCH = 0.05        # AJUSTAR con tu corrida real (match de álbumes)

# El dataset integrado mezcla filas de Spotify y de Grammy:
# cada regla se evalúa solo sobre las filas que le corresponden.
SPOTIFY_ROWS = 'match_status != "grammy_only"'
GRAMMY_ROWS = 'match_status != "spotify_only"'

EXPECTED_COLUMNS = [
    "track_id", "track_name", "explicit", "artists", "album_name", "main_artists",
    "track_genre", "duration_category", "energy", "danceability", "popularity",
    "duration_ms", "genre_weight", "year", "decade", "category", "category_group",
    "nominee", "nominee_type", "grammy_artists", "win_count",
    "winner_album_name", "winner_album_artists", "match_status",
]

CATEGORY_GROUPS = [
    "General Field", "R&B", "Rap", "Country", "Jazz", "Latin", "Classical", "Gospel",
    "Reggae", "Dance/Electronic", "Folk/Roots", "Blues", "Alternative", "Rock", "Pop",
    "Production/Technical", "Children/Comedy/Theater", "World/New Age",
    "Instrumental/Composition", "Music Video", "Other",
]


def validations_prepared(df_integrated, output_dir="docs/evidence"):

    print("Dataset shape:", df_integrated.shape)

    # ---------------------------------------------------------
    # Create the Great Expectations Data Context
    # ---------------------------------------------------------

    context = gx.get_context(
        mode="ephemeral"
    )

    print("GX Data Context created successfully.")
    print("Context type:", type(context).__name__)

    # ---------------------------------------------------------
    # Create the Pandas Data Source
    # ---------------------------------------------------------

    data_source = context.data_sources.add_pandas(
        name="Prepared_pandas_source"
    )

    print("Pandas Data Source created successfully.")
    print("Data Source name:", data_source.name)

    # ---------------------------------------------------------
    # Create the Data Asset
    # ---------------------------------------------------------

    data_asset = data_source.add_dataframe_asset(
        name="Prepared_integrated"
    )

    print("Data Asset created successfully.")
    print("Data Asset name:", data_asset.name)

    # ---------------------------------------------------------
    # Create the Batch Definition
    # ---------------------------------------------------------

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="whole_prepared_dataframe"
    )

    print("Batch Definition created successfully.")
    print("Batch Definition name:", batch_definition.name)

    # ---------------------------------------------------------
    # Create the Batch
    # ---------------------------------------------------------

    batch_parameters = {
        "dataframe": df_integrated
    }

    batch = batch_definition.get_batch(
        batch_parameters=batch_parameters
    )

    print("Batch created successfully.")
    print("Batch type:", type(batch).__name__)

    # ---------------------------------------------------------
    # Create the Expectation Suite
    # ---------------------------------------------------------

    prepared_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="prepared_suite")
    )

    # ---------------------------------------------------------
    # P01 - Estructura: columnas esperadas del modelo dimensional
    # ---------------------------------------------------------

    columns_expectation = gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=EXPECTED_COLUMNS,
        exact_match=False,
        severity="critical",
        meta={"rule_id": "P01"},
    )
    prepared_suite.add_expectation(columns_expectation)

    # ---------------------------------------------------------
    # P02 - match_status: solo los 3 estados de la unión
    # ---------------------------------------------------------

    match_status_expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="match_status",
        value_set=["both", "spotify_only", "grammy_only"],
        severity="critical",
        meta={"rule_id": "P02"},
    )
    prepared_suite.add_expectation(match_status_expectation)

    # ---------------------------------------------------------
    # P03 - nominee_type: valores permitidos (filas Grammy)
    # ---------------------------------------------------------

    nominee_type_expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="nominee_type",
        value_set=["song", "album", "artist", "unknown"],
        row_condition=GRAMMY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P03"},
    )
    prepared_suite.add_expectation(nominee_type_expectation)

    # ---------------------------------------------------------
    # P04 - Unicidad: la unión no multiplicó filas
    # ---------------------------------------------------------

    unique_expectation = gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=["track_id", "track_genre", "year", "category", "nominee"],
        severity="critical",
        meta={"rule_id": "P04"},
    )
    prepared_suite.add_expectation(unique_expectation)

    # ---------------------------------------------------------
    # P05 - win_count: cada ganador cuenta exactamente 1 (filas Grammy)
    # ---------------------------------------------------------

    win_count_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="win_count",
        min_value=1,
        max_value=1,
        row_condition=GRAMMY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P05"},
    )
    prepared_suite.add_expectation(win_count_expectation)

    # ---------------------------------------------------------
    # P06 - Integración: ganadores tipo canción con match en Spotify
    # ---------------------------------------------------------

    song_match_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="track_id",
        mostly=MIN_SONG_MATCH,
        row_condition='nominee_type == "song" and match_status != "spotify_only"',
        condition_parser="pandas",
        severity="warning",
        meta={"rule_id": "P06"},
    )
    prepared_suite.add_expectation(song_match_expectation)

    # ---------------------------------------------------------
    # P07 - Integración: ganadores tipo álbum con match en Spotify
    # ---------------------------------------------------------

    album_match_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="winner_album_name",
        mostly=MIN_ALBUM_MATCH,
        row_condition='nominee_type == "album"',
        condition_parser="pandas",
        severity="warning",
        meta={"rule_id": "P07"},
    )
    prepared_suite.add_expectation(album_match_expectation)

    # ---------------------------------------------------------
    # P08 - track_id: no nulo (filas Spotify)
    # ---------------------------------------------------------

    track_id_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="track_id",
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P08"},
    )
    prepared_suite.add_expectation(track_id_expectation)

    # ---------------------------------------------------------
    # P09 - track_genre: no nulo (filas Spotify)
    # ---------------------------------------------------------

    track_genre_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="track_genre",
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P09"},
    )
    prepared_suite.add_expectation(track_genre_expectation)

    # ---------------------------------------------------------
    # P10 - energy y danceability: entre 0 y 1 (filas Spotify)
    # ---------------------------------------------------------

    energy_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="energy",
        min_value=0.0,
        max_value=1.0,
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P10"},
    )
    prepared_suite.add_expectation(energy_expectation)

    danceability_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="danceability",
        min_value=0.0,
        max_value=1.0,
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P10"},
    )
    prepared_suite.add_expectation(danceability_expectation)

    # ---------------------------------------------------------
    # P11 - popularity: entre 0 y 100 (filas Spotify)
    # ---------------------------------------------------------

    popularity_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="popularity",
        min_value=0,
        max_value=100,
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P11"},
    )
    prepared_suite.add_expectation(popularity_expectation)

    # ---------------------------------------------------------
    # P12 - genre_weight: mayor que 0 y hasta 1 (filas Spotify)
    # ---------------------------------------------------------

    genre_weight_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="genre_weight",
        min_value=0.0,
        max_value=1.0,
        strict_min=True,
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P12"},
    )
    prepared_suite.add_expectation(genre_weight_expectation)

    # ---------------------------------------------------------
    # P13 - duration_ms: al menos 30 segundos (filas Spotify)
    # ---------------------------------------------------------

    duration_ms_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="duration_ms",
        min_value=30000,
        mostly=0.99,
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="warning",
        meta={"rule_id": "P13"},
    )
    prepared_suite.add_expectation(duration_ms_expectation)

    # ---------------------------------------------------------
    # P14 - duration_category: Corta, Media o Larga (filas Spotify)
    # ---------------------------------------------------------

    duration_category_expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="duration_category",
        value_set=["Corta", "Media", "Larga"],
        row_condition=SPOTIFY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P14"},
    )
    prepared_suite.add_expectation(duration_category_expectation)

    # ---------------------------------------------------------
    # P15 - year: dentro del rango de los Grammy (filas Grammy)
    # ---------------------------------------------------------

    year_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="year",
        min_value=MIN_YEAR,
        max_value=MAX_YEAR,
        row_condition=GRAMMY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P15"},
    )
    prepared_suite.add_expectation(year_expectation)

    # ---------------------------------------------------------
    # P16 - category_group: pertenece al catálogo de grupos (filas Grammy)
    # ---------------------------------------------------------

    category_group_expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="category_group",
        value_set=CATEGORY_GROUPS,
        row_condition=GRAMMY_ROWS,
        condition_parser="pandas",
        severity="critical",
        meta={"rule_id": "P16"},
    )
    prepared_suite.add_expectation(category_group_expectation)

    # ---------------------------------------------------------
    # P17 - grammy_artists: proporción mínima de artistas conocidos
    # ---------------------------------------------------------

    artists_known_expectation = gx.expectations.ExpectColumnValuesToNotBeInSet(
        column="grammy_artists",
        value_set=["unknown"],
        mostly=MIN_ARTIST_KNOWN,
        row_condition=GRAMMY_ROWS,
        condition_parser="pandas",
        severity="warning",
        meta={"rule_id": "P17"},
    )
    prepared_suite.add_expectation(artists_known_expectation)

    # ---------------------------------------------------------
    # P18 - category_group: proporción máxima de "Other" (informativa)
    # ---------------------------------------------------------

    other_group_expectation = gx.expectations.ExpectColumnValuesToNotBeInSet(
        column="category_group",
        value_set=["Other"],
        mostly=MIN_NOT_OTHER_GROUP,
        row_condition=GRAMMY_ROWS,
        condition_parser="pandas",
        severity="info",
        meta={"rule_id": "P18"},
    )
    prepared_suite.add_expectation(other_group_expectation)

    # ---------------------------------------------------------
    # Step 15 — Create the Validation Definition
    # ---------------------------------------------------------

    new_validation_definition = gx.ValidationDefinition(
        name="prepared_validation_definition",
        data=batch_definition,
        suite=prepared_suite,
    )

    validation_definition = context.validation_definitions.add(
        new_validation_definition
    )

    print("Validation Definition created successfully.")
    print(
        "Validation Definition name:",
        validation_definition.name,
    )
    
    # =========================================================
    # [PRUEBA CONTROLADA DE ERRORES - DESCOMENTAR PARA PROBAR]
    # =========================================================
    df_bad = df_integrated.copy()
    # # Falla P10 (Critical): Energy fuera de rango (0.0 - 1.0)
    df_bad.loc[df_bad.index[:20], "energy"] = 15.0
    # # Falla P11 (Critical): Popularity negativa
    df_bad.loc[df_bad.index[:20], "popularity"] = -100
     # Falla P09 (Critical): Inyectar nulos en track_genre
    df_bad.loc[df_bad.index[:20], "track_genre"] = None
    
    bad_validation_result = validation_definition.run(
         batch_parameters={"dataframe": df_bad},
         result_format={"result_format": "SUMMARY"}
     )
    print("\n--- [PRUEBA CONTROLADA DE FALLA TRANSFORMA] ---")
    print("Éxito de la validación con datos corruptos:", bad_validation_result.success)
    for res in bad_validation_result.results:
         if not res.success:
             print(f" -> Regla fallida: {res.expectation_config.meta.get('rule_id')} | Inesperados: {res.result.get('unexpected_count')}")
   


    # ---------------------------------------------------------
    # Step 16 — Run the Validation Definition
    # ---------------------------------------------------------

    validation_result = validation_definition.run(
        batch_parameters={
            "dataframe": df_integrated
        },
        result_format={
            "result_format": "SUMMARY"
        },
    )

    print("Validation executed successfully.")

    print(
        "Overall validation success:",
        validation_result.success,
    )
    

    # ---------------------------------------------------------
    # Step 17 — Inspect the Validation Results
    # ---------------------------------------------------------

    print("\nValidation Summary")
    print("------------------")
    print(
        "Overall success:",
        validation_result.success,
    )
    print(
        "Expectations evaluated:",
        len(validation_result.results),
    )

    filas_resumen = []

    for result in validation_result.results:
        expectation = result.expectation_config
        severity = getattr(expectation.severity, "value", expectation.severity)
        column = expectation.kwargs.get("column", expectation.kwargs.get("column_list", "(tabla)"))
        if isinstance(column, (list, tuple)):
            column = ", ".join(column)

        print("\nRule ID:", expectation.meta.get("rule_id"))
        print("Expectation:", expectation.type)
        print("Column:", column)
        print("Severity:", severity)
        print("Success:", result.success)
        print(
            "Unexpected count:",
            result.result.get("unexpected_count", 0),
        )
        print(
            "Unexpected percent:",
            result.result.get("unexpected_percent", 0),
        )

        filas_resumen.append({
            "rule_id": expectation.meta.get("rule_id"),
            "expectation": expectation.type,
            "column": column,
            "severity": severity,
            "success": bool(result.success),
            "element_count": result.result.get("element_count"),
            "unexpected_count": result.result.get("unexpected_count"),
            "unexpected_percent": result.result.get("unexpected_percent"),
        })

    summary = pd.DataFrame(filas_resumen)

    # ---------------------------------------------------------
    # Step 20 — Create and Register the Checkpoint
    # ---------------------------------------------------------

    new_checkpoint = gx.Checkpoint(
        name="prepared_validation_checkpoint",
        validation_definitions=[
            validation_definition
        ],
        actions=[],
        result_format={
            "result_format": "SUMMARY"
        },
    )

    checkpoint = context.checkpoints.add(
        new_checkpoint
    )

    print("Checkpoint created successfully.")
    print("Checkpoint name:", checkpoint.name)

    # ---------------------------------------------------------
    # Step 21 — Run the Checkpoint
    # ---------------------------------------------------------

    checkpoint_result = checkpoint.run(
        batch_parameters={
            "dataframe": df_integrated
        }
    )

    print("Checkpoint executed successfully.")
    print(
        "Checkpoint success:",
        checkpoint_result.success,
    )

    # ---------------------------------------------------------
    # Step 22 — Save the validation evidence (JSON + CSV)
    # ---------------------------------------------------------

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    (output_path / f"validate_prepared_{stamp}.json").write_text(
        json.dumps(validation_result.to_json_dict(), indent=2, default=str),
        encoding="utf-8",
    )
    summary.to_csv(output_path / f"validate_prepared_{stamp}_summary.csv", index=False)

    print("Evidence saved in:", output_path)

    # ---------------------------------------------------------
    # Step 23 — Apply the severity policy
    # ---------------------------------------------------------
    # Critical : detiene el pipeline (la tarea falla y load_dw no corre)
    # Warning  : se registra y el pipeline continúa
    # Info     : se registra para monitoreo

    failed = summary[~summary["success"]]
    critical_failed = failed[failed["severity"] == "critical"]
    warning_failed = failed[failed["severity"] == "warning"]

    if not warning_failed.empty:
        print("WARNING (el pipeline continúa):", warning_failed["rule_id"].tolist())

    if not critical_failed.empty:
        raise ValueError(
            "validate_prepared falló en reglas Critical: "
            + ", ".join(critical_failed["rule_id"].astype(str).tolist())
            + f" (evidencia en {output_path})"
        )

    return summary



def prepared_validations():
    """
    Arma el dataset integrado y lo valida:
      extract (Spotify CSV + Grammy desde la base) -> transform -> validations_prepared
    """

    PROJECT_ROOT = Path(__file__).resolve().parent.parent

    # Agregar la raíz al sys.path ANTES de importar src
    sys.path.append(str(PROJECT_ROOT))
    from src.extract_grammy import extract_grammys
    from src.transform import transform      # = transform_simple.py guardado como src/transform.py

    # ---------------------------------------------------------
    # Extract: las dos fuentes
    # ---------------------------------------------------------

    df_spotify = pd.read_csv(
        PROJECT_ROOT / "data" / "spotify_dataset.csv",
        sep=","
    )
    df_grammys = extract_grammys()

    # ---------------------------------------------------------
    # Transform: de aquí sale el dataset integrado
    # ---------------------------------------------------------

    df_integrated = transform(df_spotify, df_grammys)

    # ---------------------------------------------------------
    # Validate: puerta de datos preparados
    # ---------------------------------------------------------

    return validations_prepared(df_integrated)


if __name__ == "__main__":
    prepared_validations()