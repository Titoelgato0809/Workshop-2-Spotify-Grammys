from pathlib import Path
import sys

import pandas as pd
import great_expectations as gx



# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

def validations_spotify():
    
    
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    DATA_PATH = (
        PROJECT_ROOT
        / "data"
        / "spotify_dataset.csv"
    )

    # ---------------------------------------------------------
    # Load the Spotify Applications dataset
    # ---------------------------------------------------------

    df = pd.read_csv(
        DATA_PATH,
        sep=","
    )

    print("Dataset shape:", df.shape)


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
        name="Spotify_pandas_source"
    )

    print("Pandas Data Source created successfully.")
    print("Data Source name:", data_source.name)

    # ---------------------------------------------------------
    # Create the Data Asset
    # ---------------------------------------------------------

    data_asset = data_source.add_dataframe_asset(
        name="Spotify_applications"
    )

    print("Data Asset created successfully.")
    print("Data Asset name:", data_asset.name)

    # ---------------------------------------------------------
    # Create the Batch Definition
    # ---------------------------------------------------------

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="whole_spotify_dataframe"
    )

    print("Batch Definition created successfully.")
    print("Batch Definition name:", batch_definition.name)

    # ---------------------------------------------------------
    # Create the Batch
    # ---------------------------------------------------------

    batch_parameters = {
        "dataframe": df
    }

    batch = batch_definition.get_batch(
        batch_parameters=batch_parameters
    )

    print("Batch created successfully.")
    print("Batch type:", type(batch).__name__)

    # ---------------------------------------------------------
    # Step 12- Define and Test the First Expectation
    # track_id 
    # ---------------------------------------------------------

    spotify_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="spotify_suite")
    )

    track_id_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="track_id",
        mostly=1.0,
        severity="critical",
    )
    spotify_suite.add_expectation(track_id_expectation)
    
    # ---------------------------------------------------------
    # Step 12- Define and Test the Second Expectation
    # Artists
    # ---------------------------------------------------------

    artists_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
    column= "artists",
    mostly=0.98,
    severity= "critical"
    )
    spotify_suite.add_expectation(artists_expectation)

    # --------------------------------------------------------
    # Step 12- Define and Test the Second Expectation
    # Track_name
    # ---------------------------------------------------------

    track_name_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
    column= "track_name",
    mostly = 0.98,
    severity= "critical"
    )
    spotify_suite.add_expectation(track_name_expectation)
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 4 Expectation
    # album_name
    # ---------------------------------------------------------
    album_name_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
    column= "album_name",
    mostly = 0.98,
    severity= "critical"
    )
    spotify_suite.add_expectation(album_name_expectation)
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 5 Expectation
    # explicit
    # ---------------------------------------------------------
    
    explicit_expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="explicit",
        value_set=[True, False],
        mostly=0.98,
        severity="warning"
    )
    spotify_suite.add_expectation(explicit_expectation)
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 5 Expectation
    # energy
    # ---------------------------------------------------------
    
    energy_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="energy",
        min_value=0.0,
        max_value=1.0,
        mostly=0.98,
        severity="critical"
    )
    spotify_suite.add_expectation(energy_expectation)
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 5 Expectation
    # popularity
    # --------------------------------------------------------
    
    popularity_expectation= gx.expectations.ExpectColumnValuesToBeBetween(
        column="popularity",
        min_value=0,
        max_value=100,
        mostly=0.98,
        severity="critical"
    )
    spotify_suite.add_expectation(popularity_expectation)
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 5 Expectation
    # duration_ms
    # --------------------------------------------------------
    
    duration_ms_expectation= gx.expectations.ExpectColumnValuesToBeBetween(
        column="duration_ms",
        min_value=0,
        mostly = 1.0,
        severity="critical"
    )
    spotify_suite.add_expectation(duration_ms_expectation)
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 5 Expectation
    # danceability
    # --------------------------------------------------------
    
    danceability_expectation= gx.expectations.ExpectColumnValuesToBeBetween(
        column="danceability",
        min_value=0.0,
        max_value=1.0,
        mostly=0.98,
        severity="critical"
    )
    
    spotify_suite.add_expectation(danceability_expectation)    
    
    
    # ---------------------------------------------------------
    # Step 12-Define and Test the 5 Expectation
    # duration_ms
    # --------------------------------------------------------
    
    duration_ms_expectation= gx.expectations.ExpectColumnValuesToBeBetween(
        column="duration_ms",
        min_value=30000,
        severity="warning",
        mostly=0.96
    )
    spotify_suite.add_expectation(duration_ms_expectation)
    
    # ---------------------------------------------------------
    # Step 15 — Create the Validation Definition
    # ---------------------------------------------------------

    new_validation_definition = gx.ValidationDefinition(
        name="spotify_validation_definition",
        data=batch_definition,
        suite=spotify_suite,
    )

    validation_definition = context.validation_definitions.add(
        new_validation_definition
    )

    print("Validation Definition created successfully.")
    print(
        "Validation Definition name:",
        validation_definition.name,
    )

    # ---------------------------------------------------------
    # Step 16 — Run the Validation Definition
    # ---------------------------------------------------------

    validation_result = validation_definition.run(
        batch_parameters={
            "dataframe": df
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
    
    # =========================================================
    # [PRUEBA CONTROLADA DE ERRORES SPOTIFY - DESCOMENTAR PARA PROBAR]
    # =========================================================
    # df_bad_spotify = df.copy()
    # # Inyectar valores nulos en track_id (Falla Critical)
    # df_bad_spotify.loc[df_bad_spotify.index[:50], "track_id"] = None
    # # Inyectar energía fuera de rango (Falla Critical)
    # df_bad_spotify.loc[df_bad_spotify.index[:50], "energy"] = 99.0
    #
    # bad_spotify_result = validation_definition.run(
    #     batch_parameters={"dataframe": df_bad_spotify},
    #     result_format={"result_format": "SUMMARY"}
    # )
    # print("\n--- [PRUEBA CONTROLADA SPOTIFY] ---")
    # print("Éxito de la validación con datos corruptos:", bad_spotify_result.success)
    # =========================================================
    
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

    for result in validation_result.results:
        expectation = result.expectation

        print("\nExpectation:", type(expectation).__name__)
        print("Column:", getattr(expectation, "column", None))
        print("Severity:", expectation.severity)
        print("Success:", result.success)
        print(
            "Unexpected count:",
            result.result.get("unexpected_count", 0),
        )
        print(
            "Unexpected percent:",
            result.result.get("unexpected_percent", 0),
        )
    # ---------------------------------------------------------
    # Step 20 — Create and Register the Checkpoint
    # ---------------------------------------------------------

    new_checkpoint = gx.Checkpoint(
        name="candidate_validation_checkpoint",
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
            "dataframe": df
        }
    )

    print("Checkpoint executed successfully.")
    print(
        "Checkpoint success:",
        checkpoint_result.success,
    )







def grammy_validations():
    
    PROJECT_ROOT = Path(__file__).resolve().parent.parent

    # 2. Agregar la raíz al sys.path ANTES de importar src
    sys.path.append(str(PROJECT_ROOT))
    from src.extract_grammy import extract_grammys
    extract_grammys()
    
    # ---------------------------------------------------------
    # Load the Spotify Applications dataset
    # --------------------------------------------------------- 
    
    df_grammys = extract_grammys()
    
    print("Dataset shape:", df_grammys.shape)
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
        name="Spotify_pandas_source"
    )

    print("Pandas Data Source created successfully.")
    print("Data Source name:", data_source.name)

    # ---------------------------------------------------------
    # Create the Data Asset
    # ---------------------------------------------------------

    data_asset = data_source.add_dataframe_asset(
        name="Grammys_applications"
    )

    print("Data Asset created successfully.")
    print("Data Asset name:", data_asset.name)

    # ---------------------------------------------------------
    # Create the Batch Definition
    # ---------------------------------------------------------

    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        name="whole_grammys_dataframe"
    )

    print("Batch Definition created successfully.")
    print("Batch Definition name:", batch_definition.name)

    # ---------------------------------------------------------
    # Create the Batch
    # ---------------------------------------------------------

    batch_parameters = {
        "dataframe": df_grammys
    }

    batch = batch_definition.get_batch(
        batch_parameters=batch_parameters
    )

    print("Batch created successfully.")
    print("Batch type:", type(batch).__name__)

    # ---------------------------------------------------------
    # Step 12- Define and Test the First Expectation
    # year 1958 values
    # ---------------------------------------------------------

    grammys_suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="grammys_suite")
    )

    year_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="year",
        min_value= 1958,
        mostly=1.0,
        severity="critical",
    )
    grammys_suite.add_expectation(year_expectation)

    # ---------------------------------------------------------
    # Step 12- Define and Test the First Expectation
    # year range[`1958`,'2019'] values
    # ---------------------------------------------------------
    
    year_range_expectation = gx.expectations.ExpectColumnValuesToBeBetween(
        column="year",
        min_value=1958,
        max_value=2019,
        mostly=0.98,
        severity="warning",
    )
    grammys_suite.add_expectation(year_range_expectation)
    
    # ---------------------------------------------------------
    # Step 12- Define and Test the First Expectation
    # category values 
    # ---------------------------------------------------------
    
    category_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="category",
        mostly=0.98,
        severity="critical",
    )
    grammys_suite.add_expectation(category_expectation)

    # ---------------------------------------------------------
    # Step 12- Define and Test the First Expectation
    # nominee values 
    # ---------------------------------------------------------
    
    nominee_expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
        column="nominee",
        mostly=0.98,
        severity="critical",
    )
    grammys_suite.add_expectation(nominee_expectation)
    
    # ---------------------------------------------------------
    # Step 15 — Create the Validation Definition
    # ---------------------------------------------------------

    new_validation_definition = gx.ValidationDefinition(
        name="grammys_validation_definition",
        data=batch_definition,
        suite=grammys_suite,
    )

    validation_definition = context.validation_definitions.add(
        new_validation_definition
    )

    print("Validation Definition created successfully.")
    print(
        "Validation Definition name:",
        validation_definition.name,
    )

    # ---------------------------------------------------------
    # Step 16 — Run the Validation Definition
    # ---------------------------------------------------------

    validation_result = validation_definition.run(
        batch_parameters={
            "dataframe": df_grammys
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
    # =========================================================
    # [PRUEBA CONTROLADA DE ERRORES GRAMMYS - DESCOMENTAR PARA PROBAR]
    # =========================================================
    # df_bad_grammys = df_grammys.copy()
    # # Inyectar años fuera de rango (Falla Critical)
    # df_bad_grammys.loc[df_bad_grammys.index[:50], "year"] = 1800
    # # Inyectar categorías nulas (Falla Critical)
    # df_bad_grammys.loc[df_bad_grammys.index[:50], "category"] = None
    #
    # bad_grammys_result = validation_definition.run(
    #     batch_parameters={"dataframe": df_bad_grammys},
    #     result_format={"result_format": "SUMMARY"}
    # )
    # print("\n--- [PRUEBA CONTROLADA GRAMMYS] ---")
    # print("Éxito de la validación con datos corruptos:", bad_grammys_result.success)
    
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

    for result in validation_result.results:
        expectation = result.expectation

        print("\nExpectation:", type(expectation).__name__)
        print("Column:", getattr(expectation, "column", None))
        print("Severity:", expectation.severity)
        print("Success:", result.success)
        print(
            "Unexpected count:",
            result.result.get("unexpected_count", 0),
        )
        print(
            "Unexpected percent:",
            result.result.get("unexpected_percent", 0),
        )
    # ---------------------------------------------------------
    # Step 20 — Create and Register the Checkpoint
    # ---------------------------------------------------------

    new_checkpoint = gx.Checkpoint(
        name="grammys_validation_checkpoint",
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
            "dataframe": df_grammys
        }
    )

    print("Checkpoint executed successfully.")
    print(
        "Checkpoint success:",
        checkpoint_result.success,
    )
validations_spotify()
grammy_validations()