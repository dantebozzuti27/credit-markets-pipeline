"""Great Expectations setup and validation for credit markets data."""

import great_expectations as gx
from great_expectations import expectations as gxe
from pathlib import Path
from great_expectations.checkpoint import Checkpoint


def get_context():
    """Get or create a GX FileDataContext."""
    project_root = Path(__file__).parent.parent.parent.parent
    
    # Get or create context (file mode persists to disk)
    context = gx.get_context(mode="file", project_root_dir=str(project_root))
    return context


def create_fred_expectations():
    """Create expectations for FRED data validation."""
    
    expectations = [
        # Required columns exist
        gxe.ExpectColumnToExist(column="series_id"),
        gxe.ExpectColumnToExist(column="observation_date"),
        gxe.ExpectColumnToExist(column="value"),
        
        # No nulls in key columns
        gxe.ExpectColumnValuesToNotBeNull(column="series_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="observation_date"),
        
        # Series ID format (alphanumeric uppercase)
        gxe.ExpectColumnValuesToMatchRegex(
            column="series_id",
            regex=r"^[A-Z0-9]+$"
        ),
    ]
    
    return expectations


def create_fred_suite(context):
    """Create and save FRED expectation suite."""
    suite_name = "fred_data_suite"
    
    try:
        suite = context.suites.get(suite_name)
        return suite
    except Exception:
        pass

    suite = gx.ExpectationSuite(name=suite_name)

    for exp in create_fred_expectations():
        suite.add_expectation(exp)
    
    suite = context.suites.add(suite)
    return suite

def validate_fred_data(context, connection_string: str = None):
    """Validate FRED data in Postgres against expectations."""
    import os

    if connection_string is None:
        connection_string = os.environ.get(
            "DATABASE_URL",
            "postgresql://postgres:postgres@credit-markets-postgres:5432/credit_markets"
        )
    try:
        suite = context.suites.get("fred_data_suite")
    except Exception:
        suite = create_fred_suite(context)

    datasource_name = "postgres_silver"
    try:
        datasource = context.data_sources.get(datasource_name)
    except Exception:
        datasource = context.data_sources.add_postgres(
            name=datasource_name,
            connection_string=connection_string,
        )

    asset_name = "treasury_yields"
    try:
        data_asset = datasource.get_asset(asset_name)
    except Exception:
        data_asset = datasource.add_table_asset(
            name=asset_name,
            table_name="treasury_yields",
            schema_name='silver',
        )

    batch_def_name = "full_table"
    try:
        batch_definition = data_asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_definition = data_asset.add_batch_definition_whole_table(
            name=batch_def_name
        )
    
    batch = batch_definition.get_batch()
    result = batch.validate(suite)

    return result

def create_treasury_checkpoint(context):
    """Create a checkpoint for treasury yields validation."""

    suite = create_fred_suite(context)

    import os
    connection_string = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/credit_markets"
    )

    try:
        datasource = context.data_sources.get("postgres_silver")
    except Exception:
        datasource = context.data_sources.add_postgres(
            name="postgres_silver",
            connection_string=connection_string,
        )
    
    try:
        data_asset = datasource.get_asset("treasury_yields")
    except Exception:
        data_asset = datasource.add_table_asset(
            name="treasury_yields",
            table_name="treasury_yields",
            schema_name="silver"
        )

    try:
        batch_definition = data_asset.get_batch_definition("full_table")
    except Exception:
        batch_definition = data_asset.add_batch_definition_whole_table(
            name="full_table"
        )

    validation_definition = gx.ValidationDefinition(
        name="treasury_validation",
        data=batch_definition,
        suite=suite,
        
    )
    validation_definition = gx.ValidationDefinition(
        name="treasury_validation",
        data=batch_definition,
        suite=suite,
    )

    validation_definition = context.validation_definitions.add(validation_definition)

    checkpoint = Checkpoint(
        name="treasury_checkpoint",
        validation_definitions=[validation_definition],
    )

    checkpoint = context.checkpoints.add(checkpoint)

    return checkpoint

if __name__ == "__main__":
    print("Initializing Great Expectations context...")
    ctx = get_context()
    print(f"Context created at: {ctx.root_directory}")
    
    print("\nCreating FRED data expectation suite...")
    suite = create_fred_suite(ctx)
    print(f"Suite created: {suite.name}")
    print(f"Expectations: {len(suite.expectations)}")
    
    for exp in suite.expectations:
        print(f"  - {exp.__class__.__name__}")
    
    print("\n--- Validating FRED data---")
    result = validate_fred_data(ctx)
    print(f"Success: {result.success}")
    print(f"Statistics: {result.statistics}")

    print("\n--- Creating Checkpoint ---")
    checkpoint = create_treasury_checkpoint(ctx)
    print(f"Checkpoint created: {checkpoint.name}")

    print("\n--- Running Checkpoint ---")
    result = checkpoint.run()
    print(f"Success: {result.success}")