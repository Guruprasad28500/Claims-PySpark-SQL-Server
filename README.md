# Claims PySpark + SQL Server

PySpark pipeline to read claims data from SQL Server, run transformations, window/UDF analytics, multi-table joins (`PolicyRates`, `ClaimHandlers`, `ClaimStatus`), and optional pivot/export outputs.

## Project Structure

- `app.py` - Main pipeline script (function-based flow)
- `jars/` - JDBC driver and auth DLL (for JDBC mode)
- `README.md` - Setup and run guide

## Requirements

- Windows
- Python virtual environment at `.venv`
- SQL Server with database/tables:
  - `dbo.Claims`
  - `dbo.PolicyRates`
  - `dbo.ClaimHandlers`
  - `dbo.ClaimStatus`
- ODBC Driver 18 for SQL Server (for ODBC mode)

## Run (recommended)

Use the project venv Python:

```powershell
C:/Users/GuruprasadMekala/claims_pyspark_db/.venv/Scripts/python.exe app.py
```

## Environment Variables

Connection:

- `SQL_READ_MODE` = `odbc` or `jdbc` (default: `odbc`)
- `SQL_SERVER_HOST` (default: `localhost`)
- `SQL_SERVER_INSTANCE` (default: `SQLEXPRESS`)
- `SQL_SERVER_PORT` (default: `1433`)
- `SQL_SERVER_DB` (default: `InsuranceDB`)

Tables/keys:

- `POLICY_RATES_TABLE` (default: `dbo.PolicyRates`)
- `CLAIM_HANDLER_TABLE` (default: `dbo.ClaimHandlers`)
- `CLAIM_HANDLER_JOIN_KEY` (default: `policy_type`)
- `CLAIM_STATUS_TABLE` (default: `dbo.ClaimStatus`)
- `CLAIM_STATUS_JOIN_KEY` (default: `claim_status`)

Run control flags:

- `RUN_ALL_SEQUENCE` (default: `1`) - runs all sections in sequence
- `RUN_BASIC` (default: `1`)
- `RUN_WINDOW` (default: `1`)
- `RUN_UDF` (default: `1`)
- `RUN_QUALITY` (default: `1`) - runs data quality checks
- `RUN_PROFILE` (default: `1`) - runs data profiling
- `RUN_JOINS` (default: `1`)
- `RUN_JOIN_DEMOS` (default: `1`)
- `RUN_MULTIPLE_JOIN_ONLY` (default: `0`)
- `RUN_PIVOT` (default: `1`)
- `FAST_MODE` (default: `1`) - skips heavy count operations
- `PREVIEW_ROWS` (default: `5`) - rows shown in previews
- `PROFILE_TOP_N` (default: `10`) - top values shown for categorical profiling

Quality checks:

- `ALLOWED_CLAIM_STATUSES` (default: `Approved,Pending,Rejected`) - valid status domain for quality validation

Output/export:

- `EXPORT_OUTPUTS` (default: `0`) - when `1`, writes parquet outputs
- `OUTPUT_DIR` (default: `./output`) - output root directory

## Common Commands

Full sequence (fast):

```powershell
$env:FAST_MODE='1'; $env:PREVIEW_ROWS='5'; $env:RUN_ALL_SEQUENCE='1'; C:/Users/GuruprasadMekala/claims_pyspark_db/.venv/Scripts/python.exe app.py
```

Only multiple joins:

```powershell
$env:RUN_ALL_SEQUENCE='0'; $env:RUN_JOINS='1'; $env:RUN_MULTIPLE_JOIN_ONLY='1'; $env:RUN_JOIN_DEMOS='0'; $env:RUN_WINDOW='0'; $env:RUN_UDF='0'; $env:RUN_PIVOT='0'; $env:RUN_BASIC='0'; C:/Users/GuruprasadMekala/claims_pyspark_db/.venv/Scripts/python.exe app.py
```

Full sequence + export parquet outputs:

```powershell
$env:RUN_ALL_SEQUENCE='1'; $env:EXPORT_OUTPUTS='1'; $env:OUTPUT_DIR='C:/Users/GuruprasadMekala/claims_pyspark_db/output'; C:/Users/GuruprasadMekala/claims_pyspark_db/.venv/Scripts/python.exe app.py
```

Full sequence + profiling focus:

```powershell
$env:RUN_ALL_SEQUENCE='1'; $env:RUN_PROFILE='1'; $env:PROFILE_TOP_N='15'; C:/Users/GuruprasadMekala/claims_pyspark_db/.venv/Scripts/python.exe app.py
```

## Notes

- If you run `python app.py` and see `ModuleNotFoundError: pyodbc`, you are likely using global Python instead of `.venv`.
- Occasional Spark temp-folder cleanup warnings on Windows (`DiskBlockManager`) are usually non-fatal when exit code is `0`.
- Refactored script functions in `app.py`: `run_basic_ops`, `run_window_ops`, `run_udf_ops`, `run_join_ops`, `export_outputs`.
- Data quality function in `app.py`: `run_quality_checks` (nulls, duplicates, ranges, business rules, status-domain checks).
- Quality checks now print both failed counts and sample issue records (including `policy_id`) per rule.
- Data profiling function in `app.py`: `run_data_profiling` (row count, schema, null/distinct counts, numeric summary, top categorical values).
