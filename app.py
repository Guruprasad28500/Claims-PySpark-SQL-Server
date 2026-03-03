import os
from pathlib import Path
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,row_number,rank,dense_rank,lag,lead,avg as avg_func,udf,lit,abs as sql_abs,count,when,expr
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType



# 1️  Path Setup
workspace_dir = Path(__file__).resolve().parent
jdbc_jar = workspace_dir / "jars" / "mssql-jdbc-12.6.1.jre8.jar"
auth_dll = workspace_dir / "jars" / "mssql-jdbc_auth-12.6.1.x64.dll"
sql_host = os.getenv("SQL_SERVER_HOST", "localhost")
sql_port = os.getenv("SQL_SERVER_PORT", "1433")
sql_instance = os.getenv("SQL_SERVER_INSTANCE", "SQLEXPRESS").strip()
sql_database = os.getenv("SQL_SERVER_DB", "InsuranceDB")
sql_read_mode = os.getenv("SQL_READ_MODE", "odbc").strip().lower()
target_server = f"{sql_host}\\{sql_instance}" if sql_instance else f"{sql_host}:{sql_port}"

# Add DLL path to environment
auth_dir = str(auth_dll.parent)
os.environ["PATH"] = auth_dir + os.pathsep + os.environ.get("PATH", "")
spark_env_path = auth_dir + os.pathsep + os.environ.get("PATH", "")

# 2️  Spark Session
spark_builder = SparkSession.builder.appName("SQLServerConnection")

if sql_read_mode == "jdbc":
    if not jdbc_jar.exists():
        raise FileNotFoundError("JDBC jar file not found inside jars folder.")
    if not auth_dll.exists():
        raise FileNotFoundError("mssql-jdbc_auth-12.6.1.x64.dll not found inside jars folder.")

    spark_builder = (
        spark_builder
        .config("spark.jars", str(jdbc_jar))
        .config("spark.driver.extraLibraryPath", auth_dir)
        .config("spark.executor.extraLibraryPath", auth_dir)
        .config("spark.driver.extraJavaOptions", f"-Djava.library.path={auth_dir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.library.path={auth_dir}")
        .config("spark.driverEnv.PATH", spark_env_path)
        .config("spark.executorEnv.PATH", spark_env_path)
    )

spark = spark_builder.getOrCreate()
print(f"SQL read mode: {sql_read_mode} | server: {target_server} | database: {sql_database}")


def env_flag(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return int(default)


run_window = env_flag("RUN_WINDOW", "1")
run_udf = env_flag("RUN_UDF", "1")
run_quality = env_flag("RUN_QUALITY", "1")
run_profile = env_flag("RUN_PROFILE", "1")
run_joins = env_flag("RUN_JOINS", "1")
run_join_demos = env_flag("RUN_JOIN_DEMOS", "1")
run_multiple_join_only = env_flag("RUN_MULTIPLE_JOIN_ONLY", "0")
run_pivot = env_flag("RUN_PIVOT", "1")
run_basic = env_flag("RUN_BASIC", "1")
fast_mode = env_flag("FAST_MODE", "1")
preview_rows = env_int("PREVIEW_ROWS", "5")
profile_top_n = env_int("PROFILE_TOP_N", "10")
run_all_sequence = env_flag("RUN_ALL_SEQUENCE", "1")

if run_all_sequence:
    run_basic = True
    run_window = True
    run_udf = True
    run_quality = True
    run_profile = True
    run_joins = True
    run_join_demos = True
    run_pivot = True
    run_multiple_join_only = False

if run_multiple_join_only:
    run_basic = False
    run_window = False
    run_udf = False
    run_quality = False
    run_profile = False
    run_join_demos = False
    run_pivot = False

# 3️  JDBC URL (Windows Auth)
jdbc_url = (
    "jdbc:sqlserver://localhost:1433;"
    "databaseName=InsuranceDB;"
    "integratedSecurity=true;"
    "encrypt=true;"
    "trustServerCertificate=true;"
)

connection_properties = {
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


def load_with_odbc(table_name: str):
    odbc_driver = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
    if sql_instance:
        odbc_server = f"{sql_host}\\{sql_instance}"
    else:
        odbc_server = f"{sql_host},{sql_port}"

    conn_str = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={odbc_server};"
        f"DATABASE={sql_database};"
        "Trusted_Connection=Yes;"
        "Encrypt=Yes;"
        "TrustServerCertificate=Yes;"
    )

    with pyodbc.connect(conn_str, timeout=int(float(os.getenv("SQL_SERVER_CONNECT_TIMEOUT", "5")))) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description]

    rows_as_tuples = [tuple(row) for row in rows]
    return spark.createDataFrame(rows_as_tuples, schema=columns)


def load_sql_table(table_name: str):
    if sql_read_mode == "jdbc":
        try:
            return spark.read.jdbc(
                url=jdbc_url,
                table=table_name,
                properties=connection_properties
            )
        except Exception as exc:
            if "integrated authentication" in str(exc).lower():
                print(f"JDBC integrated auth failed for {table_name}; using ODBC fallback.")
                return load_with_odbc(table_name)
            raise
    return load_with_odbc(table_name)


def run_basic_ops(df_base, preview_rows_count: int):
    df_base.createOrReplaceTempView("claims")
    agg_df_local = df_base.groupBy("policy_type").agg(sum("net_claim").alias("total_net_claim"))
    agg_df_local.show(preview_rows_count)
    spark.sql("""
    SELECT policy_type,
           SUM(net_claim) AS total_net_claim,
           COUNT(*) AS total_policies
    FROM claims
    GROUP BY policy_type
    """).show(preview_rows_count)
    df_base.show(preview_rows_count)

    rdd_local = df_base.rdd
    total_premium_local = rdd_local.map(lambda row: row.premium).reduce(lambda left, right: left + right)
    print("Total Premium:", total_premium_local)


def run_window_ops(df_base, preview_rows_count: int):
    print("\n=== WINDOW OPERATIONS ===\n")
    window_by_policy_type = Window.partitionBy("policy_type")
    window_by_policy_type_ordered = Window.partitionBy("policy_type").orderBy(col("claim_amount").desc())

    print("1. Row Number within each policy type:")
    df_base.withColumn("row_num", row_number().over(window_by_policy_type_ordered)) \
        .select("policy_type", "claim_amount", "row_num").show(preview_rows_count)

    print("\n2. Rank by claim amount within policy type:")
    df_base.withColumn("rank", rank().over(window_by_policy_type_ordered)) \
        .select("policy_type", "claim_amount", "rank").show(preview_rows_count)

    print("\n3. Dense Rank by claim amount within policy type:")
    df_base.withColumn("dense_rank", dense_rank().over(window_by_policy_type_ordered)) \
        .select("policy_type", "claim_amount", "dense_rank").show(preview_rows_count)

    print("\n4. Lag - Previous row's claim amount:")
    df_base.withColumn("previous_claim", lag("claim_amount", 1).over(window_by_policy_type_ordered)) \
        .select("policy_type", "claim_amount", "previous_claim").show(preview_rows_count)

    print("\n5. Lead - Next row's claim amount:")
    df_base.withColumn("next_claim", lead("claim_amount", 1).over(window_by_policy_type_ordered)) \
        .select("policy_type", "claim_amount", "next_claim").show(preview_rows_count)

    print("\n6. Running Sum of claims within policy type:")
    window_running_sum_spec = Window.partitionBy("policy_type").orderBy(col("policy_type"))
    df_base.withColumn("running_sum", sum("claim_amount").over(window_running_sum_spec)) \
        .select("policy_type", "claim_amount", "running_sum").show(preview_rows_count)

    print("\n7. Average claim amount within policy type:")
    df_base.withColumn("avg_claim", avg_func("claim_amount").over(window_by_policy_type)) \
        .select("policy_type", "claim_amount", "avg_claim").show(preview_rows_count)

    print("\n8. Difference from policy type average:")
    df_base.withColumn("avg_claim", avg_func("claim_amount").over(window_by_policy_type)) \
        .withColumn("diff_from_avg", col("claim_amount") - col("avg_claim")) \
        .select("policy_type", "claim_amount", "avg_claim", "diff_from_avg").show(preview_rows_count)

    print("\n9. Combined window functions:")
    df_base.withColumn("row_num", row_number().over(window_by_policy_type_ordered)) \
        .withColumn("rank", rank().over(window_by_policy_type_ordered)) \
        .withColumn("previous_claim", lag("claim_amount").over(window_by_policy_type_ordered)) \
        .select("policy_type", "claim_amount", "row_num", "rank", "previous_claim").show(preview_rows_count)

    print("\n=== WINDOW OPERATIONS COMPLETE ===")


def run_udf_ops(df_base, preview_rows_count: int):
    print("\n=== USER DEFINED FUNCTIONS (UDF) ===\n")

    def categorize_claim(amount):
        if amount is None:
            return "Unknown"
        if amount < 500:
            return "Low"
        if amount < 2000:
            return "Medium"
        return "High"

    categorize_claim_udf = udf(categorize_claim, StringType())
    print("1. Categorize Claims by Amount:")
    df_base.withColumn("claim_category", categorize_claim_udf(col("claim_amount"))) \
        .select("claim_amount", "claim_category").show(preview_rows_count)

    def calculate_risk_score(claim_amount, recovery_amount):
        if claim_amount is None or recovery_amount is None:
            return 0.0
        net_local = claim_amount - recovery_amount
        if net_local <= 0:
            return 0.0
        if net_local < 500:
            return 1.0
        if net_local < 1500:
            return 2.5
        return 5.0

    calculate_risk_udf = udf(calculate_risk_score, DoubleType())
    print("\n2. Calculate Risk Score:")
    df_base.withColumn("risk_score", calculate_risk_udf(col("claim_amount"), col("recovery_amount"))) \
        .select("claim_amount", "recovery_amount", "risk_score").show(preview_rows_count)

    def get_policy_status(net_claim_amount):
        if net_claim_amount is None:
            return "Pending"
        if net_claim_amount > 3000:
            return "High Risk"
        if net_claim_amount > 1000:
            return "Medium Risk"
        return "Low Risk"

    policy_status_udf = udf(get_policy_status, StringType())
    print("\n3. Policy Status based on Net Claim:")
    df_base.withColumn("policy_status", policy_status_udf(col("net_claim"))) \
        .select("claim_amount", "recovery_amount", "net_claim", "policy_status").show(preview_rows_count)


def run_quality_checks(df_base, preview_rows_count: int):
    print("\n=== DATA QUALITY CHECKS ===\n")

    def show_issue_rows(title: str, issue_df):
        print(f"\n{title}")
        issue_df.select(
            "policy_id",
            "policy_type",
            "claim_amount",
            "recovery_amount",
            "premium",
            "claim_status",
            "net_claim",
        ).show(preview_rows_count, truncate=False)

    required_columns = ["policy_id", "policy_type", "claim_amount", "recovery_amount", "claim_status", "premium"]
    for column_name in required_columns:
        null_rows_df = df_base.filter(col(column_name).isNull())
        null_count = null_rows_df.count()
        print(f"Null check - {column_name}: {null_count}")
        if null_count > 0:
            show_issue_rows(f"Records with NULL in {column_name}:", null_rows_df)

    duplicate_policy_ids_df = df_base.groupBy("policy_id").count().filter(col("count") > 1)
    duplicate_count = duplicate_policy_ids_df.count()
    print(f"Duplicate policy_id groups: {duplicate_count}")
    if duplicate_count > 0:
        duplicate_rows_df = df_base.join(duplicate_policy_ids_df.select("policy_id"), on="policy_id", how="inner")
        show_issue_rows("Records with duplicate policy_id:", duplicate_rows_df)

    negative_claim_df = df_base.filter(col("claim_amount") < 0)
    negative_claim_count = negative_claim_df.count()
    negative_recovery_df = df_base.filter(col("recovery_amount") < 0)
    negative_recovery_count = negative_recovery_df.count()
    invalid_premium_df = df_base.filter(col("premium") <= 0)
    invalid_premium_count = invalid_premium_df.count()
    print(f"Range check - claim_amount < 0: {negative_claim_count}")
    print(f"Range check - recovery_amount < 0: {negative_recovery_count}")
    print(f"Range check - premium <= 0: {invalid_premium_count}")
    if negative_claim_count > 0:
        show_issue_rows("Records with claim_amount < 0:", negative_claim_df)
    if negative_recovery_count > 0:
        show_issue_rows("Records with recovery_amount < 0:", negative_recovery_df)
    if invalid_premium_count > 0:
        show_issue_rows("Records with premium <= 0:", invalid_premium_df)

    recovery_gt_claim_df = df_base.filter(col("recovery_amount") > col("claim_amount"))
    recovery_gt_claim_count = recovery_gt_claim_df.count()
    print(f"Business rule - recovery_amount > claim_amount: {recovery_gt_claim_count}")
    if recovery_gt_claim_count > 0:
        show_issue_rows("Records where recovery_amount > claim_amount:", recovery_gt_claim_df)

    net_claim_mismatch_df = df_base.filter(sql_abs(col("net_claim") - (col("claim_amount") - col("recovery_amount"))) > 0.0001)
    net_claim_mismatch = net_claim_mismatch_df.count()
    print(f"Consistency check - net_claim mismatch: {net_claim_mismatch}")
    if net_claim_mismatch > 0:
        show_issue_rows("Records with net_claim mismatch:", net_claim_mismatch_df)

    allowed_statuses = [status.strip() for status in os.getenv("ALLOWED_CLAIM_STATUSES", "Approved,Pending,Rejected").split(",") if status.strip()]
    invalid_status_df = df_base.filter(~col("claim_status").isin(allowed_statuses))
    invalid_status_count = invalid_status_df.count()
    print(f"Domain check - invalid claim_status: {invalid_status_count}")
    if invalid_status_count > 0:
        show_issue_rows("Records with invalid claim_status:", invalid_status_df)

    print("\nSample invalid rows (recovery > claim or invalid status):")
    df_base.filter((col("recovery_amount") > col("claim_amount")) | (~col("claim_status").isin(allowed_statuses))) \
        .select("policy_id", "policy_type", "claim_amount", "recovery_amount", "claim_status", "net_claim") \
        .show(preview_rows_count, truncate=False)

    print("\n=== DATA QUALITY CHECKS COMPLETE ===")


def run_data_profiling(df_base, preview_rows_count: int, top_n: int):
    print("\n=== DATA PROFILING ===\n")
    print(f"Total rows: {df_base.count()}")
    print("Schema:")
    df_base.printSchema()

    print("\nNull counts by column:")
    null_exprs = [count(when(col(column_name).isNull(), 1)).alias(column_name) for column_name in df_base.columns]
    df_base.select(*null_exprs).show(truncate=False)

    print("\nDistinct counts by column:")
    distinct_exprs = [expr(f"count(distinct `{column_name}`) as `{column_name}`") for column_name in df_base.columns]
    df_base.select(*distinct_exprs).show(truncate=False)

    numeric_cols = [name for name, dtype in df_base.dtypes if dtype in {"int", "bigint", "double", "float", "smallint", "tinyint"} or dtype.startswith("decimal")]
    if numeric_cols:
        print("\nNumeric summary:")
        df_base.select(*numeric_cols).summary("count", "min", "25%", "50%", "75%", "max", "mean", "stddev").show(truncate=False)

    for cat_col in ["policy_type", "claim_status", "customer_name"]:
        if cat_col in df_base.columns:
            print(f"\nTop {top_n} values for {cat_col}:")
            df_base.groupBy(cat_col).count().orderBy(col("count").desc()).show(top_n, truncate=False)

    print("\n=== DATA PROFILING COMPLETE ===")


def prepare_reference_tables(df_base):
    policy_rates_table = os.getenv("POLICY_RATES_TABLE", "dbo.PolicyRates")
    claim_handler_table = os.getenv("CLAIM_HANDLER_TABLE", "dbo.ClaimHandlers")
    claim_handler_join_key = os.getenv("CLAIM_HANDLER_JOIN_KEY", "policy_type")
    claim_status_table = os.getenv("CLAIM_STATUS_TABLE", "dbo.ClaimStatus")
    claim_status_join_key = os.getenv("CLAIM_STATUS_JOIN_KEY", "claim_status")

    policy_rates_df = load_sql_table(policy_rates_table)
    claim_handler_df = load_sql_table(claim_handler_table)
    claim_status_df = load_sql_table(claim_status_table)

    rate_col_candidates = ["rate_factor", "Rate", "rate", "rateFactor"]
    rate_col = next((name for name in rate_col_candidates if name in policy_rates_df.columns), None)
    if rate_col is None:
        raise ValueError(f"Rate column not found in PolicyRates columns: {policy_rates_df.columns}")
    if rate_col != "rate_factor":
        policy_rates_df = policy_rates_df.withColumnRenamed(rate_col, "rate_factor")

    handler_name_candidates = ["handler_name", "HandlerName", "handler", "name"]
    handler_name_col = next((name for name in handler_name_candidates if name in claim_handler_df.columns), None)
    if handler_name_col and handler_name_col != "handler_name":
        claim_handler_df = claim_handler_df.withColumnRenamed(handler_name_col, "handler_name")
    elif not handler_name_col:
        claim_handler_df = claim_handler_df.withColumn("handler_name", lit(None).cast(StringType()))

    status_desc_candidates = ["status_description", "StatusDescription", "description", "status_desc"]
    status_desc_col = next((name for name in status_desc_candidates if name in claim_status_df.columns), None)
    if status_desc_col and status_desc_col != "status_description":
        claim_status_df = claim_status_df.withColumnRenamed(status_desc_col, "status_description")
    elif not status_desc_col:
        claim_status_df = claim_status_df.withColumn("status_description", lit(None).cast(StringType()))

    amount_col_candidates = ["amount", "Amount", "claim_amount", "ClaimAmount"]
    amount_col = next((name for name in amount_col_candidates if name in claim_status_df.columns), None)
    if amount_col and amount_col != "amount":
        claim_status_df = claim_status_df.withColumnRenamed(amount_col, "amount")

    if claim_status_join_key not in claim_status_df.columns:
        claim_status_key_candidates = {
            "claim_status": ["Status", "status", "claimStatus"],
            "policy_type": ["PolicyType", "policyType"],
        }
        alias_candidates = claim_status_key_candidates.get(claim_status_join_key, [])
        alias_col = next((name for name in alias_candidates if name in claim_status_df.columns), None)
        if alias_col:
            claim_status_df = claim_status_df.withColumnRenamed(alias_col, claim_status_join_key)

    if claim_handler_join_key not in df_base.columns:
        raise ValueError(f"Join key '{claim_handler_join_key}' not found in Claims dataframe columns: {df_base.columns}")
    if claim_handler_join_key not in claim_handler_df.columns:
        raise ValueError(f"Join key '{claim_handler_join_key}' not found in ClaimHandler columns: {claim_handler_df.columns}")
    if claim_status_join_key not in df_base.columns:
        raise ValueError(f"Join key '{claim_status_join_key}' not found in Claims dataframe columns: {df_base.columns}")
    if claim_status_join_key not in claim_status_df.columns:
        raise ValueError(f"Join key '{claim_status_join_key}' not found in ClaimStatus columns: {claim_status_df.columns}")

    return policy_rates_df, claim_handler_df, claim_status_df, claim_handler_join_key, claim_status_join_key


def run_join_ops(df_base, preview_rows_count: int, fast_run: bool, only_multiple_join: bool, run_demos: bool, run_pivot_ops: bool):
    print("\n=== JOINS ===\n")
    policy_rates_df, claim_handler_df, claim_status_df, claim_handler_join_key, claim_status_join_key = prepare_reference_tables(df_base)

    joined_df_local = df_base.join(policy_rates_df, on="policy_type", how="left") \
        .join(claim_handler_df, on=claim_handler_join_key, how="left") \
        .join(claim_status_df, on=claim_status_join_key, how="left")

    if not only_multiple_join:
        joined_df_local.show(preview_rows_count, truncate=False)

    if only_multiple_join:
        print("\n3. Multiple Joins - Claims + Rates + Handlers:")
        multi_join_df_local = df_base.join(policy_rates_df, on="policy_type", how="left") \
            .join(claim_handler_df, on=claim_handler_join_key, how="left")
        if not fast_run:
            print(f"   Rows after multiple joins: {multi_join_df_local.count()}")
        multi_join_df_local.select("policy_type", "claim_amount", "rate_factor", "handler_name").show(preview_rows_count)

    elif run_demos:
        print("\n1. INNER JOIN - Claims with Policy Rates:")
        inner_join_df = df_base.join(policy_rates_df, on="policy_type", how="inner")
        if not fast_run:
            print(f"   Rows after inner join: {inner_join_df.count()}")
        inner_join_df.select("policy_type", "claim_amount", "rate_factor").show(preview_rows_count)

        print("\n2. LEFT JOIN - All claims, matching rates if available:")
        left_join_df = df_base.join(policy_rates_df, on="policy_type", how="left")
        if not fast_run:
            print(f"   Rows after left join: {left_join_df.count()}")
        left_join_df.select("policy_type", "claim_amount", "rate_factor").show(preview_rows_count)

        print("\n3. Multiple Joins - Claims + Rates + Handlers:")
        multi_join_df = df_base.join(policy_rates_df, on="policy_type", how="left") \
            .join(claim_handler_df, on=claim_handler_join_key, how="left")
        if not fast_run:
            print(f"   Rows after multiple joins: {multi_join_df.count()}")
        multi_join_df.select("policy_type", "claim_amount", "rate_factor", "handler_name").show(preview_rows_count)

        print("\n4. Self Join - Find matching claims by policy type and amount:")
        df_limit = df_base.limit(50)
        df_alias1 = df_limit.alias("df1")
        df_alias2 = df_limit.alias("df2")
        self_join_df = df_alias1.join(
            df_alias2,
            (col("df1.policy_type") == col("df2.policy_type")) &
            (col("df1.claim_amount") == col("df2.claim_amount"))
        )
        if not fast_run:
            print(f"   Self-join matches found: {self_join_df.count()}")
        self_join_df.select(col("df1.policy_type"), col("df1.claim_amount").alias("amount1")).show(preview_rows_count)

        print("\n5. Join Claims with Aggregated Summary:")
        agg_summary = df_base.groupBy("policy_type") \
            .agg(sum("net_claim").alias("total_net_claim"), avg_func("claim_amount").alias("avg_claim"))
        summary_join = df_base.join(agg_summary, on="policy_type", how="left")
        if not fast_run:
            print(f"   Summary join complete: {summary_join.count()} rows")
        summary_join.select("policy_type", "claim_amount", "total_net_claim", "avg_claim").show(preview_rows_count)

    pivot_df_local = None
    if run_pivot_ops:
        print("\n=== PIVOT OPERATIONS ===\n")
        print("1. Pivot with Status - Categories as Columns:")
        pivot_df_local = claim_status_df.groupBy("policy_type").pivot(claim_status_join_key).agg(sum("amount")).fillna(0)
        pivot_df_local.show(preview_rows_count)
        print("\n=== PIVOT OPERATIONS COMPLETE ===")

    print("\n=== UDF AND JOINS COMPLETE ===")
    return joined_df_local, pivot_df_local


def export_outputs(joined_df_local, pivot_df_local):
    export_outputs_flag = env_flag("EXPORT_OUTPUTS", "0")
    if not export_outputs_flag:
        return

    output_root = Path(os.getenv("OUTPUT_DIR", str(workspace_dir / "output")))
    output_root.mkdir(parents=True, exist_ok=True)
    joined_path = str(output_root / "joined")
    joined_df_local.coalesce(1).write.mode("overwrite").parquet(joined_path)
    print(f"Exported joined output to: {joined_path}")

    if pivot_df_local is not None:
        pivot_path = str(output_root / "pivot")
        pivot_df_local.coalesce(1).write.mode("overwrite").parquet(pivot_path)
        print(f"Exported pivot output to: {pivot_path}")

# 4️ Read Table + Pipeline Execution

df = load_sql_table("dbo.Claims")
df = df.withColumn("net_claim", col("claim_amount") - col("recovery_amount")).cache()

if run_basic:
    run_basic_ops(df, preview_rows)

if run_window:
    run_window_ops(df, preview_rows)

if run_udf:
    run_udf_ops(df, preview_rows)

if run_quality:
    run_quality_checks(df, preview_rows)

if run_profile:
    run_data_profiling(df, preview_rows, profile_top_n)

if run_joins:
    joined_df, pivot_df = run_join_ops(
        df_base=df,
        preview_rows_count=preview_rows,
        fast_run=fast_mode,
        only_multiple_join=run_multiple_join_only,
        run_demos=run_join_demos,
        run_pivot_ops=run_pivot,
    )
    export_outputs(joined_df, pivot_df)