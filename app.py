import os
from pathlib import Path
import pyodbc
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,row_number,rank,dense_rank,lag,lead,avg as avg_func,udf,lit
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType


# ---------------------------------------------------
# 1️⃣  Path Setup
# ---------------------------------------------------
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

# ---------------------------------------------------
# 2️⃣  Spark Session
# ---------------------------------------------------
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
run_joins = env_flag("RUN_JOINS", "1")
run_join_demos = env_flag("RUN_JOIN_DEMOS", "1")
run_multiple_join_only = env_flag("RUN_MULTIPLE_JOIN_ONLY", "0")
run_pivot = env_flag("RUN_PIVOT", "1")
run_basic = env_flag("RUN_BASIC", "1")
fast_mode = env_flag("FAST_MODE", "1")
preview_rows = env_int("PREVIEW_ROWS", "5")
run_all_sequence = env_flag("RUN_ALL_SEQUENCE", "1")

if run_all_sequence:
    run_basic = True
    run_window = True
    run_udf = True
    run_joins = True
    run_join_demos = True
    run_pivot = True
    run_multiple_join_only = False

if run_multiple_join_only:
    run_basic = False
    run_window = False
    run_udf = False
    run_join_demos = False
    run_pivot = False

# ---------------------------------------------------
# 3️⃣  JDBC URL (Windows Auth)
# ---------------------------------------------------
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

# ---------------------------------------------------
# 4️⃣  Read Table
# ---------------------------------------------------
if sql_read_mode == "jdbc":
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table="dbo.Claims",
            properties=connection_properties
        )
    except Exception as exc:
        if "integrated authentication" in str(exc).lower():
            print("JDBC integrated auth failed; using ODBC Windows-auth fallback.")
            df = load_with_odbc("dbo.Claims")
        else:
            raise
else:
    df = load_with_odbc("dbo.Claims")

df = df.withColumn(
    "net_claim",
    col("claim_amount") - col("recovery_amount")
)

# Create a temp view from the detailed dataframe (includes `net_claim`)
df.createOrReplaceTempView("claims")

if run_basic:
    agg_df = df.groupBy("policy_type") \
        .agg(sum("net_claim").alias("total_net_claim"))

    agg_df.show(preview_rows)
    spark.sql("""
    SELECT policy_type,
         SUM(net_claim) AS total_net_claim,
         COUNT(*) AS total_policies
    FROM claims
    GROUP BY policy_type
    """).show(preview_rows)
    df.show(preview_rows)

    rdd = df.rdd
    total_premium = rdd.map(lambda row: row.premium) \
               .reduce(lambda a, b: a + b)
    print("Total Premium:", total_premium)

if run_window:
    print("\n=== WINDOW OPERATIONS ===\n")

    windowByPolicyType = Window.partitionBy("policy_type")
    windowByPolicyTypeOrderedByAmount = Window.partitionBy("policy_type").orderBy(col("claim_amount").desc())
    windowOrderedByAmount = Window.orderBy(col("claim_amount").desc())

    print("1. Row Number within each policy type:")
    df_with_row_num = df.withColumn("row_num", row_number().over(windowByPolicyTypeOrderedByAmount))
    df_with_row_num.select("policy_type", "claim_amount", "row_num").show(preview_rows)

    print("\n2. Rank by claim amount within policy type:")
    df_with_rank = df.withColumn("rank", rank().over(windowByPolicyTypeOrderedByAmount))
    df_with_rank.select("policy_type", "claim_amount", "rank").show(preview_rows)

    print("\n3. Dense Rank by claim amount within policy type:")
    df_with_dense_rank = df.withColumn("dense_rank", dense_rank().over(windowByPolicyTypeOrderedByAmount))
    df_with_dense_rank.select("policy_type", "claim_amount", "dense_rank").show(preview_rows)

    print("\n4. Lag - Previous row's claim amount:")
    df_with_lag = df.withColumn("previous_claim", lag("claim_amount", 1).over(windowByPolicyTypeOrderedByAmount))
    df_with_lag.select("policy_type", "claim_amount", "previous_claim").show(preview_rows)

    print("\n5. Lead - Next row's claim amount:")
    df_with_lead = df.withColumn("next_claim", lead("claim_amount", 1).over(windowByPolicyTypeOrderedByAmount))
    df_with_lead.select("policy_type", "claim_amount", "next_claim").show(preview_rows)

    print("\n6. Running Sum of claims within policy type:")
    windowRunningSumSpec = Window.partitionBy("policy_type").orderBy(col("policy_type"))
    df_with_running_sum = df.withColumn("running_sum", sum("claim_amount").over(windowRunningSumSpec))
    df_with_running_sum.select("policy_type", "claim_amount", "running_sum").show(preview_rows)

    print("\n7. Average claim amount within policy type:")
    df_with_avg = df.withColumn("avg_claim", avg_func("claim_amount").over(windowByPolicyType))
    df_with_avg.select("policy_type", "claim_amount", "avg_claim").show(preview_rows)

    print("\n8. Difference from policy type average:")
    df_with_diff = df.withColumn("avg_claim", avg_func("claim_amount").over(windowByPolicyType)) \
                    .withColumn("diff_from_avg", col("claim_amount") - col("avg_claim"))
    df_with_diff.select("policy_type", "claim_amount", "avg_claim", "diff_from_avg").show(preview_rows)

    print("\n9. Combined window functions:")
    combined_df = df.withColumn("row_num", row_number().over(windowByPolicyTypeOrderedByAmount)) \
                    .withColumn("rank", rank().over(windowByPolicyTypeOrderedByAmount)) \
                    .withColumn("previous_claim", lag("claim_amount").over(windowByPolicyTypeOrderedByAmount))
    combined_df.select("policy_type", "claim_amount", "row_num", "rank", "previous_claim").show(preview_rows)

    print("\n=== WINDOW OPERATIONS COMPLETE ===")


if run_udf:
    print("\n=== USER DEFINED FUNCTIONS (UDF) ===\n")

    def categorize_claim(amount):
        if amount is None:
            return "Unknown"
        elif amount < 500:
            return "Low"
        elif amount < 2000:
            return "Medium"
        else:
            return "High"

    categorize_claim_udf = udf(categorize_claim, StringType())

    print("1. Categorize Claims by Amount:")
    df_categorized = df.withColumn("claim_category", categorize_claim_udf(col("claim_amount")))
    df_categorized.select("claim_amount", "claim_category").show(preview_rows)

    def calculate_risk_score(claim_amount, recovery_amount):
        if claim_amount is None or recovery_amount is None:
            return 0.0
        net = claim_amount - recovery_amount
        if net <= 0:
            return 0.0
        elif net < 500:
            return 1.0
        elif net < 1500:
            return 2.5
        else:
            return 5.0

    calculate_risk_udf = udf(calculate_risk_score, DoubleType())

    print("\n2. Calculate Risk Score:")
    df_with_risk = df.withColumn("risk_score", calculate_risk_udf(col("claim_amount"), col("recovery_amount")))
    df_with_risk.select("claim_amount", "recovery_amount", "risk_score").show(preview_rows)

    def get_policy_status(net_claim):
        if net_claim is None:
            return "Pending"
        elif net_claim > 3000:
            return "High Risk"
        elif net_claim > 1000:
            return "Medium Risk"
        else:
            return "Low Risk"

    policy_status_udf = udf(get_policy_status, StringType())

    print("\n3. Policy Status based on Net Claim:")
    df_status = df.withColumn("net_claim", col("claim_amount") - col("recovery_amount")) \
                .withColumn("policy_status", policy_status_udf(col("net_claim")))
    df_status.select("claim_amount", "recovery_amount", "net_claim", "policy_status").show(preview_rows)


if run_joins:
    print("\n=== JOINS ===\n")

    policy_rates_table = os.getenv("POLICY_RATES_TABLE", "dbo.PolicyRates")
    claim_handler_table = os.getenv("CLAIM_HANDLER_TABLE", "dbo.ClaimHandlers")
    claim_handler_join_key = os.getenv("CLAIM_HANDLER_JOIN_KEY", "policy_type")
    claim_status_table = os.getenv("CLAIM_STATUS_TABLE", "dbo.ClaimStatus")
    claim_status_join_key = os.getenv("CLAIM_STATUS_JOIN_KEY", "claim_status")

    if sql_read_mode == "jdbc":
        try:
            policy_rates_df = spark.read.jdbc(
                url=jdbc_url,
                table=policy_rates_table,
                properties=connection_properties
            )
        except Exception as exc:
            if "integrated authentication" in str(exc).lower():
                policy_rates_df = load_with_odbc(policy_rates_table)
            else:
                raise
    else:
        policy_rates_df = load_with_odbc(policy_rates_table)

    if sql_read_mode == "jdbc":
        try:
            claim_handler_df = spark.read.jdbc(
                url=jdbc_url,
                table=claim_handler_table,
                properties=connection_properties
            )
        except Exception as exc:
            if "integrated authentication" in str(exc).lower():
                claim_handler_df = load_with_odbc(claim_handler_table)
            else:
                raise
    else:
        claim_handler_df = load_with_odbc(claim_handler_table)

    if sql_read_mode == "jdbc":
        try:
            claim_status_df = spark.read.jdbc(
                url=jdbc_url,
                table=claim_status_table,
                properties=connection_properties
            )
        except Exception as exc:
            if "integrated authentication" in str(exc).lower():
                claim_status_df = load_with_odbc(claim_status_table)
            else:
                raise
    else:
        claim_status_df = load_with_odbc(claim_status_table)

# Normalize PolicyRates rate column to a common name used below
    rate_col_candidates = ["rate_factor", "Rate", "rate", "rateFactor"]
    rate_col = next((name for name in rate_col_candidates if name in policy_rates_df.columns), None)
    if rate_col is None:
        raise ValueError(
            f"Rate column not found in PolicyRates columns: {policy_rates_df.columns}"
        )
    if rate_col != "rate_factor":
        policy_rates_df = policy_rates_df.withColumnRenamed(rate_col, "rate_factor")

# Normalize ClaimHandler name column (if present) to handler_name
    handler_name_candidates = ["handler_name", "HandlerName", "handler", "name"]
    handler_name_col = next((name for name in handler_name_candidates if name in claim_handler_df.columns), None)
    if handler_name_col and handler_name_col != "handler_name":
        claim_handler_df = claim_handler_df.withColumnRenamed(handler_name_col, "handler_name")
    elif not handler_name_col:
        claim_handler_df = claim_handler_df.withColumn("handler_name", lit(None).cast(StringType()))

# Normalize ClaimStatus description column (if present) to status_description
    status_desc_candidates = ["status_description", "StatusDescription", "description", "status_desc"]
    status_desc_col = next((name for name in status_desc_candidates if name in claim_status_df.columns), None)
    if status_desc_col and status_desc_col != "status_description":
        claim_status_df = claim_status_df.withColumnRenamed(status_desc_col, "status_description")
    elif not status_desc_col:
        claim_status_df = claim_status_df.withColumn("status_description", lit(None).cast(StringType()))

# Normalize ClaimStatus amount column for pivot aggregation
    amount_col_candidates = ["amount", "Amount", "claim_amount", "ClaimAmount"]
    amount_col = next((name for name in amount_col_candidates if name in claim_status_df.columns), None)
    if amount_col and amount_col != "amount":
        claim_status_df = claim_status_df.withColumnRenamed(amount_col, "amount")

# Normalize ClaimStatus join key to expected key when aliases are used (e.g., Status -> claim_status)
    if claim_status_join_key not in claim_status_df.columns:
        claim_status_key_candidates = {
            "claim_status": ["Status", "status", "claimStatus"],
            "policy_type": ["PolicyType", "policyType"],
        }
        alias_candidates = claim_status_key_candidates.get(claim_status_join_key, [])
        alias_col = next((name for name in alias_candidates if name in claim_status_df.columns), None)
        if alias_col:
            claim_status_df = claim_status_df.withColumnRenamed(alias_col, claim_status_join_key)

    if claim_handler_join_key not in df.columns:
        raise ValueError(
            f"Join key '{claim_handler_join_key}' not found in Claims dataframe columns: {df.columns}"
        )

    if claim_handler_join_key not in claim_handler_df.columns:
        raise ValueError(
            f"Join key '{claim_handler_join_key}' not found in ClaimHandler columns: {claim_handler_df.columns}"
        )

    if claim_status_join_key not in df.columns:
        raise ValueError(
            f"Join key '{claim_status_join_key}' not found in Claims dataframe columns: {df.columns}"
        )

    if claim_status_join_key not in claim_status_df.columns:
        raise ValueError(
            f"Join key '{claim_status_join_key}' not found in ClaimStatus columns: {claim_status_df.columns}"
        )

    joined_df = df.join(policy_rates_df, on="policy_type", how="left")
    joined_df = joined_df.join(claim_handler_df, on=claim_handler_join_key, how="left")
    joined_df = joined_df.join(claim_status_df, on=claim_status_join_key, how="left")
    if not run_multiple_join_only:
        joined_df.show(preview_rows, truncate=False)

    if run_multiple_join_only:
        print("\n3. Multiple Joins - Claims + Rates + Handlers:")
        multi_join_df = df.join(policy_rates_df, on="policy_type", how="left") \
                        .join(claim_handler_df, on=claim_handler_join_key, how="left")

        if not fast_mode:
            print(f"   Rows after multiple joins: {multi_join_df.count()}")
        multi_join_df.select("policy_type", "claim_amount", "rate_factor", "handler_name").show(preview_rows)

    elif run_join_demos:
        print("\n1. INNER JOIN - Claims with Policy Rates:")
        inner_join_df = df.join(policy_rates_df, on="policy_type", how="inner")
        if not fast_mode:
            print(f"   Rows after inner join: {inner_join_df.count()}")
        inner_join_df.select("policy_type", "claim_amount", "rate_factor").show(preview_rows)

        print("\n2. LEFT JOIN - All claims, matching rates if available:")
        left_join_df = df.join(policy_rates_df, on="policy_type", how="left")
        if not fast_mode:
            print(f"   Rows after left join: {left_join_df.count()}")
        left_join_df.select("policy_type", "claim_amount", "rate_factor").show(preview_rows)

        print("\n3. Multiple Joins - Claims + Rates + Handlers:")
        multi_join_df = df.join(policy_rates_df, on="policy_type", how="left") \
                        .join(claim_handler_df, on=claim_handler_join_key, how="left") \

        if not fast_mode:
            print(f"   Rows after multiple joins: {multi_join_df.count()}")
        multi_join_df.select("policy_type", "claim_amount", "rate_factor", "handler_name").show(preview_rows)

        print("\n4. Self Join - Find matching claims by policy type and amount:")
        df_limit = df.limit(50)
        df_alias1 = df_limit.alias("df1")
        df_alias2 = df_limit.alias("df2")
        self_join_df = df_alias1.join(
            df_alias2,
            (col("df1.policy_type") == col("df2.policy_type")) &
            (col("df1.claim_amount") == col("df2.claim_amount"))
        )
        if not fast_mode:
            print(f"   Self-join matches found: {self_join_df.count()}")
        self_join_df.select(col("df1.policy_type"), col("df1.claim_amount").alias("amount1")).show(preview_rows)

        print("\n5. Join Claims with Aggregated Summary:")
        agg_summary = df.groupBy("policy_type") \
                        .agg(sum("net_claim").alias("total_net_claim"),
                            avg_func("claim_amount").alias("avg_claim"))
        summary_join = df.join(agg_summary, on="policy_type", how="left")
        if not fast_mode:
            print(f"   Summary join complete: {summary_join.count()} rows")
        summary_join.select("policy_type", "claim_amount", "total_net_claim", "avg_claim").show(preview_rows)
    print("\n=== UDF AND JOINS COMPLETE ===")


    if run_pivot:
        print("\n=== PIVOT OPERATIONS ===\n")
        print("1. Pivot with Status - Categories as Columns:")
        pivot_status = claim_status_df.groupBy("policy_type").pivot(claim_status_join_key).agg(sum("amount")).fillna(0)
        pivot_status.show(preview_rows)
        print("\n=== PIVOT OPERATIONS COMPLETE ===")


#df.show(20, truncate=False)

# ---------------------------------------------------
# 5️⃣  Stop Spark
# ---------------------------------------------------
spark.stop()