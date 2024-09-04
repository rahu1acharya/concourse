#spark-submit tjob.py
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, avg, row_number, coalesce, when, lit, round, lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
import time
import psycopg2
import json
from kafka import KafkaConsumer

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("ERROR")

# Kafka parameters
kafka_bootstrap_servers = "broker2:29092"
kafka_topic = "tcpl.public.ten_comp_pl"

# PostgreSQL connection parameters
postgres_url = "192.168.56.1"
postgres_db = "concourse"
postgres_user = "concourse_user"
postgres_password = "concourse_pass"
postgres_table = "sink_tcpl_transformed"

# Kafka consumer setup
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest',
    group_id='p333'
)

# Schema for Kafka messages
schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("date", StringType(), True),
            StructField("sales", DoubleType(), True),
            StructField("expenses", DoubleType(), True),
            StructField("operating_profit", DoubleType(), True),
            StructField("opm_percent", DoubleType(), True),
            StructField("other_income", DoubleType(), True),
            StructField("interest", DoubleType(), True),
            StructField("depreciation", DoubleType(), True),
            StructField("profit_before_tax", DoubleType(), True),
            StructField("tax_percent", DoubleType(), True),
            StructField("net_profit", DoubleType(), True),
            StructField("eps_in_rs", DoubleType(), True),
            StructField("dividend_payout_percent", DoubleType(), True),
            StructField("company_name", StringType(), True)
        ])),
        StructField("after", StructType([
            StructField("date", StringType(), True),
            StructField("sales", DoubleType(), True),
            StructField("expenses", DoubleType(), True),
            StructField("operating_profit", DoubleType(), True),
            StructField("opm_percent", DoubleType(), True),
            StructField("other_income", DoubleType(), True),
            StructField("interest", DoubleType(), True),
            StructField("depreciation", DoubleType(), True),
            StructField("profit_before_tax", DoubleType(), True),
            StructField("tax_percent", DoubleType(), True),
            StructField("net_profit", DoubleType(), True),
            StructField("eps_in_rs", DoubleType(), True),
            StructField("dividend_payout_percent", DoubleType(), True),
            StructField("company_name", StringType(), True)
        ])),
        StructField("source", StructType([
            StructField("ts_ms", LongType(), True)
        ]))
    ]))
])

# Initialize PostgreSQL connection
def get_postgres_connection():
    return psycopg2.connect(
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
        host=postgres_url
    )

# Create table if it does not exist
def create_table_if_not_exists():
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {postgres_table} (
        date VARCHAR,
        company_name VARCHAR,
        sales DOUBLE PRECISION,
        expenses DOUBLE PRECISION,
        operating_profit DOUBLE PRECISION,
        opm_percent DOUBLE PRECISION,
        other_income DOUBLE PRECISION,
        interest DOUBLE PRECISION,
        depreciation DOUBLE PRECISION,
        profit_before_tax DOUBLE PRECISION,
        tax_percent DOUBLE PRECISION,
        net_profit DOUBLE PRECISION,
        eps_in_rs DOUBLE PRECISION,
        dividend_payout_percent DOUBLE PRECISION,
        net_profit_margin DOUBLE PRECISION,
        operating_profit_margin DOUBLE PRECISION,
        tax_amount DOUBLE PRECISION,
        dividend_yield DOUBLE PRECISION,
        interest_coverage_ratio DOUBLE PRECISION,
        yoy_sales_growth DOUBLE PRECISION,
        yoy_net_profit_growth DOUBLE PRECISION,
        PRIMARY KEY (date, company_name)
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    cur.close()
    conn.close()

# Call the function to ensure table exists
create_table_if_not_exists()

while True:
    try:
        # Read Kafka data and parse it
        df = spark.read.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)") \
            .selectExpr(f"from_json(value, '{schema.simpleString()}') as data") \
            .select(
                col("data.payload.after.date").alias("date"),
                col("data.payload.after.sales"),
                col("data.payload.after.expenses"),
                col("data.payload.after.operating_profit"),
                col("data.payload.after.opm_percent"),
                col("data.payload.after.other_income"),
                col("data.payload.after.interest"),
                col("data.payload.after.depreciation"),
                col("data.payload.after.profit_before_tax"),
                col("data.payload.after.tax_percent"),
                col("data.payload.after.net_profit"),
                col("data.payload.after.eps_in_rs"),
                col("data.payload.after.dividend_payout_percent"),
                col("data.payload.source.ts_ms"),
                col("data.payload.before.date").alias("before_date"),
                col("data.payload.after.company_name")
            )

        # Deduplicate by keeping the latest record for each date and company
        window_spec = Window.partitionBy("date", "company_name").orderBy(col("ts_ms").desc())
        df_deduped = df.filter(col("date").isNotNull()) \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")

        # Read existing data from PostgreSQL
        existing_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{postgres_url}/{postgres_db}") \
            .option("dbtable", postgres_table) \
            .option("user", postgres_user) \
            .option("password", postgres_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Extract deleted records
        deleted_df = df.filter(col("before_date").isNotNull() & col("date").isNull()) \
            .select(col("before_date").alias("date"), col("company_name")).distinct()

        # Define column order
        column_order = [
            "date", "sales", "expenses", "operating_profit", "opm_percent",
            "other_income", "interest", "depreciation", "profit_before_tax",
            "tax_percent", "net_profit", "eps_in_rs", "dividend_payout_percent",
            "net_profit_margin", "operating_profit_margin", "tax_amount", "dividend_yield",
            "interest_coverage_ratio", "yoy_sales_growth", "yoy_net_profit_growth", "company_name"
        ]

        # Combine existing and deduplicated data, then filter out deleted records
        combined_df = existing_df.alias("existing") \
            .join(df_deduped.alias("incoming"), on=["date", "company_name"], how="outer") \
            .select(
                coalesce(col("incoming.date"), col("existing.date")).alias("date"),
                coalesce(col("incoming.sales"), col("existing.sales")).alias("sales"),
                coalesce(col("incoming.expenses"), col("existing.expenses")).alias("expenses"),
                coalesce(col("incoming.operating_profit"), col("existing.operating_profit")).alias("operating_profit"),
                coalesce(col("incoming.opm_percent"), col("existing.opm_percent")).alias("opm_percent"),
                coalesce(col("incoming.other_income"), col("existing.other_income")).alias("other_income"),
                coalesce(col("incoming.interest"), col("existing.interest")).alias("interest"),
                coalesce(col("incoming.depreciation"), col("existing.depreciation")).alias("depreciation"),
                coalesce(col("incoming.profit_before_tax"), col("existing.profit_before_tax")).alias("profit_before_tax"),
                coalesce(col("incoming.tax_percent"), col("existing.tax_percent")).alias("tax_percent"),
                coalesce(col("incoming.net_profit"), col("existing.net_profit")).alias("net_profit"),
                coalesce(col("incoming.eps_in_rs"), col("existing.eps_in_rs")).alias("eps_in_rs"),
                coalesce(col("incoming.dividend_payout_percent"), col("existing.dividend_payout_percent")).alias("dividend_payout_percent"),
                col("company_name")
            ) \
            .filter(~col("date").isin([row["date"] for row in deleted_df.collect()]))

        # Perform calculations
        # Explicitly cast columns to the correct types
        combined_df = combined_df \
            .withColumn("net_profit_margin", round(col("net_profit") / col("sales") * 100, 2).cast(DoubleType())) \
            .withColumn("operating_profit_margin", round(col("operating_profit") / col("sales") * 100, 2).cast(DoubleType())) \
            .withColumn("tax_amount", round(col("tax_percent") * col("profit_before_tax") / 100, 2).cast(DoubleType())) \
            .withColumn(
    "dividend_yield",
    round(col("dividend_payout_percent") * col("eps_in_rs") / 100, 2).cast(DoubleType())
) \
            .withColumn("interest_coverage_ratio", round(col("operating_profit") / col("interest"), 2).cast(DoubleType())) \
            .withColumn(
    "yoy_sales_growth",
    round(
        ((col("sales") - lag(col("sales"), 1).over(Window.partitionBy("company_name").orderBy("date"))) 
        / lag(col("sales"), 1).over(Window.partitionBy("company_name").orderBy("date"))
        ) * 100, 2
    ).cast(DoubleType())
)  \
            .withColumn(
    "yoy_net_profit_growth",
    round(
        ((col("net_profit") - lag(col("net_profit"), 1).over(Window.partitionBy("company_name").orderBy("date"))) 
        / lag(col("net_profit"), 1).over(Window.partitionBy("company_name").orderBy("date"))
        ) * 100, 2
    ).cast(DoubleType())
)

        # Sort the DataFrame
        combined_df = combined_df.orderBy(col("company_name").asc(), col("date").asc())
        combined_df = combined_df.filter(~col("date").startswith("TTM"))

        # Save to local CSV
        output_path = "/home/glue_user/workspace/data/sink_tcpl_transformed.csv"
        combined_df.write.mode("overwrite").csv(output_path, header=True)
        print(f"Data saved locally at {output_path}")

        # Convert DataFrame to Pandas and insert into PostgreSQL
        combined_df_pandas = combined_df.toPandas()

        print(combined_df_pandas)
        conn = get_postgres_connection()
        cur = conn.cursor()
        for _, row in combined_df_pandas.iterrows():
            query = """
                INSERT INTO {} (
                    date, company_name, sales, expenses, operating_profit, opm_percent, other_income,
                    interest, depreciation, profit_before_tax, tax_percent, net_profit,
                    eps_in_rs, dividend_payout_percent, net_profit_margin, operating_profit_margin, tax_amount, dividend_yield,
                    interest_coverage_ratio, yoy_sales_growth, yoy_net_profit_growth
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (date, company_name) DO UPDATE SET
                    sales = EXCLUDED.sales,
                    expenses = EXCLUDED.expenses,
                    operating_profit = EXCLUDED.operating_profit,
                    opm_percent = EXCLUDED.opm_percent,
                    other_income = EXCLUDED.other_income,
                    interest = EXCLUDED.interest,
                    depreciation = EXCLUDED.depreciation,
                    profit_before_tax = EXCLUDED.profit_before_tax,
                    tax_percent = EXCLUDED.tax_percent,
                    net_profit = EXCLUDED.net_profit,
                    eps_in_rs = EXCLUDED.eps_in_rs,
                    dividend_payout_percent = EXCLUDED.dividend_payout_percent,
                    net_profit_margin = EXCLUDED.net_profit_margin,
                    operating_profit_margin = EXCLUDED.operating_profit_margin,
                    tax_amount = EXCLUDED.tax_amount,
                    dividend_yield = EXCLUDED.dividend_yield,
                    interest_coverage_ratio = EXCLUDED.interest_coverage_ratio,
                    yoy_sales_growth = EXCLUDED.yoy_sales_growth,
                    yoy_net_profit_growth = EXCLUDED.yoy_net_profit_growth
            """.format(postgres_table)
            cur.execute(query, (
                row["date"], row["company_name"], row["sales"], row["expenses"], row["operating_profit"],
                row["opm_percent"], row["other_income"], row["interest"], row["depreciation"],
                row["profit_before_tax"], row["tax_percent"], row["net_profit"], row["eps_in_rs"],
                row["dividend_payout_percent"], row["net_profit_margin"], row["operating_profit_margin"], row["tax_amount"], row["dividend_yield"],
                row["interest_coverage_ratio"], row["yoy_sales_growth"], row["yoy_net_profit_growth"]
            ))

        conn.commit()
        cur.close()
        conn.close()
        print("Data written to PostgreSQL successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

    time.sleep(5)
