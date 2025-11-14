from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, when, col, round as spark_round, row_number, to_date
from pyspark.sql.window import Window
import psycopg2
import traceback
from datetime import datetime

# Configuration PostgreSQL
pg_host = "localhost"
pg_port = "5432"
pg_db = "BIG_DATA_DB"
pg_user = "postgres"
pg_password = "widad2003"

# URL JDBC
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
jdbc_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver"
}

# ✅ Création des tables dans PostgreSQL
def create_billing_tables():
    try:
        conn = psycopg2.connect(
            host=pg_host, port=pg_port, dbname=pg_db,
            user=pg_user, password=pg_password
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_summary (
                invoice_id TEXT,
                user_id TEXT,
                billing_period TEXT,
                invoice_date DATE,
                total_voice_cost NUMERIC(10,2),
                total_sms_cost NUMERIC(10,2),
                total_data_cost NUMERIC(10,2),
                subtotal NUMERIC(10,2),
                tax_amount NUMERIC(10,2),
                total_amount NUMERIC(10,2),
                PRIMARY KEY (invoice_id, user_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS invoice_lines (
                invoice_id TEXT,
                user_id TEXT,
                line_number INTEGER,
                service_type TEXT,
                usage_quantity NUMERIC(10,2),
                unit TEXT,
                unit_price NUMERIC(10,4),
                line_amount NUMERIC(10,2),
                PRIMARY KEY (invoice_id, line_number)
            );
        """)

        conn.commit()
        cur.close()
        conn.close()
        print(" Tables de facturation créées avec succès.")
    except Exception as e:
        print(" Erreur création tables :", e)
        traceback.print_exc()

# ✅ Chargement des enregistrements notés
def load_rated_records():
    try:
        spark = SparkSession.builder \
            .appName("TelecomBillingEngine") \
            .config("spark.jars", "file:///C:/Users/dell/Desktop/s4/BIG_DATA/postgresql-42.6.0.jar") \
            .getOrCreate()

        df_rated = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "rated_usage_records") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        df_rated = df_rated.filter(col("status") == "rated") \
            .withColumn(
                "user_id",
                when(col("record_type") == "voice", col("caller_id"))
                .when(col("record_type") == "sms", col("sender_id"))
                .otherwise(col("user_id"))
            )
        return df_rated
    except Exception as e:
        print(" Erreur chargement des données notées :", e)
        traceback.print_exc()
        return None

# ✅ Génération du résumé de facturation
def generate_billing_summary(df_rated, billing_period):
    try:
        df_summary = df_rated.groupBy("user_id", "record_type") \
            .agg(sum("cost").alias("service_cost"))

        df_pivoted = df_summary.groupBy("user_id") \
            .pivot("record_type", ["voice", "sms", "data"]) \
            .sum("service_cost") \
            .fillna(0)

        df_pivoted = df_pivoted.withColumnRenamed("voice", "total_voice_cost") \
                               .withColumnRenamed("sms", "total_sms_cost") \
                               .withColumnRenamed("data", "total_data_cost")

        df_totals = df_pivoted \
            .withColumn("subtotal", col("total_voice_cost") + col("total_sms_cost") + col("total_data_cost")) \
            .withColumn("tax_amount", col("subtotal") * 0.2) \
            .withColumn("total_amount", col("subtotal") + col("tax_amount")) \
            .withColumn("billing_period", lit(billing_period)) \
            .withColumn("invoice_date", to_date(lit(datetime.now().strftime("%Y-%m-%d")))) \
            .withColumn("invoice_id", lit(billing_period.replace("-", "")) + col("user_id"))

        return df_totals
    except Exception as e:
        print(" Erreur génération résumé de facturation :", e)
        traceback.print_exc()
        return None

# ✅ Génération des lignes de facture
def generate_invoice_lines(df_rated, billing_period):
    try:
        df_rated = df_rated.withColumn(
            "user_id",
            when(col("record_type") == "voice", col("caller_id"))
            .when(col("record_type") == "sms", col("sender_id"))
            .otherwise(col("user_id"))
        )

        df_lines = df_rated.withColumn(
            "service_type",
            when(col("record_type") == "voice", "Voice Calls")
            .when(col("record_type") == "sms", "SMS")
            .otherwise("Mobile Data")
        ).withColumn(
            "unit",
            when(col("record_type") == "voice", "minutes")
            .when(col("record_type") == "sms", "messages")
            .otherwise("MB")
        ).withColumn(
            "usage_quantity",
            when(col("record_type") == "voice", spark_round(col("duration_sec") / 60, 2))
            .when(col("record_type") == "sms", lit(1))
            .otherwise(col("data_volume_mb"))
        ).withColumn(
            "unit_price",
            when(col("record_type") == "voice", col("cost") / spark_round(col("duration_sec") / 60, 2))
            .when(col("record_type") == "sms", col("cost"))
            .otherwise(col("cost") / col("data_volume_mb"))
        ).withColumn(
            "line_amount", col("cost")
        ).withColumn(
            "invoice_id", lit(billing_period.replace("-", "")) + col("user_id")
        )

        window_spec = Window.partitionBy("invoice_id").orderBy("service_type")
        df_lines = df_lines.withColumn("line_number", row_number().over(window_spec))

        return df_lines.select(
            "invoice_id", "user_id", "line_number", "service_type",
            "usage_quantity", "unit", "unit_price", "line_amount"
        )
    except Exception as e:
        print(" Erreur génération lignes de facture :", e)
        traceback.print_exc()
        return None

# ✅ Enregistrement dans PostgreSQL
def save_billing_data(df_summary, df_lines):
    try:
        df_summary.select(
            "invoice_id", "user_id", "billing_period", "invoice_date",
            "total_voice_cost", "total_sms_cost", "total_data_cost",
            "subtotal", "tax_amount", "total_amount"
        ).write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "billing_summary") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        df_lines.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "invoice_lines") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(" Données de facturation enregistrées avec succès.")
    except Exception as e:
        print(" Erreur lors de l'enregistrement :", e)
        traceback.print_exc()

def export_invoice_lines_to_csv():
    spark = SparkSession.builder \
        .appName("ExportCSV") \
        .config("spark.jars", "file:///C:/Users/dell/Desktop/s4/BIG_DATA/postgresql-42.6.0.jar") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "invoice_lines") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    df.coalesce(1).write \
        .option("header", True) \
        .mode("overwrite") \
        .csv("file:///C:/Users/dell/Desktop/projetBigData/invoice_csv")

# ✅ Fonction principale
def main():
    create_billing_tables()
    df_rated = load_rated_records()
    if df_rated is None:
        return

    billing_period = "2025-04"
    df_summary = generate_billing_summary(df_rated, billing_period)
    df_lines = generate_invoice_lines(df_rated, billing_period)

    if df_summary is not None and df_lines is not None:
        save_billing_data(df_summary, df_lines)
    
    export_invoice_lines_to_csv()

# ✅ Lancement du script
if __name__ == "__main__":
    main()