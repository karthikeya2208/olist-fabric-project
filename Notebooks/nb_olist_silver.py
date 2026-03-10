# ==============================================================
# SILVER LAYER PROCESSING SCRIPT – MICROSOFT FABRIC LAKEHOUSE
# Cleans and standardizes raw Olist datasets and stores them in Delta format
# ==============================================================

from pyspark.sql import SparkSession, functions as F, types as T
import unicodedata

# ---------------------------------------------------------------
# Helper Function: Remove Accents from Text (e.g., "São Paulo" → "Sao Paulo")
# ---------------------------------------------------------------
def remove_accents_udf():
    return F.udf(
        lambda s: unicodedata.normalize('NFKD', s).encode('ascii', 'ignore').decode('utf-8') if s else s,
        T.StringType()
    )

# ---------------------------------------------------------------
# Define Lakehouse Paths
# ---------------------------------------------------------------

bronze_path = "abfss://ECommerce_Analytics@onelake.dfs.fabric.microsoft.com/lh_olist_bronze.Lakehouse/Files/bronze"
silver_path = "abfss://ECommerce_Analytics@onelake.dfs.fabric.microsoft.com/lh_olist_silver.Lakehouse/Tables/silver"

# ---------------------------------------------------------------
# Load Bronze Data (CSV files)
# ---------------------------------------------------------------

geo_df      = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_geolocation_dataset.csv")
cust_df     = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_customers_dataset.csv")
order_df    = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_orders_dataset.csv")
item_df     = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_order_items_dataset.csv")
pay_df      = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_order_payments_dataset.csv")
review_df   = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_order_reviews_dataset.csv")
prod_df     = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_products_dataset.csv")
seller_df   = spark.read.format("csv").option("header", True).load(f"{bronze_path}/olist_sellers_dataset.csv")
trans_df    = spark.read.format("csv").option("header", True).load(f"{bronze_path}/product_category_name_translation.csv")

# ---------------------------------------------------------------
# Geolocation Data Cleaning
# ---------------------------------------------------------------
geo_df_clean = (
    geo_df
    .withColumn("geolocation_city", F.lower(F.col("geolocation_city")))
    .withColumn("geolocation_city", F.trim(F.col("geolocation_city")))
    .withColumn("geolocation_city", remove_accents_udf()(F.col("geolocation_city")))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Customer Data Cleaning
# ---------------------------------------------------------------
cust_df_clean = (
    cust_df
    .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
    .withColumn("customer_city", remove_accents_udf()(F.col("customer_city")))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Seller Data Cleaning
# ---------------------------------------------------------------
seller_df_clean = (
    seller_df
    .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    .withColumn("seller_city", remove_accents_udf()(F.col("seller_city")))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Orders Data Cleaning
# ---------------------------------------------------------------
order_df_clean = (
    order_df
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_date("order_estimated_delivery_date"))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Order Items Data Cleaning
# ---------------------------------------------------------------
item_df_clean = (
    item_df
    .withColumn("price", F.col("price").cast(T.DoubleType()))
    .withColumn("freight_value", F.col("freight_value").cast(T.DoubleType()))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Payments Data Cleaning
# ---------------------------------------------------------------
pay_df_clean = (
    pay_df
    .withColumn("payment_value", F.col("payment_value").cast(T.DoubleType()))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Reviews Data Cleaning
# ---------------------------------------------------------------
review_df_clean = (
    review_df
    .withColumn("review_score", F.col("review_score").cast(T.IntegerType()))
    .dropDuplicates()
)

# ---------------------------------------------------------------
# Products Data Cleaning
# ---------------------------------------------------------------
prod_df_clean = prod_df.dropDuplicates()

# ---------------------------------------------------------------
# Product Category Translation Cleaning
# ---------------------------------------------------------------
trans_df_clean = (
    trans_df
    .withColumn("product_category_name", F.trim(F.lower(F.col("product_category_name"))))
    .withColumn("product_category_name_english", F.trim(F.lower(F.col("product_category_name_english"))))
)

# ---------------------------------------------------------------
# Write Cleaned Tables to Silver Layer
# ---------------------------------------------------------------

geo_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_geolocation")
cust_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_customers")
seller_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_sellers")
order_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_orders")
item_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_order_items")
pay_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_order_payments")
review_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_order_reviews")
prod_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_products")
trans_df_clean.write.mode("overwrite").format("delta").save(f"{silver_path}/olist_product_category_name_translation")

print("All Silver Layer tables successfully created and stored in Delta format.")