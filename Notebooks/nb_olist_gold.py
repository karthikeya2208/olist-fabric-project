# ==============================================================
# GOLD LAYER PROCESSING SCRIPT – MICROSOFT FABRIC LAKEHOUSE
# Builds analytical tables from Silver layer for BI consumption
# ==============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------
# Define Lakehouse Paths
# ---------------------------------------------------------------

silver_path = "abfss://ECommerce_Analytics@onelake.dfs.fabric.microsoft.com/lh_olist_silver.Lakehouse/Tables/silver"
gold_path = "abfss://ECommerce_Analytics@onelake.dfs.fabric.microsoft.com/lh_olist_gold.Lakehouse/Tables/gold"


# ---------------------------------------------------------------
# Load Silver Tables
# ---------------------------------------------------------------

orders_df = spark.read.format("delta").load(f"{silver_path}/olist_orders")
items_df = spark.read.format("delta").load(f"{silver_path}/olist_order_items")
payments_df = spark.read.format("delta").load(f"{silver_path}/olist_order_payments")
customers_df = spark.read.format("delta").load(f"{silver_path}/olist_customers")
sellers_df = spark.read.format("delta").load(f"{silver_path}/olist_sellers")
products_df = spark.read.format("delta").load(f"{silver_path}/olist_products")
geolocation_df = spark.read.format("delta").load(f"{silver_path}/olist_geolocation")
reviews_df = spark.read.format("delta").load(f"{silver_path}/olist_order_reviews")
trans_df = spark.read.format("delta").load(f"{silver_path}/olist_product_category_name_translation")


# ---------------------------------------------------------------
# Prepare Reviews (latest review per order)
# ---------------------------------------------------------------

if "review_creation_date" in reviews_df.columns:

    reviews_df = reviews_df.withColumn(
        "review_creation_ts",
        F.to_timestamp("review_creation_date")
    )

    reviews_latest = (
        reviews_df
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("order_id")
                .orderBy(F.col("review_creation_ts").desc())
            )
        )
        .filter(F.col("rn") == 1)
        .select("order_id", "review_score", "review_creation_date")
    )

else:
    reviews_latest = reviews_df.select("order_id", "review_score")


# ---------------------------------------------------------------
# Create Product Category Translation
# ---------------------------------------------------------------

products_en = (
    products_df
    .join(
        trans_df,
        on="product_category_name",
        how="left"
    )
)


# ---------------------------------------------------------------
# Create FACT TABLE – fact_orders
# ---------------------------------------------------------------

fact_orders = (
    orders_df
    .join(items_df, "order_id", "left")
    .join(payments_df, "order_id", "left")
    .join(products_en, "product_id", "left")
    .join(reviews_latest, "order_id", "left")
    .select(
        F.col("order_id").alias("Order_ID"),
        F.col("customer_id").alias("Customer_ID"),
        F.col("seller_id").alias("Seller_ID"),
        F.col("product_id").alias("Product_ID"),
        F.col("order_purchase_timestamp").alias("Order_Date"),
        F.col("price").alias("Unit_Price"),
        F.col("freight_value").alias("Freight_Value"),
        F.col("payment_value").alias("Payment_Value"),
        F.col("review_score").alias("Review_Score"),
        F.col("product_category_name_english").alias("Product_Category")
    )
)

fact_orders.write.mode("overwrite").format("delta").save(f"{gold_path}/fact_orders")


# ---------------------------------------------------------------
# Create DIMENSION TABLES
# ---------------------------------------------------------------

dim_customers = customers_df.select(
    F.col("customer_id").alias("Customer_ID"),
    F.col("customer_city").alias("Customer_City"),
    F.col("customer_state").alias("Customer_State")
)

dim_customers.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_customers")


dim_sellers = sellers_df.select(
    F.col("seller_id").alias("Seller_ID"),
    F.col("seller_city").alias("Seller_City"),
    F.col("seller_state").alias("Seller_State")
)

dim_sellers.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_sellers")


dim_products = products_en.select(
    F.col("product_id").alias("Product_ID"),
    F.col("product_category_name_english").alias("Product_Category")
)

dim_products.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_products")


dim_date = (
    fact_orders
    .select("Order_Date")
    .withColumn("Date", F.to_date("Order_Date"))
    .withColumn("Year", F.year("Date"))
    .withColumn("Month", F.month("Date"))
    .withColumn("Day", F.dayofmonth("Date"))
    .dropDuplicates()
)

dim_date.write.mode("overwrite").format("delta").save(f"{gold_path}/dim_date")


# ---------------------------------------------------------------
# Aggregated Tables
# ---------------------------------------------------------------

agg_customer_ltv = (
    fact_orders
    .groupBy("Customer_ID")
    .agg(
        F.countDistinct("Order_ID").alias("Total_Orders"),
        F.sum("Payment_Value").alias("Total_Revenue"),
        F.avg("Payment_Value").alias("Avg_Order_Value")
    )
)

agg_customer_ltv.write.mode("overwrite").format("delta").save(f"{gold_path}/agg_customer_ltv")


agg_seller_performance = (
    fact_orders
    .groupBy("Seller_ID")
    .agg(
        F.countDistinct("Order_ID").alias("Total_Orders"),
        F.sum("Payment_Value").alias("Total_Revenue"),
        F.avg("Review_Score").alias("Avg_Review_Score")
    )
)

agg_seller_performance.write.mode("overwrite").format("delta").save(f"{gold_path}/agg_seller_performance")


agg_product_monthly = (
    fact_orders
    .withColumn("Year", F.year("Order_Date"))
    .withColumn("Month", F.month("Order_Date"))
    .groupBy("Product_Category", "Year", "Month")
    .agg(
        F.countDistinct("Order_ID").alias("Total_Orders"),
        F.sum("Unit_Price").alias("Total_Revenue"),
        F.sum("Freight_Value").alias("Total_Freight"),
        F.avg("Unit_Price").alias("Avg_Price"),
        F.avg("Review_Score").alias("Avg_Review_Score")
    )
)

agg_product_monthly.write.mode("overwrite").format("delta").save(f"{gold_path}/agg_product_monthly")


print("Gold Layer tables successfully created.")