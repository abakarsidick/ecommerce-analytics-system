from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Initialize Spark
spark = SparkSession.builder \
    .appName("ECommerce Analytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("🔥 Spark Session Created")

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Read JSON files
print("\n📁 Reading datasets...")

users_df = spark.read.json("users.json")
products_df = spark.read.json("products.json")
transactions_df = spark.read.json("transactions.json")
sessions_df = spark.read.json("sessions.json")

print(f"✅ Users: {users_df.count()} records")
print(f"✅ Products: {products_df.count()} records")
print(f"✅ Transactions: {transactions_df.count()} records")
print(f"✅ Sessions: {sessions_df.count()} records")

# Show schema
print("\n📊 Data Schemas:")
print("Users schema:")
users_df.printSchema()

# Register temp views for SQL
users_df.createOrReplaceTempView("users")
products_df.createOrReplaceTempView("products")
transactions_df.createOrReplaceTempView("transactions")
sessions_df.createOrReplaceTempView("sessions")

print("\n" + "="*60)
print("🔍 SPARK SQL ANALYTICS")
print("="*60)

# 1. Customer Lifetime Value Analysis
print("\n📊 TOP 10 CUSTOMERS BY LIFETIME VALUE:")
result = spark.sql("""
    SELECT 
        u.user_id,
        u.geo_data.state,
        COUNT(DISTINCT t.transaction_id) as num_orders,
        ROUND(SUM(t.total), 2) as total_spent,
        ROUND(AVG(t.total), 2) as avg_order_value
    FROM users u
    JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id, u.geo_data.state
    ORDER BY total_spent DESC
    LIMIT 10
""")
result.show(truncate=False)

# 2. Product Performance Analysis
print("\n📊 PRODUCT PERFORMANCE (TOP 10):")
result = spark.sql("""
    WITH product_sales AS (
        SELECT 
            explode(items) as item,
            transaction_id
        FROM transactions
    )
    SELECT 
        p.product_id,
        p.name,
        p.category_id,
        p.price as unit_price,
        COUNT(DISTINCT ps.transaction_id) as times_purchased,
        SUM(ps.item.quantity) as total_quantity,
        ROUND(SUM(ps.item.subtotal), 2) as total_revenue
    FROM product_sales ps
    JOIN products p ON ps.item.product_id = p.product_id
    GROUP BY p.product_id, p.name, p.category_id, p.price
    ORDER BY total_revenue DESC
    LIMIT 10
""")
result.show(truncate=False)

# 3. Category Performance
print("\n📊 CATEGORY PERFORMANCE:")
result = spark.sql("""
    WITH product_sales AS (
        SELECT 
            explode(items) as item,
            transaction_id
        FROM transactions
    )
    SELECT 
        p.category_id,
        COUNT(DISTINCT ps.transaction_id) as num_transactions,
        SUM(ps.item.quantity) as items_sold,
        ROUND(SUM(ps.item.subtotal), 2) as total_revenue,
        ROUND(AVG(ps.item.subtotal), 2) as avg_revenue_per_sale
    FROM product_sales ps
    JOIN products p ON ps.item.product_id = p.product_id
    GROUP BY p.category_id
    ORDER BY total_revenue DESC
""")
result.show(truncate=False)

# 4. Session to Purchase Conversion Analysis
print("\n📊 SESSION CONVERSION RATES:")
result = spark.sql("""
    SELECT 
        conversion_status,
        COUNT(*) as num_sessions,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sessions), 2) as percentage
    FROM sessions
    GROUP BY conversion_status
    ORDER BY num_sessions DESC
""")
result.show()

# 5. Monthly Active Users vs New Users
print("\n📊 USER ACTIVITY TRENDS:")
result = spark.sql("""
    SELECT 
        DATE_FORMAT(TO_DATE(t.timestamp), 'yyyy-MM') as month,
        COUNT(DISTINCT t.user_id) as active_users,
        COUNT(DISTINCT CASE WHEN DATE_FORMAT(TO_DATE(u.registration_date), 'yyyy-MM') = DATE_FORMAT(TO_DATE(t.timestamp), 'yyyy-MM') 
                          THEN u.user_id END) as new_users
    FROM transactions t
    JOIN users u ON t.user_id = u.user_id
    GROUP BY DATE_FORMAT(TO_DATE(t.timestamp), 'yyyy-MM')
    ORDER BY month
""")
result.show()

# 6. Product Affinity (Products bought together)
print("\n📊 PRODUCTS FREQUENTLY BOUGHT TOGETHER:")
affinity_df = spark.sql("""
    WITH transaction_items AS (
        SELECT 
            transaction_id,
            collect_list(item.product_id) as products
        FROM (
            SELECT 
                transaction_id,
                explode(items) as item
            FROM transactions
        )
        GROUP BY transaction_id
        HAVING SIZE(products) >= 2
    )
    SELECT 
        ti1.products[0] as product1,
        ti2.products[0] as product2,
        COUNT(*) as times_bought_together
    FROM transaction_items ti1
    JOIN transaction_items ti2 ON ti1.transaction_id = ti2.transaction_id
    WHERE ti1.products[0] < ti2.products[0]
    GROUP BY ti1.products[0], ti2.products[0]
    ORDER BY times_bought_together DESC
    LIMIT 10
""")
affinity_df.show(truncate=False)

# 7. State-wise Performance Summary
print("\n📊 STATE-WISE PERFORMANCE:")
result = spark.sql("""
    SELECT 
        u.geo_data.state,
        COUNT(DISTINCT u.user_id) as total_users,
        COUNT(DISTINCT t.transaction_id) as total_transactions,
        ROUND(COALESCE(SUM(t.total), 0), 2) as total_revenue,
        ROUND(COALESCE(AVG(t.total), 0), 2) as avg_transaction_value
    FROM users u
    LEFT JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.geo_data.state
    ORDER BY total_revenue DESC
""")
result.show()

print("\n" + "="*60)
print("✅ SPARK ANALYTICS COMPLETE!")
print("="*60)

# Save results to CSV for visualization
print("\n💾 Saving results for visualization...")

# Top customers
spark.sql("""
    SELECT 
        u.user_id,
        u.geo_data.state,
        COUNT(DISTINCT t.transaction_id) as num_orders,
        ROUND(SUM(t.total), 2) as total_spent
    FROM users u
    JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id, u.geo_data.state
    ORDER BY total_spent DESC
    LIMIT 20
""").coalesce(1).write.mode("overwrite").option("header", "true").csv("output/top_customers")

# Monthly revenue
spark.sql("""
    SELECT 
        DATE_FORMAT(TO_DATE(timestamp), 'yyyy-MM') as month,
        COUNT(*) as transactions,
        ROUND(SUM(total), 2) as revenue
    FROM transactions
    GROUP BY DATE_FORMAT(TO_DATE(timestamp), 'yyyy-MM')
    ORDER BY month
""").coalesce(1).write.mode("overwrite").option("header", "true").csv("output/monthly_revenue")

print("✅ Results saved to 'output/' folder")

spark.stop()