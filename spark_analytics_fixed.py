from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark
spark = SparkSession.builder \
    .appName("ECommerce Analytics") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("🔥 Spark Session Created")
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

# Register temp views
users_df.createOrReplaceTempView("users")
products_df.createOrReplaceTempView("products")
transactions_df.createOrReplaceTempView("transactions")
sessions_df.createOrReplaceTempView("sessions")

print("\n" + "="*60)
print("🔍 SPARK SQL ANALYTICS")
print("="*60)

# 1. Customer Lifetime Value
print("\n📊 TOP 10 CUSTOMERS BY LIFETIME VALUE:")
spark.sql("""
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
""").show(truncate=False)

# 2. Product Performance
print("\n📊 PRODUCT PERFORMANCE (TOP 10):")
spark.sql("""
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
""").show(truncate=False)

# 3. Category Performance
print("\n📊 CATEGORY PERFORMANCE:")
spark.sql("""
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
""").show(truncate=False)

# 4. Conversion Analysis
print("\n📊 SESSION CONVERSION RATES:")
spark.sql("""
    SELECT 
        conversion_status,
        COUNT(*) as num_sessions,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sessions), 2) as percentage
    FROM sessions
    GROUP BY conversion_status
    ORDER BY num_sessions DESC
""").show()

# 5. Monthly Trends
print("\n📊 USER ACTIVITY TRENDS:")
spark.sql("""
    SELECT 
        DATE_FORMAT(TO_DATE(t.timestamp), 'yyyy-MM') as month,
        COUNT(DISTINCT t.user_id) as active_users,
        COUNT(DISTINCT CASE WHEN DATE_FORMAT(TO_DATE(u.registration_date), 'yyyy-MM') = DATE_FORMAT(TO_DATE(t.timestamp), 'yyyy-MM') 
                          THEN u.user_id END) as new_users
    FROM transactions t
    JOIN users u ON t.user_id = u.user_id
    GROUP BY DATE_FORMAT(TO_DATE(t.timestamp), 'yyyy-MM')
    ORDER BY month
""").show()

# 6. State Performance
print("\n📊 STATE-WISE PERFORMANCE:")
spark.sql("""
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
""").show()

print("\n" + "="*60)
print("✅ SPARK ANALYTICS COMPLETE!")
print("="*60)
print("\n💡 For visualizations, copy the outputs above into Excel/Google Sheets")
print("   or use Python libraries like matplotlib/seaborn")

spark.stop()