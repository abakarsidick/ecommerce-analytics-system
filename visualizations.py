import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
import os

# Create output directory for charts
os.makedirs("charts", exist_ok=True)

# Initialize Spark
spark = SparkSession.builder \
    .appName("ECommerce Visualizations") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

print("🔥 Creating Visualizations...")
spark.sparkContext.setLogLevel("WARN")

# Read data
users_df = spark.read.json("users.json")
products_df = spark.read.json("products.json")
transactions_df = spark.read.json("transactions.json")
sessions_df = spark.read.json("sessions.json")

# Register temp views
users_df.createOrReplaceTempView("users")
products_df.createOrReplaceTempView("products")
transactions_df.createOrReplaceTempView("transactions")
sessions_df.createOrReplaceTempView("sessions")

# Set style
plt.style.use('default')
colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEEAD', '#D4A5A5', '#9B59B6', '#3498DB', '#E67E22', '#2ECC71']

# 1. REVENUE BY STATE
print("\n📊 Creating Revenue by State chart...")
state_revenue = spark.sql("""
    SELECT 
        u.geo_data.state,
        ROUND(SUM(t.total), 2) as revenue
    FROM users u
    JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.geo_data.state
    ORDER BY revenue DESC
""").toPandas()

plt.figure(figsize=(14, 8))
bars = plt.bar(state_revenue['state'], state_revenue['revenue'], color=colors[:len(state_revenue)])
plt.title('Total Revenue by State', fontsize=20, fontweight='bold', pad=20)
plt.xlabel('State', fontsize=14)
plt.ylabel('Revenue ($)', fontsize=14)
plt.grid(axis='y', alpha=0.3)

# Add value labels on bars
for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'${height:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('charts/revenue_by_state.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/revenue_by_state.png")

# 2. MONTHLY REVENUE TREND
print("\n📊 Creating Monthly Revenue Trend chart...")
monthly_revenue = spark.sql("""
    SELECT 
        DATE_FORMAT(TO_DATE(timestamp), 'yyyy-MM') as month,
        ROUND(SUM(total), 2) as revenue
    FROM transactions
    GROUP BY DATE_FORMAT(TO_DATE(timestamp), 'yyyy-MM')
    ORDER BY month
""").toPandas()

plt.figure(figsize=(14, 8))
plt.plot(monthly_revenue['month'], monthly_revenue['revenue'], 
         marker='o', linewidth=4, markersize=10, color='#2ECC71', markerfacecolor='#E74C3C')
plt.title('Monthly Revenue Trend', fontsize=20, fontweight='bold', pad=20)
plt.xlabel('Month', fontsize=14)
plt.ylabel('Revenue ($)', fontsize=14)
plt.grid(True, alpha=0.3)

# Add value labels
for i, (month, revenue) in enumerate(zip(monthly_revenue['month'], monthly_revenue['revenue'])):
    plt.annotate(f'${revenue:,.0f}', 
                xy=(i, revenue), 
                xytext=(0, 10),
                textcoords='offset points',
                ha='center',
                fontsize=10,
                fontweight='bold')

plt.tight_layout()
plt.savefig('charts/monthly_revenue.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/monthly_revenue.png")

# 3. TOP 10 PRODUCTS BY REVENUE
print("\n📊 Creating Top Products chart...")
top_products = spark.sql("""
    WITH product_sales AS (
        SELECT 
            explode(items) as item
        FROM transactions
    )
    SELECT 
        p.name,
        ROUND(SUM(ps.item.subtotal), 2) as revenue
    FROM product_sales ps
    JOIN products p ON ps.item.product_id = p.product_id
    GROUP BY p.name
    ORDER BY revenue DESC
    LIMIT 10
""").toPandas()

# Truncate long names
top_products['name'] = top_products['name'].str[:30] + '...'

plt.figure(figsize=(14, 8))
bars = plt.barh(range(len(top_products)), top_products['revenue'], color=plt.cm.Paired(np.arange(len(top_products))))
plt.yticks(range(len(top_products)), top_products['name'])
plt.title('Top 10 Products by Revenue', fontsize=20, fontweight='bold', pad=20)
plt.xlabel('Revenue ($)', fontsize=14)
plt.ylabel('Product Name', fontsize=14)
plt.grid(axis='x', alpha=0.3)

# Add value labels
for i, (bar, revenue) in enumerate(zip(bars, top_products['revenue'])):
    plt.text(bar.get_width() + 50, bar.get_y() + bar.get_height()/2,
             f'${revenue:,.0f}', va='center', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('charts/top_products.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/top_products.png")

# 4. CONVERSION FUNNEL
print("\n📊 Creating Conversion Funnel chart...")
funnel = spark.sql("""
    SELECT 
        conversion_status,
        COUNT(*) as count
    FROM sessions
    GROUP BY conversion_status
    ORDER BY CASE conversion_status
        WHEN 'bounce' THEN 1
        WHEN 'viewed' THEN 2
        WHEN 'added_to_cart' THEN 3
        WHEN 'converted' THEN 4
    END
""").toPandas()

plt.figure(figsize=(12, 8))
colors_funnel = ['#E74C3C', '#F39C12', '#3498DB', '#2ECC71']
bars = plt.bar(funnel['conversion_status'], funnel['count'], color=colors_funnel)
plt.title('Session Conversion Funnel', fontsize=20, fontweight='bold', pad=20)
plt.xlabel('Conversion Status', fontsize=14)
plt.ylabel('Number of Sessions', fontsize=14)
plt.grid(axis='y', alpha=0.3)

# Add percentage labels
total = funnel['count'].sum()
for i, (bar, count) in enumerate(zip(bars, funnel['count'])):
    percentage = (count / total) * 100
    plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
             f'{count:,}\n({percentage:.1f}%)', ha='center', fontsize=11, fontweight='bold')

plt.tight_layout()
plt.savefig('charts/conversion_funnel.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/conversion_funnel.png")

# 5. CATEGORY PERFORMANCE (PIE CHART)
print("\n📊 Creating Category Performance chart...")
category_perf = spark.sql("""
    WITH product_sales AS (
        SELECT 
            explode(items) as item
        FROM transactions
    )
    SELECT 
        p.category_id,
        ROUND(SUM(ps.item.subtotal), 2) as revenue
    FROM product_sales ps
    JOIN products p ON ps.item.product_id = p.product_id
    GROUP BY p.category_id
    ORDER BY revenue DESC
    LIMIT 8
""").toPandas()

plt.figure(figsize=(12, 8))
plt.pie(category_perf['revenue'], labels=category_perf['category_id'], 
        autopct='%1.1f%%', startangle=90, colors=plt.cm.Set3(np.arange(len(category_perf))))
plt.title('Top 8 Categories by Revenue', fontsize=20, fontweight='bold', pad=20)
plt.axis('equal')
plt.tight_layout()
plt.savefig('charts/category_pie.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/category_pie.png")

# 6. CUSTOMER SEGMENTATION (Orders per customer)
print("\n📊 Creating Customer Segmentation chart...")
customer_orders = spark.sql("""
    SELECT 
        user_id,
        COUNT(*) as order_count
    FROM transactions
    GROUP BY user_id
""").toPandas()

# Create segments
segments = {
    '1 order': (customer_orders['order_count'] == 1).sum(),
    '2-3 orders': ((customer_orders['order_count'] >= 2) & (customer_orders['order_count'] <= 3)).sum(),
    '4-5 orders': ((customer_orders['order_count'] >= 4) & (customer_orders['order_count'] <= 5)).sum(),
    '6+ orders': (customer_orders['order_count'] >= 6).sum()
}

plt.figure(figsize=(10, 8))
plt.pie(segments.values(), labels=segments.keys(), autopct='%1.1f%%', 
        startangle=90, colors=['#FF9999', '#66B2FF', '#99FF99', '#FFCC99'])
plt.title('Customer Segmentation by Order Frequency', fontsize=20, fontweight='bold', pad=20)
plt.axis('equal')
plt.tight_layout()
plt.savefig('charts/customer_segments.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/customer_segments.png")

# 7. AVERAGE ORDER VALUE BY STATE
print("\n📊 Creating Average Order Value chart...")
avg_order = spark.sql("""
    SELECT 
        u.geo_data.state,
        ROUND(AVG(t.total), 2) as avg_order_value
    FROM users u
    JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.geo_data.state
    ORDER BY avg_order_value DESC
    LIMIT 10
""").toPandas()

plt.figure(figsize=(14, 8))
bars = plt.bar(avg_order['state'], avg_order['avg_order_value'], color='#9B59B6')
plt.title('Average Order Value by State (Top 10)', fontsize=20, fontweight='bold', pad=20)
plt.xlabel('State', fontsize=14)
plt.ylabel('Average Order Value ($)', fontsize=14)
plt.grid(axis='y', alpha=0.3)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2., height,
             f'${height:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
plt.savefig('charts/avg_order_value.png', dpi=300, bbox_inches='tight')
print("✅ Saved: charts/avg_order_value.png")

print("\n" + "="*60)
print("✅ ALL VISUALIZATIONS COMPLETE!")
print("="*60)
print("\n📁 Charts saved in: charts/")
print("Files created:")
for file in os.listdir('charts'):
    print(f"  • charts/{file}")

spark.stop()