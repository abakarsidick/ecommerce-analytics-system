import json
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import os

# Connect to MongoDB
try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    # Test connection
    client.admin.command('ping')
    db = client['ecommerce_db']
    print("🔥 Connected to MongoDB successfully!")
except Exception as e:
    print(f"❌ Cannot connect to MongoDB: {e}")
    print("\n💡 Make sure MongoDB is running:")
    print("   net start MongoDB (run as Administrator)")
    exit(1)

def load_json_lines(filename):
    """Load JSON file where each line is a separate JSON object"""
    data = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"  ⚠️ Error on line {line_num}: {e}")
                        continue
        print(f"  ✅ Loaded {len(data)} records from {filename}")
        return data
    except FileNotFoundError:
        print(f"  ❌ File not found: {filename}")
        return []

def load_categories():
    print("\n📁 Loading categories...")
    categories = load_json_lines('categories.json')
    
    if not categories:
        print("  ⚠️ No categories to load")
        return
    
    db.categories.drop()
    result = db.categories.insert_many(categories)
    print(f"  ✅ Inserted {len(result.inserted_ids)} categories")
    
    db.categories.create_index([("category_id", ASCENDING)], unique=True)
    print("  ✅ Created index on category_id")

def load_products():
    print("\n📁 Loading products...")
    products = load_json_lines('products.json')
    
    if not products:
        print("  ⚠️ No products to load")
        return
    
    db.products.drop()
    result = db.products.insert_many(products)
    print(f"  ✅ Inserted {len(result.inserted_ids)} products")
    
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING)])
    db.products.create_index([("price", ASCENDING)])
    print("  ✅ Created indexes")

def load_users():
    print("\n📁 Loading users...")
    users = load_json_lines('users.json')
    
    if not users:
        print("  ⚠️ No users to load")
        return
    
    db.users.drop()
    result = db.users.insert_many(users)
    print(f"  ✅ Inserted {len(result.inserted_ids)} users")
    
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.users.create_index([("geo_data.state", ASCENDING)])
    print("  ✅ Created indexes")

def load_transactions():
    print("\n📁 Loading transactions...")
    transactions = load_json_lines('transactions.json')
    
    if not transactions:
        print("  ⚠️ No transactions to load")
        return
    
    db.transactions.drop()
    result = db.transactions.insert_many(transactions)
    print(f"  ✅ Inserted {len(result.inserted_ids)} transactions")
    
    db.transactions.create_index([("transaction_id", ASCENDING)], unique=True)
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    print("  ✅ Created indexes")

def run_analytics():
    print("\n" + "="*60)
    print("🔍 RUNNING MONGODB ANALYTICS QUERIES")
    print("="*60)
    
    # Check if we have data
    if db.transactions.count_documents({}) == 0:
        print("⚠️ No transactions found. Skipping analytics.")
        return
    
    # Query 1: Top 10 products by revenue
    print("\n📊 TOP 10 PRODUCTS BY REVENUE:")
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.product_id",
            "revenue": {"$sum": "$items.subtotal"},
            "quantity": {"$sum": "$items.quantity"},
            "orders": {"$sum": 1}
        }},
        {"$sort": {"revenue": -1}},
        {"$limit": 10}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    if results:
        for i, r in enumerate(results, 1):
            print(f"{i}. Product {r['_id']}: ${r['revenue']:.2f} ({r['orders']} orders)")
    else:
        print("  No results found")
    
    # Query 2: Revenue by state
    print("\n📊 REVENUE BY STATE:")
    pipeline = [
        {"$lookup": {
            "from": "users",
            "localField": "user_id",
            "foreignField": "user_id",
            "as": "user"
        }},
        {"$unwind": "$user"},
        {"$group": {
            "_id": "$user.geo_data.state",
            "revenue": {"$sum": "$total"},
            "transactions": {"$sum": 1},
            "avg_order": {"$avg": "$total"}
        }},
        {"$sort": {"revenue": -1}}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    if results:
        for r in results[:10]:  # Top 10 states
            print(f"{r['_id']}: ${r['revenue']:.2f} ({r['transactions']} orders, avg ${r['avg_order']:.2f})")
    else:
        print("  No results found")
    
    # Query 3: Monthly sales trend
    print("\n📊 MONTHLY SALES TREND:")
    pipeline = [
        {"$addFields": {
            "month": {"$dateToString": {"format": "%Y-%m", "date": {"$dateFromString": {"dateString": "$timestamp"}}}}
        }},
        {"$group": {
            "_id": "$month",
            "revenue": {"$sum": "$total"},
            "transactions": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    
    results = list(db.transactions.aggregate(pipeline))
    if results:
        for r in results:
            print(f"{r['_id']}: ${r['revenue']:.2f} ({r['transactions']} transactions)")
    else:
        print("  No results found")

def main():
    print("🚀 LOADING DATA INTO MONGODB...")
    
    # Check if MongoDB is accessible
    try:
        client.admin.command('ping')
    except Exception as e:
        print(f"❌ MongoDB not accessible: {e}")
        return
    
    load_categories()
    load_products()
    load_users()
    load_transactions()
    
    # Show collection counts
    print("\n" + "="*60)
    print("📊 COLLECTION SUMMARY")
    print("="*60)
    print(f"  • categories: {db.categories.count_documents({})}")
    print(f"  • products: {db.products.count_documents({})}")
    print(f"  • users: {db.users.count_documents({})}")
    print(f"  • transactions: {db.transactions.count_documents({})}")
    
    run_analytics()
    
    print("\n" + "="*60)
    print("✅ MONGODB IMPLEMENTATION COMPLETE!")
    print("="*60)

if __name__ == "__main__":
    main()