import json
from pymongo import MongoClient, ASCENDING
from datetime import datetime
import os

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ecommerce_db']

print("🔥 Connected to MongoDB")

def load_json_file(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        return json.load(f)

def load_categories():
    print("\n📁 Loading categories...")
    categories = load_json_file('categories.json')
    
    db.categories.drop()
    result = db.categories.insert_many(categories)
    print(f"✅ Inserted {len(result.inserted_ids)} categories")
    
    db.categories.create_index([("category_id", ASCENDING)], unique=True)
    print("✅ Created index on category_id")

def load_products():
    print("\n📁 Loading products...")
    products = load_json_file('products.json')
    
    db.products.drop()
    result = db.products.insert_many(products)
    print(f"✅ Inserted {len(result.inserted_ids)} products")
    
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING)])
    db.products.create_index([("price", ASCENDING)])
    print("✅ Created indexes")

def load_users():
    print("\n📁 Loading users...")
    users = load_json_file('users.json')
    
    db.users.drop()
    result = db.users.insert_many(users)
    print(f"✅ Inserted {len(result.inserted_ids)} users")
    
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.users.create_index([("geo_data.state", ASCENDING)])
    print("✅ Created indexes")

def load_transactions():
    print("\n📁 Loading transactions...")
    transactions = load_json_file('transactions.json')
    
    db.transactions.drop()
    result = db.transactions.insert_many(transactions)
    print(f"✅ Inserted {len(result.inserted_ids)} transactions")
    
    db.transactions.create_index([("transaction_id", ASCENDING)], unique=True)
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    print("✅ Created indexes")

def run_analytics():
    print("\n" + "="*60)
    print("🔍 RUNNING MONGODB ANALYTICS QUERIES")
    print("="*60)
    
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
    
    for i, r in enumerate(db.transactions.aggregate(pipeline), 1):
        print(f"{i}. Product {r['_id']}: ${r['revenue']:.2f} ({r['orders']} orders)")
    
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
    
    for r in db.transactions.aggregate(pipeline):
        print(f"{r['_id']}: ${r['revenue']:.2f} ({r['transactions']} orders, avg ${r['avg_order']:.2f})")
    
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
    
    for r in db.transactions.aggregate(pipeline):
        print(f"{r['_id']}: ${r['revenue']:.2f} ({r['transactions']} transactions)")

def main():
    print("🚀 LOADING DATA INTO MONGODB...")
    
    load_categories()
    load_products()
    load_users()
    load_transactions()
    
    run_analytics()
    
    print("\n" + "="*60)
    print("✅ MONGODB IMPLEMENTATION COMPLETE!")
    print("="*60)
    print(f"\nCollections:")
    print(f"  • categories: {db.categories.count_documents({})}")
    print(f"  • products: {db.products.count_documents({})}")
    print(f"  • users: {db.users.count_documents({})}")
    print(f"  • transactions: {db.transactions.count_documents({})}")

if __name__ == "__main__":
    main()