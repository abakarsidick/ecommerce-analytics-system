import json
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid

fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_USERS = 1000
NUM_PRODUCTS = 500
NUM_CATEGORIES = 20
NUM_SESSIONS = 5000
NUM_TRANSACTIONS = 2000
DAYS_OF_DATA = 90

end_date = datetime.now()
start_date = end_date - timedelta(days=DAYS_OF_DATA)

def generate_categories():
    categories = []
    for i in range(1, NUM_CATEGORIES + 1):
        category = {
            "category_id": f"cat_{i:03d}",
            "name": fake.word().capitalize() + " " + fake.word(),
            "description": fake.sentence(),
            "parent_category": None if i < 5 else f"cat_{random.randint(1,5):03d}",
            "subcategories": []
        }
        
        # Add subcategories
        num_subcats = random.randint(2, 5)
        for j in range(1, num_subcats + 1):
            subcat = {
                "subcategory_id": f"sub_{i:03d}_{j:02d}",
                "name": fake.word().capitalize(),
                "description": fake.sentence()
            }
            category["subcategories"].append(subcat)
        
        categories.append(category)
    
    return categories

def generate_products(categories):
    products = []
    statuses = ["active", "active", "active", "inactive"]  # 75% active
    
    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choice(categories)
        subcategory = random.choice(category["subcategories"]) if category["subcategories"] else None
        
        product = {
            "product_id": f"prod_{i:05d}",
            "name": fake.catch_phrase(),
            "description": fake.text(max_nb_chars=200),
            "category_id": category["category_id"],
            "subcategory_id": subcategory["subcategory_id"] if subcategory else None,
            "price": round(random.uniform(10.0, 500.0), 2),
            "current_stock": random.randint(0, 1000),
            "status": random.choice(statuses),
            "created_at": fake.date_time_between(start_date=start_date, end_date=end_date).isoformat(),
            "specifications": {
                "brand": fake.company(),
                "weight": f"{random.randint(1, 10)} kg",
                "dimensions": f"{random.randint(10, 50)}x{random.randint(10, 50)}x{random.randint(10, 50)} cm"
            }
        }
        products.append(product)
    
    return products

def generate_users():
    users = []
    states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    
    for i in range(1, NUM_USERS + 1):
        registration = fake.date_time_between(start_date=start_date, end_date=end_date)
        last_active = registration + timedelta(days=random.randint(1, 30))
        
        user = {
            "user_id": f"user_{i:06d}",
            "geo_data": {
                "city": fake.city(),
                "state": random.choice(states),
                "country": "US"
            },
            "registration_date": registration.isoformat(),
            "last_active": last_active.isoformat() if last_active < end_date else end_date.isoformat()
        }
        users.append(user)
    
    return users

def generate_sessions(users, products):
    sessions = []
    page_types = ["home", "search", "product_detail", "category", "cart", "checkout"]
    referrers = ["direct", "search_engine", "social_media", "email", "advertisement"]
    devices = ["desktop", "mobile", "tablet"]
    
    for i in range(1, NUM_SESSIONS + 1):
        user = random.choice(users)
        session_start = fake.date_time_between(start_date=start_date, end_date=end_date)
        
        # Generate page views (2-15 per session)
        num_views = random.randint(2, 15)
        page_views = []
        cart_contents = {}
        conversion_status = "bounce" if num_views < 3 else random.choice(["viewed", "added_to_cart", "converted"])
        
        current_time = session_start
        for j in range(num_views):
            page_type = random.choice(page_types)
            product_id = random.choice(products)["product_id"] if page_type in ["product_detail", "cart"] else None
            category_id = random.choice([p["category_id"] for p in products]) if page_type == "category" else None
            
            view = {
                "timestamp": current_time.isoformat(),
                "page_type": page_type,
                "product_id": product_id,
                "category_id": category_id,
                "view_duration": random.randint(5, 300)
            }
            page_views.append(view)
            
            # Simulate adding to cart
            if page_type == "product_detail" and random.random() < 0.3 and conversion_status in ["added_to_cart", "converted"]:
                if product_id not in cart_contents:
                    cart_contents[product_id] = {
                        "quantity": random.randint(1, 3),
                        "price": round(random.uniform(10.0, 500.0), 2)
                    }
            
            current_time += timedelta(seconds=random.randint(30, 300))
        
        session = {
            "session_id": f"sess_{uuid.uuid4().hex[:10]}",
            "user_id": user["user_id"],
            "start_time": session_start.isoformat(),
            "end_time": current_time.isoformat(),
            "device": random.choice(devices),
            "referrer": random.choice(referrers),
            "page_views": page_views,
            "cart_contents": cart_contents,
            "conversion_status": conversion_status
        }
        sessions.append(session)
    
    return sessions

def generate_transactions(users, sessions, products):
    transactions = []
    payment_methods = ["credit_card", "paypal", "debit_card", "gift_card"]
    statuses = ["completed", "shipped", "delivered", "cancelled", "refunded"]
    
    # Only use sessions that converted
    converted_sessions = [s for s in sessions if s["conversion_status"] == "converted"]
    num_transactions = min(len(converted_sessions), NUM_TRANSACTIONS)
    
    for i in range(num_transactions):
        session = random.choice(converted_sessions)
        user = next(u for u in users if u["user_id"] == session["user_id"])
        
        # Generate line items from cart contents
        items = []
        total_amount = 0
        
        for product_id, cart_item in session["cart_contents"].items():
            # Find product price
            product = next((p for p in products if p["product_id"] == product_id), None)
            if product:
                price = cart_item["price"]
                quantity = cart_item["quantity"]
                subtotal = price * quantity
                total_amount += subtotal
                
                items.append({
                    "product_id": product_id,
                    "quantity": quantity,
                    "price_per_unit": price,
                    "subtotal": round(subtotal, 2)
                })
        
        if not items:  # Skip if no items
            continue
        
        transaction = {
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            "session_id": session["session_id"],
            "user_id": user["user_id"],
            "timestamp": session["end_time"],
            "items": items,
            "total": round(total_amount, 2),
            "payment_method": random.choice(payment_methods),
            "status": random.choice(statuses),
            "shipping_address": {
                "city": user["geo_data"]["city"],
                "state": user["geo_data"]["state"],
                "country": user["geo_data"]["country"]
            }
        }
        transactions.append(transaction)
    
    return transactions

def main():
    print("🚀 Generating e-commerce dataset...")
    
    print("📁 Generating categories...")
    categories = generate_categories()
    with open('categories.json', 'w') as f:
        json.dump(categories, f, indent=2)
    print(f"✅ Generated {len(categories)} categories")
    
    print("📁 Generating products...")
    products = generate_products(categories)
    with open('products.json', 'w') as f:
        json.dump(products, f, indent=2)
    print(f"✅ Generated {len(products)} products")
    
    print("📁 Generating users...")
    users = generate_users()
    with open('users.json', 'w') as f:
        json.dump(users, f, indent=2)
    print(f"✅ Generated {len(users)} users")
    
    print("📁 Generating sessions...")
    sessions = generate_sessions(users, products)
    with open('sessions.json', 'w') as f:
        json.dump(sessions, f, indent=2)
    print(f"✅ Generated {len(sessions)} sessions")
    
    print("📁 Generating transactions...")
    transactions = generate_transactions(users, sessions, products)
    with open('transactions.json', 'w') as f:
        json.dump(transactions, f, indent=2)
    print(f"✅ Generated {len(transactions)} transactions")
    
    print("\n🔥 Dataset generation complete!")
    print(f"Files created in: {os.getcwd()}")
    print("📄 categories.json")
    print("📄 products.json")
    print("📄 users.json")
    print("📄 sessions.json")
    print("📄 transactions.json")

if __name__ == "__main__":
    import os
    main()