import os
import redis
import random
import time
import json
import pandas as pd

# Redis connection configuration
REDIS_HOST = os.environ.get('NODE_IP', 'localhost')  # Priority to NODE_IP environment variable
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))  # Priority to REDIS_PORT environment variable
REDIS_DB = 0

# Data generation configuration
TOTAL_USERS = 100000
TOTAL_ITEMS = 100000
BATCH_SIZE = 10000

# Mock data pools
COUNTRIES = ['China', 'United States', 'Japan', 'Germany', 'United Kingdom', 'France', 'India', 'Canada', 'Brazil', 'Australia',
             'Russia', 'South Korea', 'Italy', 'Spain', 'Mexico', 'Indonesia', 'Netherlands', 'Belgium', 'Switzerland', 'Sweden']

OS_LIST = ['Android', 'iOS', 'Windows', 'macOS', 'Linux']

NETWORK_LIST = ['4G', '5G', 'Wi-Fi', 'Ethernet', '3G']

BRANDS = ['Apple', 'Samsung', 'Xiaomi', 'Huawei', 'OPPO', 'vivo', 'Sony', 'LG', 'Google', 'Microsoft',
          'Dell', 'HP', 'Lenovo', 'Asus', 'Acer', 'Nike', 'Adidas', 'Puma', 'Uniqlo', 'Zara']

CATEGORIES1 = ['Electronics', 'Clothing', 'Food', 'Books', 'Toys', 'Furniture', 'Sports', 'Beauty', 'Health', 'Automotive']

CATEGORIES2 = {
    'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Headphones', 'Cameras'],
    'Clothing': ['Tops', 'Pants', 'Dresses', 'Shoes', 'Accessories'],
    'Food': ['Fruits', 'Vegetables', 'Meat', 'Dairy', 'Snacks'],
    'Books': ['Fiction', 'Non-Fiction', 'Science', 'History', 'Biography'],
    'Toys': ['Action Figures', 'Dolls', 'Board Games', 'Puzzles', 'Outdoor Toys'],
    'Furniture': ['Chairs', 'Tables', 'Sofas', 'Beds', 'Storage'],
    'Sports': ['Equipment', 'Clothing', 'Footwear', 'Accessories', 'Nutrition'],
    'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrances', 'Tools'],
    'Health': ['Vitamins', 'Supplements', 'Personal Care', 'Fitness', 'First Aid'],
    'Automotive': ['Parts', 'Accessories', 'Tools', 'Cleaning', 'Maintenance']
}

def generate_user_data(start_id, end_id):
    """Generate user data within specified range"""
    users = []
    for i in range(start_id, end_id):
        user = {
            'id': i,
            'name': f"User{i}",
            'country': random.choice(COUNTRIES),
            'age': random.randint(18, 80),
            'os': random.choice(OS_LIST),
            'network': random.choice(NETWORK_LIST)
        }
        users.append(user)
    return users

def generate_item_data(start_id, end_id):
    """Generate item data within specified range"""
    items = []
    for i in range(start_id, end_id):
        category1 = random.choice(CATEGORIES1)
        category2 = random.choice(CATEGORIES2[category1])
        # Generate category3 and category4 based on category2
        category3 = f"{category2}-Sub1"
        category4 = f"{category2}-Sub2"

        item = {
            'id': i,
            'name': f"{random.choice(BRANDS)} {category2} {i}",  # Use id as part of name
            'price': round(random.uniform(0.99, 9999.99), 2),
            'brand': random.choice(BRANDS),
            'category1': category1,
            'category2': category2,
            'category3': category3,
            'category4': category4
        }
        items.append(item)
    return items

def insert_batch(redis_client, data_list, prefix):
    """Batch insert data into Redis as JSON strings"""
    pipeline = redis_client.pipeline()
    for data in data_list:
        # Add prefix to the key with data id
        key = f"{prefix}{data['id']}"
        # Insert all fields from the data dictionary
        pipeline.set(key, json.dumps(data))
    pipeline.execute()

# Keep original function names for backward compatibility
def insert_user_batch(redis_client, users):
    """Batch insert user data into Redis as JSON strings"""
    insert_batch(redis_client, users, "default:user_table:")

def insert_item_batch(redis_client, items):
    """Batch insert item data into Redis as JSON strings"""
    insert_batch(redis_client, items, "default:item_table:")

def generate_user_table(redis_client, max_users=TOTAL_USERS):
    """Generate data for user_table in single thread"""
    print(f"\nGenerating {max_users} user records for user_table...")

    for i in range(0, max_users, BATCH_SIZE):
        batch_start = i
        batch_end = min(batch_start + BATCH_SIZE, max_users)
        users = generate_user_data(batch_start, batch_end)
        insert_user_batch(redis_client, users)

        progress = ((i + BATCH_SIZE) / max_users) * 100
        print(f"Processing users {batch_start}-{batch_end-1}: {progress:.1f}% completed")

    print("User data generation completed!")

def generate_item_table(redis_client, max_items=TOTAL_ITEMS):
    """Generate data for item_table in single thread"""
    print(f"\nGenerating {max_items} item records for item_table...")

    for i in range(0, max_items, BATCH_SIZE):
        batch_start = i
        batch_end = min(batch_start + BATCH_SIZE, max_items)
        items = generate_item_data(batch_start, batch_end)
        insert_item_batch(redis_client, items)

        progress = ((i + BATCH_SIZE) / max_items) * 100
        print(f"Processing items {batch_start}-{batch_end-1}: {progress:.1f}% completed")

    print("Item data generation completed!")

def generate_global_hot_items(redis_client, total_items):
    """Generate global hot items list with shuffled item IDs and scores from highest to lowest"""
    print("Generating global hot items...")
    pipeline = redis_client.pipeline()
    
    # Generate list of item IDs and shuffle them
    item_ids = list(range(0, total_items))
    random.shuffle(item_ids)
    num_items = 1000
    item_ids = item_ids[:num_items]
    
    # Generate scores from highest to lowest (100.0 down to 0.1)
    min_score = 0.1
    max_score = 100.0
    score_step = (max_score - min_score) / (num_items - 1) if num_items > 1 else 0
    
    # Create global hot items list with default: prefix
    for idx, item_id in enumerate(item_ids):
        score = round(max_score - (idx * score_step), 2)
        score = max(score, min_score)
        item_data = json.dumps({"invert_key": "global", "id": item_id, "score": score})
        pipeline.lpush("default:global_hot_item:global", item_data)
    
    pipeline.execute()
    print(f"Global hot items generated: {num_items} items")

def generate_user_interest_category1(redis_client, total_users):
    """Generate user interest categories list"""
    print("Generating user interest categories...")
    for start_id in range(0, total_users, BATCH_SIZE):
        end_id = min(start_id + BATCH_SIZE - 1, total_users - 1)
        pipeline = redis_client.pipeline()

        for user_id in range(start_id, end_id + 1):
            # Each user has 3-5 category interests
            interest_count = random.randint(3, 5)
            interests = random.sample(CATEGORIES1, interest_count)

            for category1 in interests:
                score = round(random.uniform(0.1, 10.0), 2)
                interest_data = json.dumps({"user_id": user_id, "category1": category1, "score": score})
                # Add default: prefix to the key
                pipeline.lpush(f"default:user_interest_category1:{user_id}", interest_data)

        pipeline.execute()
        print(f"Processed users {start_id}-{end_id} for interest categories")


def generate_category1_hot_items(redis_client, total_items):
    """Generate category hot items lists with 1000 randomly selected items per category"""
    print("Generating category hot items...")

    # For each category, create a hot items list
    item_ids = list(range(0, total_items))
    for category1 in CATEGORIES1:
        pipeline = redis_client.pipeline()
        
        # Generate 1000 unique random item IDs between 0 and total_items-1
        num_items = min(1000, total_items)
        selected_items = random.sample(item_ids, num_items)
        
        # Generate scores from highest to lowest (100.0 down to 0.1)
        min_score = 0.1
        max_score = 100.0
        score_step = (max_score - min_score) / (num_items - 1) if num_items > 1 else 0
        
        # Add items to Redis with scores from highest to lowest
        for idx, item_id in enumerate(selected_items):
            score = round(max_score - (idx * score_step), 2)
            score = max(score, min_score)  # Ensure minimum score
            item_data = json.dumps({"category1": category1, "item_id": item_id, "score": score})
            # Add default: prefix to the key
            pipeline.lpush(f"default:category1_hot_item:{category1}", item_data)

        pipeline.execute()
        print(f"Generated hot items for category: {category1}, items: {num_items}")

def generate_user_recent_click(redis_client, total_users, total_items):
    """Generate user recent click items lists"""
    print("Generating user recent click items...")
    item_ids = list(range(0, total_items))

    for start_id in range(0, total_users, BATCH_SIZE):
        end_id = min(start_id + BATCH_SIZE - 1, total_users - 1)
        pipeline = redis_client.pipeline()

        for user_id in range(start_id, end_id + 1):
            clicked_items = random.sample(item_ids, 100)
            base_time = int(time.time() * 1000)  # Current time in milliseconds
            day_ms = 24 * 60 * 60 * 1000

            for item_id in clicked_items:
                # Random time within last 30 days
                bhv_time = base_time - random.randint(0, 30 * day_ms)
                click_data = json.dumps({"user_id": user_id, "item_id": item_id, "bhv_time": bhv_time})
                # Add default: prefix to the key
                pipeline.lpush(f"default:user_recent_click_item:{user_id}", click_data)

        pipeline.execute()
        print(f"Processed users {start_id}-{end_id} for recent clicks")

def generate_itemcf_i2i(redis_client, total_items):
    """Generate item-based collaborative filtering similarity lists"""
    print("Generating item-to-item similarity data...")

    all_items = list(range(0, total_items))

    for start_id in range(0, total_items, BATCH_SIZE):
        end_id = min(start_id + BATCH_SIZE - 1, total_items - 1)
        pipeline = redis_client.pipeline()

        for item_id1 in range(start_id, end_id + 1):
            item_id2_list = random.sample(all_items, 100)
            for item_id2 in item_id2_list:
                score = round(random.uniform(0.1, 1.0), 4)  # Similarity score between 0-1
                sim_data = json.dumps({"item_id1": item_id1, "item_id2": item_id2, "score": score})
                # Add default: prefix to the key
                pipeline.lpush(f"default:itemcf_i2i:{item_id1}", sim_data)

        pipeline.execute()
        print(f"Processed items {start_id}-{end_id} for item similarity")

def generate_behavior_sample_parquet(output_dir, sample_size=1000):
    """Generate behavior sample data as parquet file"""
    print(f"\nGenerating {sample_size} behavior sample records as parquet...")

    data = []
    base_time = int(time.time() * 1000)
    day_ms = 24 * 60 * 60 * 1000

    for i in range(sample_size):
        user_id = random.randint(0, TOTAL_USERS - 1)
        item_id = random.randint(0, TOTAL_ITEMS - 1)

        data.append({
            'user_id': user_id,
            'user_name': f"User{user_id}",
            'user_country': random.choice(COUNTRIES),
            'user_age': random.randint(18, 80),
            'user_os': random.choice(OS_LIST),
            'user_network': random.choice(NETWORK_LIST),
            'item_id': item_id,
            'item_name': f"Item{item_id}",
            'item_brand': random.choice(BRANDS),
            'item_category1': random.choice(CATEGORIES1),
            'item_category2': random.choice(CATEGORIES2[random.choice(CATEGORIES1)]),
            'item_category3': f"Sub1",
            'item_category4': f"Sub2",
            'bhv_time': base_time - random.randint(0, 30 * day_ms),
            'is_click': random.randint(0, 1),
        })

    df = pd.DataFrame(data)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, 'behavior_sample.parquet')

    df['user_age'] = df['user_age'].astype('int32')
    df['is_click'] = df['is_click'].astype('float32')
    df.to_parquet(output_path, index=False)

    print(f"Behavior sample parquet generated: {output_path}")
    print(f"Total records: {len(df)}")
    return output_path

def main():
    print("=== Redis Data Generation for SQLRec Benchmark ===")
    print(f"Configuration: {TOTAL_USERS} users, {TOTAL_ITEMS} items")

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

    generate_user_table(redis_client, TOTAL_USERS)  # For user_table
    generate_item_table(redis_client, TOTAL_ITEMS)  # For item_table
    generate_global_hot_items(redis_client, TOTAL_ITEMS)
    generate_user_interest_category1(redis_client, TOTAL_USERS)  # Limit to 100k users for interest data
    generate_category1_hot_items(redis_client, TOTAL_ITEMS)
    generate_user_recent_click(redis_client, TOTAL_USERS, TOTAL_ITEMS)  # Limit to 100k users
    generate_itemcf_i2i(redis_client, TOTAL_ITEMS)

    parquet_output_dir = os.path.dirname(os.path.realpath(__file__))
    generate_behavior_sample_parquet(parquet_output_dir, 10000)

    print("\n=== All Redis tables data generation completed successfully! ===")

if __name__ == "__main__":
    main()