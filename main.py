from fastapi import FastAPI, HTTPException
import redis
import os
import json
import asyncio

# Configuration
MAX_CLIENTS_PER_ITEM = 2  # Each item serves 4 clients before rotating
USAGE_FILE = "item_usage.txt"  # File to store usage data persistently

# Initialize FastAPI app
app = FastAPI()

# Connect to Redis using Railway environment variables
redis_client = redis.StrictRedis(
    host=os.getenv("REDISHOST", "localhost"),
    port=int(os.getenv("REDISPORT", 6379)),
    password=os.getenv("REDISPASSWORD", None),
    username=os.getenv("REDISUSER", None),
    decode_responses=True
)

# Load saved usage data
def load_usage_data():
    if os.path.exists(USAGE_FILE):
        try:
            with open(USAGE_FILE, "r") as file:
                return json.load(file)
        except json.JSONDecodeError:
            print("Error loading usage data. Resetting...")
    return {}

# Save usage data
def save_usage_data():
    usage_data = redis_client.hgetall("item_usage")
    with open(USAGE_FILE, "w") as file:
        json.dump(usage_data, file)
    print("Usage data saved.")

# Initialize Redis data with items from links.txt
def initialize_items(force_reset=False):
    """
    Initialize the Redis database with items from links.txt.
    :param force_reset: If True, clears existing data in Redis before initialization.
    """
    if force_reset:
        redis_client.delete("items")
        redis_client.delete("item_usage")
        print("Redis data cleared.")

    if not redis_client.exists("items"):
        try:
            with open('links.txt', 'r') as file:
                items = [line.strip() for line in file if line.strip()]
            if not items:
                raise ValueError("No items found in links.txt")
            
            redis_client.rpush("items", *items)

            # Load saved usage data
            saved_usage = load_usage_data()
            for item in items:
                usage_count = int(saved_usage.get(item, 0))
                redis_client.hset("item_usage", item, usage_count)
            
            print(f"Loaded {len(items)} items into Redis.")
        except FileNotFoundError:
            print("Error: links.txt file not found. Please create the file and add items.")
        except ValueError as e:
            print(f"Error: {e}")

def reset_item_usage():
    """
    Resets the usage count for all items and reloads them into Redis.
    """
    print("Resetting item usage counts and reloading items...")
    redis_client.delete("items")
    
    try:
        with open('links.txt', 'r') as file:
            items = [line.strip() for line in file if line.strip()]
        
        redis_client.rpush("items", *items)
        for item in items:
            redis_client.hset("item_usage", item, 0)
        print("Items and usage counts have been reset.")
    except FileNotFoundError:
        print("Error: links.txt file not found during reset.")

# Endpoint to get an item
@app.get("/get_item")
async def get_item():
    """
    Assigns an item to the client, ensuring no item is overused.
    When all items reach their usage limit, the server resets and starts over.
    """
    async with asyncio.Lock():
        while True:
            item = redis_client.lindex("items", 0)  # Peek at the first item
            if item:
                usage = int(redis_client.hget("item_usage", item) or 0)
                if usage < MAX_CLIENTS_PER_ITEM:
                    redis_client.hincrby("item_usage", item, 1)
                    save_usage_data()  # Save usage data after each request
                    return {"assigned_item": item, "current_usage": usage + 1}
                else:
                    redis_client.lpop("items")  # Remove item if usage limit is reached
            else:
                # All items exhausted, reset usage and restart
                reset_item_usage()

# Initialize items with a forced reset on startup
initialize_items(force_reset=True)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))  # Use Railway's PORT or default to 8000
    uvicorn.run(app, host="0.0.0.0", port=port)
