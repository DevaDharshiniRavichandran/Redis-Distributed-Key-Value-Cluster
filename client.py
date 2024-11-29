import threading
import time
import requests
import random

SERVER_URL = "http://127.0.0.1:8000"

# Shared statistics for latency tracking
set_key_stats = []
get_key_stats = []

# Lock for thread-safe statistics updates
stats_lock = threading.Lock()

def set_key(thread_id, key, value):
    """
    Perform a SET request to the server.
    """
    try:
        start_time = time.time()
        response = requests.post(f"{SERVER_URL}/set", json={"key": key, "value": value})
        elapsed_time = time.time() - start_time
        with stats_lock:
            set_key_stats.append(elapsed_time)
        print(f"Thread-{thread_id} SET: {response.json()} | Time Taken: {elapsed_time:.4f}s")
    except Exception as e:
        print(f"Thread-{thread_id} SET Error: {e}")

def get_key(thread_id, key):
    """
    Perform a GET request to the server.
    """
    try:
        start_time = time.time()
        response = requests.get(f"{SERVER_URL}/get", params={"key": key})
        elapsed_time = time.time() - start_time
        with stats_lock:
            get_key_stats.append(elapsed_time)
        print(f"Thread-{thread_id} GET: {response.json()} | Time Taken: {elapsed_time:.4f}s")
    except Exception as e:
        print(f"Thread-{thread_id} GET Error: {e}")

def run_concurrent_requests(num_threads):
    """
    Launch concurrent SET and GET requests using threading.
    """
    threads = []

    # Launch threads for SET requests
    for i in range(num_threads):
        key = f"gamerHandle_{i}"
        value = random.randint(50, 100)
        t = threading.Thread(target=set_key, args=(i + 1, key, value))
        threads.append(t)
        t.start()

    # Wait for all SET threads to finish
    for t in threads:
        t.join()

    print("All SET requests completed.")
    threads = []

    # Launch threads for GET requests
    for i in range(num_threads):
        key = f"gamerHandle_{i}"
        t = threading.Thread(target=get_key, args=(i + 1, key))
        threads.append(t)
        t.start()

    # Wait for all GET threads to finish
    for t in threads:
        t.join()

    print("All GET requests completed.")

if __name__ == "__main__":
    num_threads = 100  # Number of threads for concurrent requests

    # Run concurrent requests
    run_concurrent_requests(num_threads)

    # Display average latencies
    print("------------------------------------------------")
    print(f"Avg. latency for SET requests: {sum(set_key_stats)/len(set_key_stats):.4f}s")
    print(f"Avg. latency for GET requests: {sum(get_key_stats)/len(get_key_stats):.4f}s")
    print("------------------------------------------------")

    # Fetch stats from the load balancer
    try:
        response_stats = requests.get(f"{SERVER_URL}/stats")
        print("------------------------------------------------")
        print("Stats from the Load Balancer:", response_stats.json())
    except Exception as e:
        print("Error fetching stats from Load Balancer:", e)
