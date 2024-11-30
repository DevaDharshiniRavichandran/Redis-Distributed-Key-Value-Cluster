import threading
import time
import requests
import random

SERVER_URL = "http://0.0.0.0:8000"

# Shared statistics for latency tracking
set_key_stats = []
get_key_stats = []

# Lock for thread-safe statistics updates
stats_lock = threading.Lock()

# Create a session for connection pooling
session = requests.Session()

def set_key(thread_id, key, value):
    """
    Perform a SET request to the server.
    """
    try:
        start_time = time.time()
        response = session.post(f"{SERVER_URL}/set", json={"key": key, "value": value})
        elapsed_time = time.time() - start_time
        with stats_lock:
            set_key_stats.append(elapsed_time)
        if response.status_code == 200:
            try:
                response_json = response.json()
                print(f"Thread-{thread_id} SET {key}: {response_json} | Time Taken: {elapsed_time:.4f}s")
            except ValueError:
                print(f"Thread-{thread_id} SET Error: Invalid JSON Response: {response.text}")
        else:
            print(f"Thread-{thread_id} SET Error: HTTP {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Thread-{thread_id} SET Error for {key}: {e}")

def get_key(thread_id, key):
    """
    Perform a GET request to the server.
    """
    try:
        start_time = time.time()
        response = session.get(f"{SERVER_URL}/get", params={"key": key})
        elapsed_time = time.time() - start_time
        with stats_lock:
            get_key_stats.append(elapsed_time)
        print(f"Thread-{thread_id} GET {key}: {response.json()} | Time Taken: {elapsed_time:.4f}s")
    except Exception as e:
        print(f"Thread-{thread_id} GET Error for {key}: {e}")


def player_thread(thread_id, start_index, end_index):
    """
    Each thread assigns scores to players (SET) and retrieves them (GET) sequentially.
    """
    for player_id in range(start_index, end_index):
        key = f"player_{player_id}"
        value = random.randint(50, 100)

        # Perform a SET request
        set_key(thread_id, key, value)

        # Perform a GET request
        get_key(thread_id, key)

def run_concurrent_threads(num_threads, total_requests):
    """
    Launch concurrent threads where each thread performs sequential SET and GET requests for players.
    """
    threads = []
    requests_per_thread = total_requests // num_threads

    # Launch threads
    for i in range(num_threads):
        start_index = i * requests_per_thread
        end_index = start_index + requests_per_thread
        t = threading.Thread(target=player_thread, args=(i + 1, start_index, end_index))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    print("All threads completed.")

if __name__ == "__main__":
    total_requests = 100_000  # Total number of requests to send
    num_threads = 10  # Number of concurrent threads

    # Run concurrent threads
    run_concurrent_threads(num_threads, total_requests)

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
