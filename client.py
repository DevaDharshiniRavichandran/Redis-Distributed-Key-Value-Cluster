import threading
import time
import requests
import random

SERVER_URL = "http://127.0.0.1:8000"

set_key_stats = []
get_key_stats = []
def set_key(key, value):
    start_time = time.time()
    response = requests.post(f"{SERVER_URL}/set", json={"key": key, "value": value})
    elapsed_time = time.time() - start_time
    set_key_stats.append(elapsed_time)
    print(f"SET: {response.json()} | Time Taken: {elapsed_time:.4f}s")

def get_key(key):
    start_time = time.time()
    response = requests.get(f"{SERVER_URL}/get", params={"key": key})
    elapsed_time = time.time() - start_time
    get_key_stats.append(elapsed_time)
    #print(f"GET: {response.json()} | Time Taken: {elapsed_time:.4f}s")

if __name__ == "__main__":
    threads = []
    num_threads = 100

    # Launch threads for setting keys
    for i in range(num_threads):
        t = threading.Thread(target=set_key, args=(f"gamerHandle_{i}", f"{random.randint(50,100)}"))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    threads = []

    # Launch threads for getting keys
    for i in range(num_threads):
        t = threading.Thread(target=get_key, args=(f"gamerHandle_{i}",))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    print("------------------------------------------------")
    print(f"Avg. latency for SET request: {sum(set_key_stats)/len(set_key_stats)}")
    print(f"Avg. latency for GET request: {sum(get_key_stats)/len(get_key_stats)}")
    print("------------------------------------------------")

    response_stats = requests.get(f"{SERVER_URL}/stats")
    print("------------------------------------------------")
    print("Stats from the Load Balancer: ", response_stats.content)
