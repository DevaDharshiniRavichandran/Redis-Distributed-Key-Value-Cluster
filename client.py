import threading
import time
import requests

SERVER_URL = "http://127.0.0.1:8000"

def set_key(key, value):
    start_time = time.time()
    response = requests.post(f"{SERVER_URL}/set", json={"key": key, "value": value})
    elapsed_time = time.time() - start_time
    print(f"SET: {response.json()} | Time Taken: {elapsed_time:.4f}s")

def get_key(key):
    start_time = time.time()
    response = requests.get(f"{SERVER_URL}/get", params={"key": key})
    elapsed_time = time.time() - start_time
    print(f"GET: {response.json()} | Time Taken: {elapsed_time:.4f}s")

if __name__ == "__main__":
    threads = []
    num_threads = 10

    # Launch threads for setting keys
    for i in range(num_threads):
        t = threading.Thread(target=set_key, args=(f"key{i}", f"value{i}"))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()

    threads = []

    # Launch threads for getting keys
    for i in range(num_threads):
        t = threading.Thread(target=get_key, args=(f"key{i}",))
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for t in threads:
        t.join()
