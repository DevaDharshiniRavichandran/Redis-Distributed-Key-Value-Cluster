from flask import Flask, request, jsonify
import threading
import requests
import time
import os
import random

app = Flask(__name__)

# Partition configuration
partitions_cases = {
    'case3': {
        1: ["http://0.0.0.0:5001", "http://0.0.0.0:5002", "http://0.0.0.0:5003"],  # Partition 1 replicas
        2: ["http://0.0.0.0:5004", "http://0.0.0.0:5005", "http://0.0.0.0:5006"],  # Partition 2 replicas
        3: ["http://0.0.0.0:5007", "http://0.0.0.0:5008", "http://0.0.0.0:5009"]
    },
    'case2': {
        1: ["http://0.0.0.0:5001"]
    },
    'case1': {
        1: ["http://0.0.0.0:5001"],
        2: ["http://0.0.0.0:5002"],
        3: ["http://0.0.0.0:5003"]
    }
}

# Health status for replicas (default: all healthy)
partitions = partitions_cases[os.environ.get('case')]
case = os.environ.get('case')
eventual = 1
if os.environ.get('eventual'):
    eventual = os.environ.get('eventual')
print("Choosing case: ", partitions)
replica_health = {replica: True for partition in partitions.values() for replica in partition}

# Track query execution times and key counts
query_times = {'get': [], 'set': []}
key_counts = {1: 0, 2: 0, 3: 0}

# Utility function to contact a replica
def contact_replica(url, method, data=None):
    try:
        if method == "GET":
            response = requests.get(url, params=data, timeout=2)
        elif method == "POST":
            response = requests.post(url, json=data, timeout=2)
        else:
            return None, "Invalid method"
        return response.json(), None
    except Exception as e:
        return None, str(e)

# Periodic health check for replicas
def health_check():
    while True:
        for replica in replica_health.keys():
            try:
                response = requests.get(f"{replica}/getFullStore", timeout=2)
                replica_health[replica] = response.status_code == 200
            except Exception:
                replica_health[replica] = False
        time.sleep(5)  # Check every 5 seconds

# Forward client requests to appropriate partition
@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get('key')
    partition_id = hash(key) % len(partitions) + 1
    replicas = partitions[partition_id]
    start_time = time.time()
    what_replica = 0 if case in ['case2', 'case1'] else random.randint(0, len(replicas)-1)
    responses = []
    errors = []
    # Fetch from the first available replica
    replica = replicas[what_replica]
    response, error = contact_replica(f"{replica}/get", "GET", {"key": key})
    if error:
        errors.append(error)
    responses.append(response)

    elapsed_time = time.time() - start_time
    query_times['get'].append(elapsed_time)

    if responses:
        return jsonify({'partition': partition_id, 'data': responses[0]}), 200
    else:
        return jsonify({'error': f"Failed to fetch key from partition {partition_id}", 'details': errors}), 500

@app.route('/set', methods=['POST'])
def set_value():
    data = request.json
    key = data.get('key')
    value = data.get('value')

    if not key or value is None:
        return jsonify({'error': 'Invalid payload, "key" and "value" are required'}), 400

    partition_id = hash(key) % len(partitions) + 1
    replicas = partitions[partition_id]
    start_time = time.time()

    # Write to all healthy replicas
    errors = []
    for replica in replicas:
        if replica_health[replica]:
            _, error = contact_replica(f"{replica}/set", "POST", {"key": key, "value": value})
            if error:
                errors.append(error)
        if eventual:
            break
        else:
            pass

    if len(errors) < len(replicas):  # At least one replica succeeded
        key_counts[partition_id] += 1
        elapsed_time = time.time() - start_time
        query_times['set'].append(elapsed_time)
        return jsonify({'message': f'Key "{key}" set successfully in partition {partition_id}'}), 200
    else:
        return jsonify({'error': f'Failed to set key in partition {partition_id}', 'details': errors}), 500

# Helper function to calculate the data spread ratio
def calculate_data_spread_ratio():
    total_keys = sum(key_counts.values())
    if total_keys == 0:
        return {partition_id: 0 for partition_id in key_counts}  # Avoid division by zero

    spread_ratio = {}
    for partition_id, count in key_counts.items():
        spread_ratio[partition_id] = count / total_keys

    return spread_ratio

@app.route('/stats', methods=['GET'])
def get_stats():
    # Calculate the data spread ratio
    spread_ratio = calculate_data_spread_ratio()

    # Calculate average query execution times for set and get operations
    avg_set_time = sum(query_times['set']) / len(query_times['set']) if query_times['set'] else 0
    avg_get_time = sum(query_times['get']) / len(query_times['get']) if query_times['get'] else 0

    # Return stats
    return jsonify({
        'data_spread_ratio': spread_ratio,
        'avg_set_query_time': avg_set_time,
        'avg_get_query_time': avg_get_time
    }), 200

if __name__ == '__main__':

    # Start health check thread
    health_thread = threading.Thread(target=health_check, daemon=True)
    health_thread.start()

    app.run(host='0.0.0.0', port=8000)
