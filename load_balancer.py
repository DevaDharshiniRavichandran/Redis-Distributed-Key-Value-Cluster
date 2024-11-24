from flask import Flask, request, jsonify
import threading
import requests

app = Flask(__name__)

# Partition configuration
partitions = {
    1: ["http://0.0.0.0:5001", "http://0.0.0.0:5002"],  # Partition 1 replicas
    2: ["http://0.0.0.0:5003", "http://0.0.0.0:5004"],  # Partition 2 replicas
}

# Utility function to contact a replica
def contact_replica(url, method, data=None):
    try:
        if method == "GET":
            response = requests.get(url, params=data)
        elif method == "POST":
            response = requests.post(url, json=data)
        else:
            return None, "Invalid method"
        return response.json(), None
    except Exception as e:
        return None, str(e)

# Forward client requests to appropriate partition
@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get('key')
    partition_id = hash(key) % len(partitions) + 1
    replicas = partitions[partition_id]

    # Fetch from all replicas for consistency
    responses = []
    errors = []
    for replica in replicas:
        response, error = contact_replica(f"{replica}/get", "GET", {"key": key})
        if error:
            errors.append(error)
        else:
            responses.append(response)

    if responses:
        # Return the first response assuming all replicas are consistent
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

    # Write to all replicas for consistency
    errors = []
    for replica in replicas:
        _, error = contact_replica(f"{replica}/set", "POST", {"key": key, "value": value})
        if error:
            errors.append(error)
        break

    if not errors:
        return jsonify({'message': f'Key "{key}" set successfully in partition {partition_id}'}), 200
    else:
        return jsonify({'error': f'Failed to set key in partition {partition_id}', 'details': errors}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
