# Args - port, master/slave, masterReplicaPort

from flask import Flask, request, jsonify
from collections import defaultdict
import threading, sys, copy, time
import requests

SLAVE_REPLICA_CMD_ARG = "slave"

# In-memory thread-safe key-value store with version tracking
class DataStore:
    def __init__(self):
        self._store = defaultdict(lambda: None)  # {key: (value, version)}
        self._lock = threading.Lock()
        self._version = defaultdict(int)  # Version tracking for each key
    
    def set(self, key, value):
        with self._lock:
            self._store[key] = value
            self._version[key] += 1  # Increment version
    
    def get(self, key):
        with self._lock:
            return self._store.get(key)
    
    def get_version(self, key):
        with self._lock:
            return self._version.get(key, 0)
    
    def get_full_store(self):
        with self._lock:
            return copy.deepcopy(self._store)
    
    def set_full_store(self, updated_store):
        with self._lock:
            self._store = updated_store
    
    def apply_updates(self, updates):
        """Apply updates from master based on version comparison."""
        with self._lock:
            for key, (value, version) in updates.items():
                if version > self._version[key]:
                    self._store[key] = value
                    self._version[key] = version

dataStore = DataStore()

# Write-ahead log (WAL) for tracking updates
write_ahead_log = []

app = Flask(__name__)

@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get('key')
    value = dataStore.get(key)
    if value is not None:
        return jsonify({'key': key, 'value': value}), 200
    else:
        return jsonify({'error': f'Key "{key}" not found'}), 404

@app.route('/set', methods=['POST'])
def set_value():
    data = request.json
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({'error': 'Invalid payload, "key" and "value" are required'}), 400
    
    key = data['key']
    value = data['value']
    
    # Update the store and increment version
    dataStore.set(key, value)
    
    # Log the update
    version = dataStore.get_version(key)
    write_ahead_log.append({key: (value, version)})
    
    return jsonify({'message': f'Key "{key}" set successfully', 'version': version}), 200

@app.route('/getFullStore', methods=['GET'])
def get_full_store():
    return jsonify(dataStore.get_full_store()), 200

@app.route('/getUpdates', methods=['GET'])
def get_updates():
    """Send updates from the WAL since the last request."""
    last_version = int(request.args.get('last_version', 0))
    updates = {}
    for entry in write_ahead_log:
        for key, (value, version) in entry.items():
            if version > last_version:
                updates[key] = (value, version)
    return jsonify(updates), 200

def set_up_sync_thread(masterReplicaPort):
    def sync_task():
        print(f"\nSync thread on slave port {sys.argv[1]} polling master port {masterReplicaPort}\n")
        master_url_updates = f"http://127.0.0.1:{masterReplicaPort}/getUpdates"
        last_version = 0
        
        while True:
            # Pull updates from master
            try:
                response = requests.get(master_url_updates, params={'last_version': last_version})
                if response.status_code == 200:
                    updates = response.json()
                    if updates:
                        dataStore.apply_updates(updates)
                        # Update last_version to the highest version received
                        last_version = max(v for _, v in updates.values())
            except Exception as e:
                print(f"Error during sync: {e}")
            
            time.sleep(5)
    
    sync_thread = threading.Thread(target=sync_task, daemon=True)
    sync_thread.start()

if __name__ == '__main__':
    if sys.argv[2] == SLAVE_REPLICA_CMD_ARG:
        set_up_sync_thread(sys.argv[3])
    
    app.run(host='0.0.0.0', port=sys.argv[1], threaded=True)
