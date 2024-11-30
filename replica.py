# Args - port, master/slave, masterReplicaPort

from flask import Flask, request, jsonify
from collections import defaultdict
import threading, sys, copy, time, requests

SLAVE_REPLICA_CMD_ARG = "slave"

# In-memory thread-safe key-value store
class DataStore:
    def __init__(self):
        self._store = defaultdict(lambda: None)
        self._lock = threading.Lock()
    
    def set(self, key, value):
        with self._lock:
            self._store[key] = value
        
    def get(self, key):
        with self._lock:
            return self._store.get(key)
    
    def getFullStore(self):
        with self._lock:
            return copy.deepcopy(self._store)
    
    def setFullStore(self, updatedStore):
        with self._lock, open('sync' + sys.argv[1] + ".txt", 'w') as syncFile:
            syncFile.write(str(self._store))
            syncFile.write("\n\n\n")
            self._store = updatedStore
            syncFile.write(str(self._store))
        
dataStore = DataStore()

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
    
    dataStore.set(data['key'], data['value'])
    return jsonify({'message': f'Key "{data["key"]}" set successfully'}), 200


@app.route('/getFullStore', methods=['GET'])
def get_full_store():
    return jsonify(dataStore.getFullStore()), 200

def set_up_sync_thread():
    masterReplicaPort = sys.argv[3]
    def syncTask():
        print("\nSync thread on slave port " + sys.argv[1] + " polling master port " + masterReplicaPort + "\n")
        master_url = f"http://127.0.0.1:{masterReplicaPort}/getFullStore"
        while True:
            try:
                response = requests.get(master_url)
                if response.status_code == 200:
                    updated_store = response.json()
                    dataStore.setFullStore(updated_store)
            except Exception as e:
                print(f"Error during sync: {e}")
            time.sleep(5)
    syncThread = threading.Thread(target=syncTask, daemon=True)
    syncThread.start()


if __name__ == '__main__':
    if(sys.argv[2] == SLAVE_REPLICA_CMD_ARG):
        set_up_sync_thread()        
    
    app.run(host='0.0.0.0', port=sys.argv[1], threaded=True)