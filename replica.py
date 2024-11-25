# Args - port, master/slave, masterReplicaPort

from flask import Flask, request, jsonify
from collections import defaultdict
import threading, sys, copy, time

SLAVE_REPLICA_CMD_ARG = "slave"

# In-memory thread-safe key-value store
class DataStore:
    def __init__(self):
        self._store = defaultdict(lambda: None)
        self._lock = threading.Lock()
    
    def set(self, key, value):
        with self._lock:
            self._dict[key] = value
        
    def get(self, key):
        with self._lock:
            return self._dict.get(key)
    
    def getFullStore(self):
        with self._lock:
            return copy.deepcopy(self._store)
    
    def setFullStore(self, updatedStore):
        with self._lock:
            self._store = updatedStore
        
dataStore = DataStore()

app = Flask(__name__)
    
@app.route('/get', methods=['GET'])
def get_value():
    # TODO - Use DataStore
    return jsonify({'message': "Route incomplete"}), 500

    # key = request.args.get('key')
    # if key in store:
    #     return jsonify({'key': key, 'value': store[key]}), 200
    # else:
    #     return jsonify({'error': f'Key "{key}" not found'}), 404

@app.route('/set', methods=['POST'])
def set_value():
    # TODO - Use DataStore
    return jsonify({'message': "Route incomplete"}), 500

    # data = request.json
    # if not data or 'key' not in data or 'value' not in data:
    #     return jsonify({'error': 'Invalid payload, "key" and "value" are required'}), 400
    # store[data['key']] = data['value']
    # return jsonify({'message': f'Key "{data["key"]}" set successfully'}), 200

@app.route('/getFullStore', methods=['GET'])
def get_full_store():
    # TODO - Implement logic to leverage DataStore.getFullStore()
    return jsonify({'message': "Route incomplete"}), 500

def set_up_sync_thread(masterReplicaPort):
    def syncTask():
        print("\nSync thread on slave port " + sys.argv[1] + " polling master port " + masterReplicaPort + "\n")
        while(True):
            # TODO - If this is a slave replica, update your store with parent every 5sec leveraging DataStore.setFullStore()
            time.sleep(5)
    syncThread = threading.Thread(target=syncTask, daemon=True)
    syncThread.start()

if __name__ == '__main__':
    if(sys.argv[2] == SLAVE_REPLICA_CMD_ARG):
        set_up_sync_thread(sys.argv[3])        
    
    app.run(host='0.0.0.0', port=sys.argv[1], threaded=True)