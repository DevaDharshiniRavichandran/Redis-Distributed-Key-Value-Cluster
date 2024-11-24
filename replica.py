from flask import Flask, request, jsonify

app = Flask(__name__)

# In-memory key-value store
store = {}

@app.route('/get', methods=['GET'])
def get_value():
    key = request.args.get('key')
    if key in store:
        return jsonify({'key': key, 'value': store[key]}), 200
    else:
        return jsonify({'error': f'Key "{key}" not found'}), 404

@app.route('/set', methods=['POST'])
def set_value():
    data = request.json
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({'error': 'Invalid payload, "key" and "value" are required'}), 400
    store[data['key']] = data['value']
    return jsonify({'message': f'Key "{data["key"]}" set successfully'}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=sys.argv[1])  # Change port for each replica
