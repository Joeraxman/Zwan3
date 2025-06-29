import os
import time
import uuid
import threading
import logging
import signal
from flask import Flask, jsonify
from dotenv import load_dotenv
from azure.data.tables import TableServiceClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from tenacity import retry, stop_after_attempt, wait_exponential
import requests
from waitress import serve
from collections import deque

# Load .env
load_dotenv()
required_env_vars = ['DERIBIT_CLIENT_ID', 'DERIBIT_CLIENT_SECRET', 'AZURE_STORAGE_CONNECTION_STRING']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    logging.critical(f"Missing environment variables: {', '.join(missing_vars)}. Exiting.")
    exit(1)

client_id = os.getenv('DERIBIT_CLIENT_ID')
client_secret = os.getenv('DERIBIT_CLIENT_SECRET')
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')

# Constants
instrument_name = 'BTC_USDC'
spread = 275
amount = 0.0075
max_trade_age_ms = 5 * 60 * 1000
table_name = "ProcessedOrders"

# Globals
access_token = None
access_token_lock = threading.Lock()
recent_trades = deque(maxlen=100)
recent_errors = deque(maxlen=100)
recent_counterorders = deque(maxlen=100)
processed_cache = {}
cache_expiry_secs = 600
data_lock = threading.Lock()
stop_event = threading.Event()

# Flask
app = Flask(__name__)
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Azure
table_client = None
def setup_table():
    global table_client
    try:
        service_client = TableServiceClient.from_connection_string(connection_string)
        try:
            service_client.create_table(table_name)
            logging.info(f"✅ Table '{table_name}' created.")
        except ResourceExistsError:
            logging.info(f"ℹ️ Table '{table_name}' already exists.")
        table_client = service_client.get_table_client(table_name)
    except Exception as e:
        logging.error(f"❌ Azure Table setup failed: {e}")
        raise

def is_order_processed(order_id):
    with data_lock:
        if order_id in processed_cache and time.time() - processed_cache[order_id] < cache_expiry_secs:
            return True
        elif order_id in processed_cache:
            del processed_cache[order_id]

    try:
        table_client.get_entity("ProcessedOrders", order_id)
        with data_lock:
            processed_cache[order_id] = time.time()
        return True
    except ResourceNotFoundError:
        return False
    except Exception as e:
        logging.error(f"❌ Error checking order: {e}")
        return False

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def save_order_to_table(order_id, direction, price, timestamp):
    entity = {
        "PartitionKey": "ProcessedOrders",
        "RowKey": order_id,
        "Direction": direction,
        "Price": price,
        "Timestamp": int(timestamp / 1000)
    }
    try:
        table_client.create_entity(entity=entity)
        with data_lock:
            processed_cache[order_id] = time.time()
        logging.info(f"✅ Order {order_id} saved.")
    except Exception as e:
        logging.error(f"❌ Failed to save order {order_id} to Azure: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def authenticate():
    url = 'https://www.deribit.com/api/v2/public/auth'
    params = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if 'result' in data:
        logging.info("✅ Authenticated.")
        return data['result']['access_token']
    raise Exception("❌ Auth failed")

def token_renewer():
    global access_token
    while not stop_event.wait(timeout=60 * 14):  # elke 14 minuten vernieuwen
        try:
            with access_token_lock:
                access_token = authenticate()
                logging.info("🔁 Token vernieuwd")
        except Exception as e:
            logging.error(f"❌ Token vernieuwing mislukt: {e}")

def get_recent_trades():
    url = 'https://www.deribit.com/api/v2/private/get_user_trades_by_instrument'
    with access_token_lock:
        token = access_token
    headers = {'Authorization': f'Bearer {token}'}
    params = {'instrument_name': instrument_name, 'count': 10, 'include_old': True}
    response = requests.get(url, headers=headers, params=params, timeout=10)
    if response.status_code in [400, 401]:
        raise Exception("Token expired")
    response.raise_for_status()
    return response.json().get('result', {}).get('trades', [])

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
def place_opposite_order(side, price):
    with access_token_lock:
        token = access_token
    url = f'https://www.deribit.com/api/v2/private/{side}'
    headers = {'Authorization': f'Bearer {token}'}
    params = {
        'instrument_name': instrument_name,
        'amount': amount,
        'type': 'limit',
        'price': round(price, 2),
        'post_only': 'true'
    }
    response = requests.get(url, headers=headers, params=params, timeout=10)
    if response.status_code == 401:
        raise Exception("Token expired")
    data = response.json()
    if 'result' in data:
        logging.info(f"✅ Counterorder geplaatst: {side} @ {price}")
        return True
    logging.error(f"❌ Fout bij order: {data}")
    return False

def trading_loop():
    global access_token
    logging.info("🚀 Trading loop gestart.")
    with access_token_lock:
        access_token = authenticate()

    while not stop_event.wait(timeout=5):
        try:
            trades = get_recent_trades()
            now_ms = int(time.time() * 1000)
            processed_this_loop = set()

            for trade in trades:
                order_id = str(trade['order_id'])
                price = float(trade['price'])
                direction = trade['direction']
                timestamp = int(trade['timestamp'])

                if order_id in processed_this_loop:
                    logging.info(f"⏩ Skipping {order_id}: already processed this loop.")
                    continue

                if is_order_processed(order_id):
                    logging.info(f"⏩ Skipping {order_id}: already processed in Azure.")
                    processed_this_loop.add(order_id)
                    continue

                if now_ms - timestamp > max_trade_age_ms:
                    logging.info(f"⏩ Skipping {order_id}: trade too old.")
                    processed_this_loop.add(order_id)
                    continue

                logging.info(f"➡️ Saving order {order_id} to Azure...")
                try:
                    save_order_to_table(order_id, direction, price, timestamp)
                    processed_this_loop.add(order_id)
                except Exception as e:
                    logging.error(f"❌ Failed to save order {order_id}: {e}")
                    continue

                with data_lock:
                    recent_trades.append({
                        "order_id": order_id,
                        "direction": direction,
                        "price": price,
                        "timestamp": timestamp,
                        "quantity": trade.get('amount', 0),
                        "pair": instrument_name
                    })

                if direction == 'buy':
                    new_price = price + spread
                    new_side = 'sell'
                elif direction == 'sell':
                    new_price = price - spread
                    new_side = 'buy'
                else:
                    continue

                if place_opposite_order(new_side, new_price):
                    with data_lock:
                        recent_counterorders.append({
                            "order_id": order_id,
                            "side": new_side,
                            "price": new_price
                        })

        except Exception as e:
            msg = str(e)
            logging.warning(f"⚠️ Fout: {msg}")
            with data_lock:
                recent_errors.append({"error": msg, "timestamp": int(time.time())})
            if 'Token expired' in msg:
                with access_token_lock:
                    access_token = authenticate()

# Flask routes
@app.route('/')
def home():
    return "Trading bot is running."

@app.route('/health')
def health():
    return jsonify({"status": "ok"})

@app.route('/trades')
def trades():
    with data_lock:
        buy_orders = [t for t in recent_trades if t['direction'] == 'buy']
        sell_orders = [t for t in recent_trades if t['direction'] == 'sell']
        buy_qty = sum(t['quantity'] for t in buy_orders)
        sell_qty = sum(t['quantity'] for t in sell_orders)
        buy_avg = sum(t['price'] * t['quantity'] for t in buy_orders) / buy_qty if buy_qty else 0
        sell_avg = sum(t['price'] * t['quantity'] for t in sell_orders) / sell_qty if sell_qty else 0
        return jsonify({
            "buy": {"total_quantity": buy_qty, "avg_price": round(buy_avg, 2), "orders": buy_orders},
            "sell": {"total_quantity": sell_qty, "avg_price": round(sell_avg, 2), "orders": sell_orders}
        })

@app.route('/counterorders')
def counterorders():
    with data_lock:
        return jsonify({"recent_counterorders": list(recent_counterorders)[-10:]})

@app.route('/errors')
def errors():
    with data_lock:
        return jsonify({"recent_errors": list(recent_errors)[-10:]})

@app.route('/shutdown', methods=['POST'])
def shutdown():
    logging.info("🛑 Shutdown aangevraagd.")
    stop_event.set()
    return jsonify({"status": "shutdown initiated"})

def handle_shutdown(signum, frame):
    logging.info("🛑 Ontvangen shutdown signaal.")
    stop_event.set()

signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    setup_table()

    threading.Thread(target=trading_loop, daemon=True).start()
    threading.Thread(target=token_renewer, daemon=True).start()

    port = int(os.getenv('PORT', 8000))
    serve(app, host='0.0.0.0', port=port, threads=10, connection_limit=100)
