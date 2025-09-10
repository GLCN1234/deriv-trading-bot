import asyncio
import websockets
import json
import pandas as pd
import numpy as np
import os
from datetime import datetime
from dotenv import load_dotenv
from collections import deque
import time
import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
import signal
import pickle
import traceback

load_dotenv()

app = FastAPI()
ws = None
candles_data = {}
final_signals = {}
balance = 9946.63
symbols = ["frxEURUSD", "frxGBPUSD", "frxUSDJPY", "frxXAUUSD", "frxUSDCAD", "1HZ10V", "1HZ25V", "1HZ75V", "1HZ90V", "1HZ100V", "R_75", "R_100"]
active_symbols_data = []
subscription_ids = {}
app_id = os.getenv("DERIV_APP_ID", "1089")
api_token = os.getenv("DERIV_API_TOKEN", "NLhZn6KsIamjQlF")
message_queue = deque()
response_callbacks = {}
request_counter = 0
last_request_time = 0
RATE_LIMIT_DELAY = 0.5
bot_running = False
auto_trading = True
emergency_stop = False
shutdown_triggered = False
valid_durations = {}
DEBUG_MODE = False

def save_state():
    try:
        with open("candles_data.pkl", "wb") as f:
            pickle.dump(candles_data, f)
        with open("valid_durations.pkl", "wb") as f:
            pickle.dump(valid_durations, f)
        print(f"[{datetime.now()}] State saved to candles_data.pkl and valid_durations.pkl")
    except Exception as e:
        print(f"[{datetime.now()}] Error saving state: {e}")

def load_state():
    global candles_data, valid_durations
    try:
        if os.path.exists("candles_data.pkl"):
            with open("candles_data.pkl", "rb") as f:
                candles_data = pickle.load(f)
            print(f"[{datetime.now()}] Loaded candles_data from candles_data.pkl")
        if os.path.exists("valid_durations.pkl"):
            with open("valid_durations.pkl", "rb") as f:
                valid_durations = pickle.load(f)
            print(f"[{datetime.now()}] Loaded valid_durations from valid_durations.pkl")
    except Exception as e:
        print(f"[{datetime.now()}] Error loading state: {e}")

def signal_handler(sig, frame):
    global bot_running, auto_trading, emergency_stop, ws, shutdown_triggered
    if shutdown_triggered:
        return
    shutdown_triggered = True
    print(f"[{datetime.now()}] Received interrupt signal, shutting down...")
    bot_running = False
    auto_trading = False
    emergency_stop = True
    save_state()
    if ws and not ws.closed:
        asyncio.create_task(ws.close())
        print(f"[{datetime.now()}] WebSocket close scheduled")
    if os.path.exists("trades.csv"):
        print(f"[{datetime.now()}] Trades saved in trades.csv")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def send_with_rate_limit(message):
    global last_request_time
    current_time = time.time()
    elapsed = current_time - last_request_time
    if elapsed < RATE_LIMIT_DELAY:
        await asyncio.sleep(RATE_LIMIT_DELAY - elapsed)
    if ws and not ws.closed:
        await ws.send(json.dumps(message))
        last_request_time = time.time()
    else:
        print(f"[{datetime.now()}] WebSocket closed, cannot send message: {json.dumps(message, indent=2)}")

async def fetch_contract_info(symbol):
    global ws, request_counter, valid_durations
    if symbol in valid_durations:
        print(f"[{datetime.now()}] Using cached duration for {symbol}: {valid_durations[symbol]}")
        return valid_durations[symbol]
    if symbol in ["R_75", "R_100"]:
        fallback = {"min": 5, "unit": "m"}
        valid_durations[symbol] = fallback
        print(f"[{datetime.now()}] Using hardcoded duration for {symbol}: {fallback}")
        save_state()
        return fallback
    retries = 3
    for attempt in range(retries):
        try:
            if not ws or ws.closed:
                print(f"[{datetime.now()}] WebSocket closed before fetching contract info for {symbol}, reconnecting...")
                await connect_websocket()
            request_counter += 1
            request_id = request_counter
            contract_request = {
                "contracts_for": symbol,
                "req_id": request_id
            }
            future = asyncio.Future()
            response_callbacks[request_id] = future
            await send_with_rate_limit(contract_request)
            print(f"[{datetime.now()}] Sent contract info request for {symbol} (attempt {attempt + 1})")
            response = await asyncio.wait_for(future, timeout=30)
            print(f"[{datetime.now()}] Received contract info response for {symbol}")
            if "error" in response:
                error_msg = f"Contract info error for {symbol}: {response['error']['message']}"
                print(f"[{datetime.now()}] {error_msg}")
                pd.DataFrame([{
                    "symbol": symbol,
                    "signal": "N/A",
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "contract_id": None,
                    "balance_after": balance,
                    "error": error_msg
                }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                if attempt < retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue
            contracts = response.get("contracts_for", {}).get("available", [])
            for contract in contracts:
                if contract["contract_type"] in ["CALL", "PUT"]:
                    durations = contract.get("durations", [])
                    if durations:
                        valid_durations[symbol] = {
                            "min": durations[0]["min"],
                            "unit": durations[0]["unit"]
                        }
                        print(f"[{datetime.now()}] Valid duration for {symbol}: {valid_durations[symbol]}")
                        save_state()
                        return valid_durations[symbol]
            print(f"[{datetime.now()}] No CALL/PUT durations found for {symbol}, using fallback")
            fallback = {"min": 10, "unit": "t"} if symbol.startswith("frx") or "1HZ" in symbol else {"min": 5, "unit": "m"}
            valid_durations[symbol] = fallback
            save_state()
            return fallback
        except asyncio.TimeoutError:
            error_msg = f"Contract info timeout for {symbol} (attempt {attempt + 1})"
            print(f"[{datetime.now()}] {error_msg}")
            pd.DataFrame([{
                "symbol": symbol,
                "signal": "N/A",
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": None,
                "balance_after": balance,
                "error": error_msg
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            if attempt < retries - 1:
                print(f"[{datetime.now()}] Forcing WebSocket reconnect due to timeout")
                await connect_websocket()
                await asyncio.sleep(2 ** attempt)
            continue
        except Exception as e:
            error_msg = f"Contract info exception for {symbol} (attempt {attempt + 1}): {str(e)}\n{traceback.format_exc()}"
            print(f"[{datetime.now()}] {error_msg}")
            pd.DataFrame([{
                "symbol": symbol,
                "signal": "N/A",
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": None,
                "balance_after": balance,
                "error": error_msg
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
            continue
    print(f"[{datetime.now()}] Failed to fetch contract info for {symbol} after {retries} attempts, using fallback")
    fallback = {"min": 10, "unit": "t"} if symbol.startswith("frx") or "1HZ" in symbol else {"min": 5, "unit": "m"}
    valid_durations[symbol] = fallback
    save_state()
    return fallback

async def connect_websocket():
    global ws, candles_data, subscription_ids, request_counter, active_symbols_data
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    retries = 5
    for attempt in range(retries):
        try:
            ws = await websockets.connect(uri, ping_interval=120, ping_timeout=20)
            print(f"[{datetime.now()}] WebSocket opened")
            request_counter += 1
            await send_with_rate_limit({"authorize": api_token, "req_id": request_counter})
            auth_response = await asyncio.wait_for(ws.recv(), timeout=15)
            auth_response = json.loads(auth_response)
            print(f"[{datetime.now()}] Auth response: {json.dumps(auth_response, indent=2)}")
            if "error" in auth_response:
                print(f"[{datetime.now()}] Authorization failed: {auth_response['error']['message']}")
                return False
            if "trade" not in auth_response.get("authorize", {}).get("scopes", []):
                print(f"[{datetime.now()}] Error: API token lacks 'trade' scope")
                return False
            async def keep_alive():
                global request_counter
                while True:
                    try:
                        if ws.closed:
                            print(f"[{datetime.now()}] Keep-alive: WebSocket closed, stopping pings")
                            break
                        request_counter += 1
                        await send_with_rate_limit({"ping": 1, "req_id": request_counter})
                        print(f"[{datetime.now()}] Sent ping")
                        await asyncio.sleep(60)
                    except Exception as e:
                        print(f"[{datetime.now()}] Keep-alive error: {str(e)}\n{traceback.format_exc()}")
                        break
            asyncio.create_task(keep_alive())
            request_counter += 1
            await send_with_rate_limit({"forget_all": "ticks", "req_id": request_counter})
            await asyncio.sleep(1)
            request_counter += 1
            await send_with_rate_limit({"active_symbols": "brief", "req_id": request_counter})
            active_symbols_data = []
            for _ in range(5):
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=15)
                    response = json.loads(response)
                    print(f"[{datetime.now()}] Received active_symbols response: {json.dumps(response, indent=2)}")
                    if "active_symbols" in response:
                        active_symbols_data.extend(response.get("active_symbols", []))
                        print(f"[{datetime.now()}] Active symbols: {[s['symbol'] for s in active_symbols_data]}")
                        break
                except asyncio.TimeoutError:
                    print(f"[{datetime.now()}] Timeout waiting for active_symbols, retrying...")
                    request_counter += 1
                    await send_with_rate_limit({"active_symbols": "brief", "req_id": request_counter})
            else:
                print(f"[{datetime.now()}] Failed to get active_symbols, using fallback symbols")
                active_symbols_data.extend([{"symbol": s, "symbol_type": "forex" if s.startswith("frx") else "stockindex"} for s in symbols])
            for symbol in symbols:
                if symbol not in [s["symbol"] for s in active_symbols_data]:
                    print(f"[{datetime.now()}] Warning: {symbol} not in active symbols, skipping")
                    continue
                if symbol not in candles_data:
                    candles_data[symbol] = []
                symbol_info = next((s for s in active_symbols_data if s["symbol"] == symbol), {})
                granularity = 60 if "1HZ" in symbol else 300
                request_counter += 1
                await send_with_rate_limit({
                    "ticks_history": symbol,
                    "subscribe": 1,
                    "style": "candles",
                    "end": "latest",
                    "count": 50,
                    "granularity": granularity,
                    "req_id": request_counter
                })
                print(f"[{datetime.now()}] Subscribed to candles for {symbol} (granularity: {granularity}s)")
                await fetch_contract_info(symbol)
                await asyncio.sleep(3)
            return True
        except Exception as e:
            print(f"[{datetime.now()}] WebSocket Error (attempt {attempt + 1}/{retries}): {str(e)}\n{traceback.format_exc()}")
            await asyncio.sleep(2 ** attempt)
    print(f"[{datetime.now()}] Failed to connect WebSocket after {retries} attempts")
    return False

async def websocket_consumer():
    global ws, candles_data, subscription_ids, message_queue
    while True:
        try:
            async for message in ws:
                msg = json.loads(message)
                message_queue.append(msg)
                if "error" in msg:
                    error_msg = f"API Error: {msg['error']['message']}, Code: {msg['error'].get('code', 'N/A')}"
                    print(f"[{datetime.now()}] {error_msg}")
                    pd.DataFrame([{
                        "symbol": msg.get("echo_req", {}).get("symbol", "N/A"),
                        "signal": "N/A",
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "contract_id": None,
                        "balance_after": balance,
                        "error": error_msg
                    }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                elif "candles" in msg:
                    symbol = msg["echo_req"]["ticks_history"]
                    candles_data[symbol].extend([{
                        "epoch": c["epoch"],
                        "open": float(c["open"]),
                        "high": float(c["high"]),
                        "low": float(c["low"]),
                        "close": float(c["close"]),
                        "volume": 1
                    } for c in msg["candles"]])
                    print(f"[{datetime.now()}] Received {len(msg['candles'])} candles for {symbol} (Total: {len(candles_data[symbol])})")
                    save_state()
                elif "ohlc" in msg:
                    symbol = msg["ohlc"]["symbol"]
                    candles_data[symbol].append({
                        "epoch": msg["ohlc"]["epoch"],
                        "open": float(msg["ohlc"]["open"]),
                        "high": float(msg["ohlc"]["high"]),
                        "low": float(msg["ohlc"]["low"]),
                        "close": float(msg["ohlc"]["close"]),
                        "volume": 1
                    })
                    print(f"[{datetime.now()}] Received OHLC update for {symbol} (Total: {len(candles_data[symbol])})")
                    save_state()
                elif "subscription" in msg:
                    symbol = msg["echo_req"]["ticks_history"]
                    subscription_ids[symbol] = msg["subscription"]["id"]
                    print(f"[{datetime.now()}] Subscribed to {symbol}: ID {subscription_ids[symbol]}")
                elif "proposal" in msg:
                    request_id = msg.get("req_id")
                    if request_id in response_callbacks:
                        response_callbacks[request_id].set_result(msg)
                        del response_callbacks[request_id]
                        print(f"[{datetime.now()}] Processed proposal response for req_id {request_id}")
                elif "buy" in msg:
                    request_id = msg.get("req_id")
                    if request_id in response_callbacks:
                        response_callbacks[request_id].set_result(msg)
                        del response_callbacks[request_id]
                elif "sell" in msg:
                    request_id = msg.get("req_id")
                    if request_id in response_callbacks:
                        response_callbacks[request_id].set_result(msg)
                        del response_callbacks[request_id]
                elif "balance" in msg:
                    request_id = msg.get("req_id")
                    if request_id in response_callbacks:
                        response_callbacks[request_id].set_result(msg)
                        del response_callbacks[request_id]
                elif "contracts_for" in msg:
                    request_id = msg.get("req_id")
                    if request_id in response_callbacks:
                        response_callbacks[request_id].set_result(msg)
                        del response_callbacks[request_id]
        except Exception as e:
            print(f"[{datetime.now()}] WebSocket consumer error: {str(e)}\n{traceback.format_exc()}")
            save_state()
            await asyncio.sleep(5)
            print(f"[{datetime.now()}] Reconnecting WebSocket...")
            await connect_websocket()

async def fetch_candles(symbol):
    global candles_data
    retries = 5
    for attempt in range(retries):
        try:
            if symbol in candles_data and len(candles_data[symbol]) >= 50:
                df = pd.DataFrame(candles_data[symbol])
                df['time'] = pd.to_datetime(df['epoch'], unit='s')
                print(f"[{datetime.now()}] Fetched {len(df)} candles for {symbol}")
                return df[['time', 'open', 'high', 'low', 'close', 'volume']]
            else:
                print(f"[{datetime.now()}] Not enough candles for {symbol}, waiting...")
                await asyncio.sleep(5)
        except Exception as e:
            print(f"[{datetime.now()}] Error fetching candles for {symbol}: {str(e)}\n{traceback.format_exc()}")
        await asyncio.sleep(5)
    print(f"[{datetime.now()}] Failed to fetch candles for {symbol}")
    return pd.DataFrame()

async def generate_signals(symbol):
    df = await fetch_candles(symbol)
    if df.empty:
        print(f"[{datetime.now()}] No data for {symbol}")
        return "HOLD"
    
    # EMA (Price Action)
    df['ema_fast'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=26, adjust=False).mean()
    ema_signal = "BUY" if df['ema_fast'].iloc[-1] > df['ema_slow'].iloc[-1] else "SELL"
    
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0)
    df['rsi'] = 100 - (100 / (1 + rs))
    rsi_signal = "BUY" if df['rsi'].iloc[-1] < 40 else "SELL" if df['rsi'].iloc[-1] > 60 else "HOLD"
    
    # Price Action & Swing Level S/R
    df['swing_high'] = (df['high'] == df['high'].rolling(window=5, center=True).max()) & (df['high'] > df['high'].shift(1)) & (df['high'] > df['high'].shift(-1))
    df['swing_low'] = (df['low'] == df['low'].rolling(window=5, center=True).min()) & (df['low'] < df['low'].shift(1)) & (df['low'] < df['low'].shift(-1))
    price_action_signal = "BUY" if df['swing_low'].iloc[-1] else "SELL" if df['swing_high'].iloc[-1] else "HOLD"
    
    # Candlestick Structure
    df['body'] = abs(df['close'] - df['open'])
    df['doji'] = df['body'] < (df['high'] - df['low']) * 0.1
    df['hammer'] = (df['close'] > df['open']) & ((df['open'] - df['low']) > 2 * df['body']) & ((df['high'] - df['close']) < df['body'])
    df['shooting_star'] = (df['close'] < df['open']) & ((df['high'] - df['open']) > 2 * df['body']) & ((df['close'] - df['low']) < df['body'])
    df['bull_engulf'] = (df['close'].shift(1) < df['open'].shift(1)) & (df['close'] > df['open'].shift(1)) & (df['open'] < df['close'].shift(1)) & (df['close'] > df['open'].shift(1))
    df['bear_engulf'] = (df['close'].shift(1) > df['open'].shift(1)) & (df['close'] < df['open'].shift(1)) & (df['open'] > df['close'].shift(1)) & (df['close'] < df['open'].shift(1))
    candlestick_signal = "BUY" if df['hammer'].iloc[-1] or df['bull_engulf'].iloc[-1] else "SELL" if df['shooting_star'].iloc[-1] or df['bear_engulf'].iloc[-1] else "HOLD" if df['doji'].iloc[-1] else "HOLD"
    
    # BOS (Break of Structure)
    bos_signal = "HOLD"
    if len(df) >= 3:
        if df['high'].iloc[-1] > df['high'].iloc[-2] and df['high'].iloc[-2] > df['high'].iloc[-3]:
            bos_signal = "BUY"
        elif df['low'].iloc[-1] < df['low'].iloc[-2] and df['low'].iloc[-2] < df['low'].iloc[-3]:
            bos_signal = "SELL"
    
    # Choch (Change of Character)
    choch_signal = "HOLD"
    if len(df) >= 5:
        if df['high'].iloc[-1] < df['high'].iloc[-2] and df['low'].iloc[-1] > df['low'].iloc[-2]:
            choch_signal = "BUY"
        elif df['high'].iloc[-1] > df['high'].iloc[-2] and df['low'].iloc[-1] < df['low'].iloc[-2]:
            choch_signal = "SELL"
    
    # Liquidity
    liquidity_signal = "HOLD"
    recent_highs = df['high'].rolling(window=10).max()
    recent_lows = df['low'].rolling(window=10).min()
    if df['close'].iloc[-1] > recent_highs.iloc[-2]:
        liquidity_signal = "BUY"
    elif df['close'].iloc[-1] < recent_lows.iloc[-2]:
        liquidity_signal = "SELL"
    
    # Line Strategy Inducement
    inducement_signal = "HOLD"
    if len(df) >= 5:
        if df['close'].iloc[-1] > df['high'].iloc[-2] and df['close'].iloc[-2] < df['low'].iloc[-3]:
            inducement_signal = "BUY"
        elif df['close'].iloc[-1] < df['low'].iloc[-2] and df['close'].iloc[-2] > df['high'].iloc[-3]:
            inducement_signal = "SELL"
    
    # Swing Level & Support/Resistance
    sr_signal = "HOLD"
    if df['swing_low'].iloc[-1]:
        sr_signal = "BUY"
    elif df['swing_high'].iloc[-1]:
        sr_signal = "SELL"
    
    # Risk Management (SL/TP Filter)
    if len(df) >= 10:
        sl = df['low'].rolling(window=10).min().iloc[-1] if ema_signal == "BUY" else df['high'].rolling(window=10).max().iloc[-1]
        tp = df['close'].iloc[-1] + (df['close'].iloc[-1] - sl) * 2 if ema_signal == "BUY" else df['close'].iloc[-1] - (sl - df['close'].iloc[-1]) * 2
        r_r = abs(tp - df['close'].iloc[-1]) / abs(df['close'].iloc[-1] - sl) if abs(df['close'].iloc[-1] - sl) > 0 else 0
        if r_r < 1.5:
            ema_signal = "HOLD"  # Filter low R:R trades
    
    all_signals = [ema_signal, rsi_signal, price_action_signal, candlestick_signal, bos_signal, choch_signal, liquidity_signal, inducement_signal, sr_signal]
    buy_count = all_signals.count("BUY")
    sell_count = all_signals.count("SELL")
    final_signal = "BUY" if buy_count > sell_count else "SELL" if sell_count > buy_count else "HOLD"
    print(f"[{datetime.now()}] Signals for {symbol}: {all_signals}, Final: {final_signal}")
    final_signals[symbol] = final_signal
    return final_signal

async def get_proposal(symbol, amount, contract_type, duration, duration_unit):
    global request_counter
    try:
        request_counter += 1
        proposal_request = {
            "proposal": 1,
            "amount": amount,
            "basis": "stake",
            "contract_type": contract_type,
            "currency": "USD",
            "symbol": symbol,
            "duration": duration,
            "duration_unit": duration_unit,
            "req_id": request_counter
        }
        future = asyncio.Future()
        response_callbacks[request_counter] = future
        await send_with_rate_limit(proposal_request)
        print(f"[{datetime.now()}] Sent proposal request for {symbol}: {json.dumps(proposal_request, indent=2)}")
        response = await asyncio.wait_for(future, timeout=30)
        if "error" in response:
            error_msg = f"Proposal error for {symbol}: {response['error']['message']}"
            print(f"[{datetime.now()}] {error_msg}")
            pd.DataFrame([{
                "symbol": symbol,
                "signal": contract_type,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": None,
                "balance_after": balance,
                "error": error_msg
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            return None
        proposal_id = response.get("proposal", {}).get("id")
        print(f"[{datetime.now()}] Received proposal ID {proposal_id} for {symbol}")
        return proposal_id
    except Exception as e:
        error_msg = f"Proposal exception for {symbol}: {str(e)}\n{traceback.format_exc()}"
        print(f"[{datetime.now()}] {error_msg}")
        pd.DataFrame([{
            "symbol": symbol,
            "signal": contract_type,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "contract_id": None,
            "balance_after": balance,
            "error": error_msg
        }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
        return None

async def execute_trade(symbol, signal, amount=5.0):
    global balance, request_counter
    try:
        if DEBUG_MODE:
            print(f"[{datetime.now()}] DEBUG_MODE: Skipping trade for {symbol}, signal: {signal}")
            return
        if signal not in ["BUY", "SELL"]:
            print(f"[{datetime.now()}] Invalid signal for {symbol}: {signal}")
            return
        print(f"[{datetime.now()}] Starting trade for {symbol} with stake ${amount}...")
        duration_info = await fetch_contract_info(symbol)
        duration = duration_info["min"]
        duration_unit = duration_info["unit"]
        contract_type = "CALL" if signal == "BUY" else "PUT"
        proposal_id = await get_proposal(symbol, amount, contract_type, duration, duration_unit)
        if not proposal_id:
            print(f"[{datetime.now()}] No proposal ID for {symbol}, skipping trade")
            return
        request_counter += 1
        buy_request = {
            "buy": proposal_id,
            "price": amount,
            "req_id": request_counter
        }
        future = asyncio.Future()
        response_callbacks[request_counter] = future
        await send_with_rate_limit(buy_request)
        print(f"[{datetime.now()}] Sent buy request for {symbol}: {json.dumps(buy_request, indent=2)}")
        response = await asyncio.wait_for(future, timeout=30)
        if "error" in response:
            error_msg = f"Trade error for {symbol}: {response['error']['message']}"
            print(f"[{datetime.now()}] {error_msg}")
            pd.DataFrame([{
                "symbol": symbol,
                "signal": signal,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": None,
                "balance_after": balance,
                "error": error_msg
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            return
        contract_id = response.get("buy", {}).get("contract_id")
        balance_after = await check_balance()
        print(f"[{datetime.now()}] Trade {signal} placed for {symbol}, Contract ID: {contract_id}, Balance: ${balance_after}")
        pd.DataFrame([{
            "symbol": symbol,
            "signal": signal,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "contract_id": contract_id,
            "balance_after": balance_after,
            "error": ""
        }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
    except Exception as e:
        error_msg = f"Trade execution exception for {symbol}: {str(e)}\n{traceback.format_exc()}"
        print(f"[{datetime.now()}] {error_msg}")
        pd.DataFrame([{
            "symbol": symbol,
            "signal": signal,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "contract_id": None,
            "balance_after": balance,
            "error": error_msg
        }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))

async def check_balance():
    global request_counter, balance
    try:
        request_counter += 1
        balance_request = {
            "balance": 1,
            "subscribe": 1,
            "req_id": request_counter
        }
        future = asyncio.Future()
        response_callbacks[request_counter] = future
        await send_with_rate_limit(balance_request)
        response = await asyncio.wait_for(future, timeout=30)
        if "error" in response:
            print(f"[{datetime.now()}] Balance check error: {response['error']['message']}")
            return balance
        new_balance = float(response.get("balance", {}).get("balance", balance))
        balance = new_balance
        print(f"[{datetime.now()}] Updated balance: ${balance}")
        return balance
    except Exception as e:
        print(f"[{datetime.now()}] Balance check exception: {str(e)}\n{traceback.format_exc()}")
        return balance

async def trade_loop():
    global auto_trading, emergency_stop
    while auto_trading and not emergency_stop:
        for symbol in symbols:
            if not auto_trading or emergency_stop:
                break
            try:
                signal = await generate_signals(symbol)
                if signal in ["BUY", "SELL"]:
                    await execute_trade(symbol, signal)
                await asyncio.sleep(1)
            except Exception as e:
                print(f"[{datetime.now()}] Trade loop error for {symbol}: {str(e)}\n{traceback.format_exc()}")
        await asyncio.sleep(60)

@app.get("/status")
async def get_status():
    return JSONResponse({
        "bot_running": bot_running,
        "auto_trading": auto_trading,
        "emergency_stop": emergency_stop,
        "balance": balance,
        "signals": final_signals
    })

@app.get("/start")
async def start_bot():
    global bot_running, auto_trading
    if not bot_running:
        bot_running = True
        auto_trading = True
        asyncio.create_task(trade_loop())
        print(f"[{datetime.now()}] Bot started")
        return JSONResponse({"status": "Bot started"})
    return JSONResponse({"status": "Bot already running"})

@app.get("/stop")
async def stop_bot():
    global bot_running, auto_trading
    bot_running = False
    auto_trading = False
    print(f"[{datetime.now()}] Bot stopped")
    return JSONResponse({"status": "Bot stopped"})

@app.get("/emergency_stop")
async def emergency_stop_bot():
    global bot_running, auto_trading, emergency_stop
    bot_running = False
    auto_trading = False
    emergency_stop = True
    print(f"[{datetime.now()}] Emergency stop triggered")
    return JSONResponse({"status": "Emergency stop triggered"})

@app.get("/trade/{symbol}/{amount}")
async def manual_trade(symbol: str, amount: float):
    if symbol not in symbols:
        return JSONResponse({"status": f"Invalid symbol {symbol}"}, status_code=400)
    signal = await generate_signals(symbol)
    if signal in ["BUY", "SELL"]:
        await execute_trade(symbol, signal, amount)
        return JSONResponse({"status": f"Trade placed for {symbol}, signal: {signal}"})
    return JSONResponse({"status": f"No trade placed for {symbol}, signal: {signal}"})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = {
                "balance": balance,
                "signals": final_signals,
                "bot_running": bot_running,
                "auto_trading": auto_trading,
                "emergency_stop": emergency_stop
            }
            await websocket.send_json(data)
            await asyncio.sleep(5)
    except Exception as e:
        print(f"[{datetime.now()}] WebSocket endpoint error: {str(e)}\n{traceback.format_exc()}")
    finally:
        await websocket.close()

async def main():
    global ws, candles_data, final_signals, request_counter, subscription_ids, bot_running
    try:
        load_state()
        candles_data = {symbol: candles_data.get(symbol, []) for symbol in symbols}
        final_signals = {symbol: "HOLD" for symbol in symbols}
        success = await connect_websocket()
        if not success:
            print(f"[{datetime.now()}] Failed to initialize WebSocket")
            return
        asyncio.create_task(websocket_consumer())
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 1200:  # 20min
            all_candles_ready = all(len(candles_data.get(s, [])) >= 50 for s in symbols if s in candles_data)
            if all_candles_ready:
                break
            await asyncio.sleep(1)
        print(f"[{datetime.now()}] Finished collecting initial candles")
        bot_running = True
        print(await check_balance())
        if os.path.exists("trades.csv"):
            print(f"[{datetime.now()}] Trades saved in trades.csv")
        if auto_trading:
            asyncio.create_task(trade_loop())
        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()
    except Exception as e:
        print(f"[{datetime.now()}] Main loop error: {str(e)}\n{traceback.format_exc()}")
        save_state()
        if ws and not ws.closed:
            try:
                await ws.close()
                print(f"[{datetime.now()}] WebSocket closed gracefully")
            except Exception as ex:
                print(f"[{datetime.now()}] Error closing WebSocket: {str(ex)}\n{traceback.format_exc()}")
        if os.path.exists("trades.csv"):
            print(f"[{datetime.now()}] Trades saved in trades.csv")
    finally:
        save_state()
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.sleep(1)

if __name__ == "__main__":
    if not os.path.exists("trades.csv"):
        pd.DataFrame(columns=["symbol", "signal", "time", "contract_id", "balance_after", "error"]).to_csv("trades.csv", index=False)
    else:
        print(f"[{datetime.now()}] Existing trades.csv found")
    asyncio.run(main())