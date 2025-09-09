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

load_dotenv()

app = FastAPI()
ws = None
candles_data = {}
final_signals = {}
balance = 9946.63  # From latest log
symbols = ["frxEURUSD", "1HZ10V", "frxGBPUSD", "frxUSDJPY", "frxXAUUSD", "1HZ25V", "R_100", "frxUSDCAD", "1HZ75V", "1HZ90V", "1HZ100V", "R_75"]
active_symbols = []
subscription_ids = {}
app_id = os.getenv("DERIV_APP_ID", "1089")
api_token = os.getenv("DERIV_API_TOKEN", "NLhZn6KsIamjQlF")
message_queue = deque()
response_callbacks = {}
request_counter = 0
last_request_time = 0
RATE_LIMIT_DELAY = 3.0
bot_running = False
auto_trading = True
emergency_stop = False
shutdown_triggered = False
valid_durations = {}

def signal_handler(sig, frame):
    global bot_running, auto_trading, emergency_stop, ws, shutdown_triggered
    if shutdown_triggered:
        return
    shutdown_triggered = True
    print("Received interrupt signal, shutting down gracefully...")
    bot_running = False
    auto_trading = False
    emergency_stop = True
    if ws and not ws.closed:
        asyncio.create_task(ws.close())
        print("WebSocket close scheduled")
    if os.path.exists("trades.csv"):
        print("Trades saved in trades.csv")
    exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

async def send_with_rate_limit(message):
    global last_request_time
    current_time = time.time()
    elapsed = current_time - last_request_time
    if elapsed < RATE_LIMIT_DELAY:
        await asyncio.sleep(RATE_LIMIT_DELAY - elapsed)
    await ws.send(json.dumps(message))
    last_request_time = time.time()

async def fetch_contract_info(symbol):
    global ws, request_counter, valid_durations
    try:
        request_counter += 1
        request_id = request_counter
        contract_request = {
            "contracts_for": symbol,
            "req_id": request_id
        }
        future = asyncio.Future()
        response_callbacks[request_id] = future
        await send_with_rate_limit(contract_request)
        print(f"Sent contract info request for {symbol}")
        response = await asyncio.wait_for(future, timeout=30)
        if "error" in response:
            print(f"Contract info error for {symbol}: {response['error']['message']}")
            return None
        contracts = response.get("contracts_for", {}).get("available", [])
        for contract in contracts:
            if contract["contract_type"] in ["CALL", "PUT"]:
                durations = contract.get("durations", [])
                valid_durations[symbol] = {
                    "min": durations[0]["min"] if durations else 10,
                    "unit": durations[0]["unit"] if durations else "t"
                }
                print(f"Valid duration for {symbol}: {valid_durations[symbol]}")
                return valid_durations[symbol]
        return None
    except Exception as e:
        print(f"Error fetching contract info for {symbol}: {e}")
        return None

async def connect_websocket():
    global ws, candles_data, subscription_ids, request_counter, active_symbols
    uri = f"wss://ws.derivws.com/websockets/v3?app_id={app_id}"
    retries = 5
    for attempt in range(retries):
        try:
            ws = await websockets.connect(uri, ping_interval=30, ping_timeout=20)
            print("WebSocket opened")
            request_counter += 1
            await send_with_rate_limit({"authorize": api_token, "req_id": request_counter})
            auth_response = await asyncio.wait_for(ws.recv(), timeout=15)
            auth_response = json.loads(auth_response)
            print("Auth response:", json.dumps(auth_response, indent=2))
            if "error" in auth_response:
                print(f"Authorization failed: {auth_response['error']['message']}")
                return False
            if "trade" not in auth_response.get("authorize", {}).get("scopes", []):
                print("Error: API token lacks 'trade' scope.")
                return False
            async def keep_alive():
                global request_counter
                while True:
                    try:
                        request_counter += 1
                        await send_with_rate_limit({"ping": 1, "req_id": request_counter})
                        print("Sent ping")
                        await asyncio.sleep(30)
                    except Exception as e:
                        print(f"Keep-alive error: {e}")
                        break
            asyncio.create_task(keep_alive())
            request_counter += 1
            await send_with_rate_limit({"forget_all": "ticks", "req_id": request_counter})
            await asyncio.sleep(1)
            request_counter += 1
            await send_with_rate_limit({"active_symbols": "brief", "req_id": request_counter})
            active_symbols = []
            for _ in range(5):
                try:
                    response = await asyncio.wait_for(ws.recv(), timeout=15)
                    response = json.loads(response)
                    print(f"Received active_symbols response: {json.dumps(response, indent=2)}")
                    if "active_symbols" in response:
                        active_symbols.extend([
                            s["symbol"] for s in response.get("active_symbols", [])
                            if s.get("exchange_is_open", 0) == 1 and s.get("is_trading_suspended", 0) == 0
                        ])
                        print("Active symbols:", active_symbols)
                        break
                except asyncio.TimeoutError:
                    print("Timeout waiting for active_symbols, retrying...")
                    request_counter += 1
                    await send_with_rate_limit({"active_symbols": "brief", "req_id": request_counter})
            else:
                print("Failed to get active_symbols, using fallback symbols")
                active_symbols.extend(symbols)
            for symbol in symbols:
                candles_data[symbol] = []
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
                print(f"Subscribed to candles for {symbol}")
                await fetch_contract_info(symbol)  # Fetch valid durations
            return True
        except Exception as e:
            print(f"WebSocket Error (attempt {attempt + 1}/{retries}): {e}")
            await asyncio.sleep(5)
    print(f"Failed to connect WebSocket after {retries} attempts")
    return False

async def websocket_consumer():
    global ws, candles_data, subscription_ids, message_queue
    while True:
        try:
            async for message in ws:
                msg = json.loads(message)
                message_queue.append(msg)
                if "error" in msg:
                    error_msg = f"API Error: {msg['error']['message']}, Code: {msg['error'].get('code', 'N/A')}, Details: {json.dumps(msg, indent=2)}"
                    print(error_msg)
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
                    print(f"Received {len(msg['candles'])} candles for {symbol}")
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
                    print(f"Received OHLC update for {symbol}")
                elif "subscription" in msg:
                    symbol = msg["echo_req"]["ticks_history"]
                    subscription_ids[symbol] = msg["subscription"]["id"]
                    print(f"Subscribed to {symbol}: ID {subscription_ids[symbol]}")
                elif "proposal" in msg:
                    request_id = msg.get("req_id")
                    if request_id in response_callbacks:
                        response_callbacks[request_id].set_result(msg)
                        del response_callbacks[request_id]
                        print(f"Processed proposal response for req_id {request_id}")
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
            print(f"WebSocket consumer error: {e}, reconnecting...")
            await asyncio.sleep(5)
            await connect_websocket()

async def fetch_candles(symbol):
    global candles_data
    retries = 5
    for attempt in range(retries):
        try:
            if symbol in candles_data and len(candles_data[symbol]) >= 10:
                df = pd.DataFrame(candles_data[symbol])
                df['time'] = pd.to_datetime(df['epoch'], unit='s')
                print(f"Fetched {len(df)} candles for {symbol}")
                return df[['time', 'open', 'high', 'low', 'close', 'volume']]
            else:
                print(f"Not enough candles for {symbol}, waiting...")
                await asyncio.sleep(5)
        except Exception as e:
            print(f"Error fetching candles for {symbol}: {e}")
        await asyncio.sleep(5)
    print(f"Failed to fetch candles for {symbol}")
    return pd.DataFrame()

async def generate_signals(symbol):
    df = await fetch_candles(symbol)
    if df.empty:
        print(f"No data for {symbol}")
        return "HOLD"
    
    df['ema_fast'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_slow'] = df['close'].ewm(span=26, adjust=False).mean()
    ema_signal = "BUY" if df['ema_fast'].iloc[-1] > df['ema_slow'].iloc[-1] else "SELL"
    
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0)
    df['rsi'] = 100 - (100 / (1 + rs))
    rsi_signal = "BUY" if df['rsi'].iloc[-1] < 40 else "SELL" if df['rsi'].iloc[-1] > 60 else "HOLD"
    
    df['swing_high'] = (df['high'] == df['high'].rolling(window=5, center=True).max()) & (df['high'] > df['high'].shift(1)) & (df['high'] > df['high'].shift(-1))
    df['swing_low'] = (df['low'] == df['low'].rolling(window=5, center=True).min()) & (df['low'] < df['low'].shift(1)) & (df['low'] < df['low'].shift(-1))
    price_action_signal = "BUY" if df['swing_low'].iloc[-1] else "SELL" if df['swing_high'].iloc[-1] else "HOLD"
    
    df['body'] = abs(df['close'] - df['open'])
    df['doji'] = df['body'] < (df['high'] - df['low']) * 0.1
    df['hammer'] = (df['close'] > df['open']) & ((df['open'] - df['low']) > 2 * df['body']) & ((df['high'] - df['close']) < df['body'])
    df['bull_engulf'] = (df['close'].shift(1) < df['open'].shift(1)) & (df['close'] > df['open'].shift(1)) & (df['open'] < df['close'].shift(1)) & (df['close'] > df['open'].shift(1))
    df['bear_engulf'] = (df['close'].shift(1) > df['open'].shift(1)) & (df['close'] < df['open'].shift(1)) & (df['open'] > df['close'].shift(1)) & (df['close'] < df['open'].shift(1))
    candlestick_signal = "BUY" if df['hammer'].iloc[-1] or df['bull_engulf'].iloc[-1] else "SELL" if df['bear_engulf'].iloc[-1] else "HOLD" if df['doji'].iloc[-1] else "HOLD"
    
    bos_signal = "HOLD"
    if len(df) >= 3:
        if df['high'].iloc[-1] > df['high'].iloc[-2] and df['high'].iloc[-2] > df['high'].iloc[-3]:
            bos_signal = "BUY"
        elif df['low'].iloc[-1] < df['low'].iloc[-2] and df['low'].iloc[-2] < df['low'].iloc[-3]:
            bos_signal = "SELL"
    
    liquidity_signal = "HOLD"
    recent_highs = df['high'].rolling(window=10).max()
    recent_lows = df['low'].rolling(window=10).min()
    if df['close'].iloc[-1] > recent_highs.iloc[-2]:
        liquidity_signal = "BUY"
    elif df['close'].iloc[-1] < recent_lows.iloc[-2]:
        liquidity_signal = "SELL"
    
    signals = [ema_signal, rsi_signal, price_action_signal, candlestick_signal, bos_signal, liquidity_signal]
    buy_count = signals.count("BUY")
    sell_count = signals.count("SELL")
    final_signal = "BUY" if buy_count >= 1 else "SELL" if sell_count >= 1 else "HOLD"
    print(f"Signals for {symbol}: {signals}, Final: {final_signal}")
    final_signals[symbol] = final_signal
    return final_signal

async def get_proposal(symbol, signal, stake=5.0, retries=3):
    global ws, request_counter, valid_durations
    contract_types = ["CALL", "PUT"] if signal == "BUY" else ["PUT", "CALL"]
    duration_info = valid_durations.get(symbol, {"min": 10, "unit": "t" if "1HZ" in symbol or symbol.startswith("frx") else "m"})
    duration = duration_info["min"]
    duration_unit = duration_info["unit"]
    for contract_type in contract_types:
        for attempt in range(retries):
            try:
                if not ws or ws.closed:
                    print(f"WebSocket closed, reconnecting...")
                    await connect_websocket()
                request_counter += 1
                request_id = request_counter
                proposal_request = {
                    "proposal": 1,
                    "contract_type": contract_type,
                    "amount": stake,
                    "basis": "stake",
                    "currency": "USD",
                    "duration": duration,
                    "duration_unit": duration_unit,
                    "symbol": symbol,
                    "req_id": request_id
                }
                future = asyncio.Future()
                response_callbacks[request_id] = future
                await send_with_rate_limit(proposal_request)
                print(f"Sent proposal request for {symbol}: {json.dumps(proposal_request)}")
                response = await asyncio.wait_for(future, timeout=30)
                print(f"Received proposal response for {symbol}")
                if "error" in response:
                    error_msg = f"Proposal Error for {symbol} ({contract_type}, attempt {attempt + 1}): {response['error']['message']}, Code: {response['error'].get('code', 'N/A')}"
                    print(error_msg)
                    pd.DataFrame([{
                        "symbol": symbol,
                        "signal": signal,
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "contract_id": None,
                        "balance_after": balance,
                        "error": error_msg
                    }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                    await asyncio.sleep(2 ** attempt)
                    continue
                if "proposal" not in response or "ask_price" not in response["proposal"]:
                    error_msg = f"Proposal Error for {symbol} ({contract_type}, attempt {attempt + 1}): Invalid response, missing proposal or ask_price"
                    print(error_msg)
                    pd.DataFrame([{
                        "symbol": symbol,
                        "signal": signal,
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "contract_id": None,
                        "balance_after": balance,
                        "error": error_msg
                    }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                    await asyncio.sleep(2 ** attempt)
                    continue
                return response["proposal"]["ask_price"], contract_type, duration, duration_unit
            except asyncio.TimeoutError:
                error_msg = f"Proposal Timeout for {symbol} ({contract_type}, attempt {attempt + 1}): No response from API"
                print(error_msg)
                pd.DataFrame([{
                    "symbol": symbol,
                    "signal": signal,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "contract_id": None,
                    "balance_after": balance,
                    "error": error_msg
                }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                error_msg = f"Proposal Exception for {symbol} ({contract_type}, attempt {attempt + 1}): {str(e)}"
                print(error_msg)
                pd.DataFrame([{
                    "symbol": symbol,
                    "signal": signal,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "contract_id": None,
                    "balance_after": balance,
                    "error": error_msg
                }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                await asyncio.sleep(2 ** attempt)
        print(f"Failed to get proposal for {symbol} with {contract_type} after {retries} attempts")
    error_msg = f"Failed to get proposal for {symbol} after trying all contract types"
    print(error_msg)
    pd.DataFrame([{
        "symbol": symbol,
        "signal": signal,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "contract_id": None,
        "balance_after": balance,
        "error": error_msg
    }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
    return None, None, None, None

async def execute_trade(symbol, stake=5.0):
    global balance, request_counter
    signal = final_signals.get(symbol, "HOLD")
    print(f"Starting trade for {symbol} with stake ${stake}...")
    if signal not in ["BUY", "SELL"]:
        error_msg = f"No trade signal for {symbol} ({signal})"
        print(error_msg)
        pd.DataFrame([{
            "symbol": symbol,
            "signal": signal,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "contract_id": None,
            "balance_after": balance,
            "error": error_msg
        }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
        return None
    for attempt in range(3):
        try:
            price, contract_type, duration, duration_unit = await get_proposal(symbol, signal, stake)
            if not price:
                error_msg = f"No valid proposal price for {symbol}"
                print(error_msg)
                pd.DataFrame([{
                    "symbol": symbol,
                    "signal": signal,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "contract_id": None,
                    "balance_after": balance,
                    "error": error_msg
                }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                return None
            request_counter += 1
            request_id = request_counter
            contract = {
                "buy": 1,
                "price": float(price),
                "parameters": {
                    "contract_type": contract_type,
                    "amount": float(stake),
                    "basis": "stake",
                    "currency": "USD",
                    "duration": duration,
                    "duration_unit": duration_unit,
                    "symbol": symbol
                },
                "req_id": request_id
            }
            future = asyncio.Future()
            response_callbacks[request_id] = future
            await send_with_rate_limit(contract)
            print(f"Sent buy request for {symbol}")
            response = await asyncio.wait_for(future, timeout=30)
            print(f"Received buy response for {symbol}")
            if "error" in response:
                error_msg = f"Trade Error for {symbol}: {response['error']['message']}, Code: {response['error'].get('code', 'N/A')}"
                print(error_msg)
                pd.DataFrame([{
                    "symbol": symbol,
                    "signal": signal,
                    "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "contract_id": None,
                    "balance_after": balance,
                    "error": error_msg
                }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
                return None
            balance = response["buy"]["balance_after"]
            contract_id = response["buy"]["contract_id"]
            print(f"Trade {signal} placed for {symbol}, Contract ID: {contract_id}, Balance: ${balance:.2f}")
            pd.DataFrame([{
                "symbol": symbol,
                "signal": signal,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": contract_id,
                "balance_after": balance
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            return contract_id
        except asyncio.TimeoutError:
            error_msg = f"Trade Timeout for {symbol}: No response from API"
            print(error_msg)
            pd.DataFrame([{
                "symbol": symbol,
                "signal": signal,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": None,
                "balance_after": balance,
                "error": error_msg
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            return None
        except Exception as e:
            error_msg = f"Trade Exception for {symbol}: {str(e)}"
            print(error_msg)
            pd.DataFrame([{
                "symbol": symbol,
                "signal": signal,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "contract_id": None,
                "balance_after": balance,
                "error": error_msg
            }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
            return None
    error_msg = f"Failed to place trade for {symbol} after 3 attempts"
    print(error_msg)
    return None

async def sell_contract(contract_id):
    global balance, request_counter
    try:
        request_counter += 1
        request_id = request_counter
        sell_request = {
            "sell": 1,
            "contract_id": int(contract_id),
            "req_id": request_id
        }
        future = asyncio.Future()
        response_callbacks[request_id] = future
        await send_with_rate_limit(sell_request)
        print(f"Sent sell request for contract {contract_id}")
        response = await asyncio.wait_for(future, timeout=30)
        print(f"Received sell response for contract {contract_id}")
        if "error" in response:
            error_msg = f"Sell Error for contract {contract_id}: {response['error']['message']}, Code: {response['error'].get('code', 'N/A')}"
            print(error_msg)
            return error_msg
        balance = response["sell"]["balance_after"]
        print(f"Sold contract {contract_id}, Balance: ${balance:.2f}")
        pd.DataFrame([{
            "symbol": "N/A",
            "signal": "SELL",
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "contract_id": contract_id,
            "balance_after": balance,
            "error": None
        }]).to_csv("trades.csv", mode="a", index=False, header=not os.path.exists("trades.csv"))
        return f"Sold contract {contract_id}, Balance: ${balance:.2f}"
    except Exception as e:
        error_msg = f"Sell Exception for contract {contract_id}: {str(e)}"
        print(error_msg)
        return error_msg

async def check_balance():
    global ws, request_counter, balance
    try:
        request_counter += 1
        request_id = request_counter
        balance_request = {"balance": 1, "req_id": request_id}
        future = asyncio.Future()
        response_callbacks[request_id] = future
        await send_with_rate_limit(balance_request)
        print(f"Sent balance request")
        response = await asyncio.wait_for(future, timeout=30)
        print(f"Received balance response")
        if "error" in response:
            error_msg = f"Balance Error: {response['error']['message']}, Code: {response['error'].get('code', 'N/A')}"
            print(error_msg)
            return error_msg
        balance = response["balance"]["balance"]
        return f"Account Balance: ${balance:.2f}"
    except Exception as e:
        error_msg = f"Balance Exception: {str(e)}"
        print(error_msg)
        return error_msg

async def trade_loop():
    global auto_trading, emergency_stop
    while auto_trading and not emergency_stop:
        for symbol in symbols:
            if not auto_trading or emergency_stop:
                break
            try:
                signal = await generate_signals(symbol)
                print(f"{symbol}: {signal}")
                if signal in ["BUY", "SELL"]:
                    contract_id = await execute_trade(symbol, stake=5.0)
                    if contract_id:
                        print(f"Trade placed for {symbol}, Contract ID: {contract_id}")
                await asyncio.sleep(1)  # Prevent overwhelming API
            except Exception as e:
                print(f"Error in trade loop for {symbol}: {e}")
                continue
        await asyncio.sleep(60)

@app.websocket("/ws/signals")
async def websocket_signals(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            if bot_running and not emergency_stop:
                signal_summary = {symbol: final_signals.get(symbol, "HOLD") for symbol in symbols}
                await websocket.send_json({"type": "signals", "data": signal_summary})
            await asyncio.sleep(60)
    except Exception as e:
        print(f"WebSocket signals error: {e}")
    finally:
        await websocket.close()

@app.get("/trades")
async def get_trades():
    try:
        trades_df = pd.read_csv("trades.csv")
        return trades_df.to_dict(orient="records")
    except FileNotFoundError:
        return []

@app.get("/start")
async def start_bot():
    global bot_running, emergency_stop
    if bot_running:
        return JSONResponse({"status": "Bot already running"})
    bot_running = True
    emergency_stop = False
    print("Bot started")
    return JSONResponse({"status": "Bot started"})

@app.get("/stop")
async def stop_bot():
    global bot_running, auto_trading
    bot_running = False
    auto_trading = False
    print("Bot stopped")
    return JSONResponse({"status": "Bot stopped"})

@app.get("/emergency")
async def emergency_stop_bot():
    global bot_running, auto_trading, emergency_stop
    bot_running = False
    auto_trading = False
    emergency_stop = True
    print("Emergency stop activated")
    return JSONResponse({"status": "Emergency stop activated"})

@app.get("/toggle_auto")
async def toggle_auto_trading():
    global auto_trading
    auto_trading = not auto_trading
    status = "enabled" if auto_trading else "disabled"
    print(f"Auto-trading {status}")
    if auto_trading:
        asyncio.create_task(trade_loop())
    return JSONResponse({"status": f"Auto-trading {status}"})

@app.get("/trade/{symbol}/{stake}")
async def trigger_trade(symbol: str, stake: float):
    if not symbols:
        return JSONResponse({"error": "No tradeable symbols available"}, status_code=500)
    if symbol not in symbols:
        return JSONResponse({"error": f"Invalid symbol. Choose from {symbols}"}, status_code=400)
    if not bot_running or emergency_stop:
        return JSONResponse({"error": "Bot is stopped or in emergency mode"}, status_code=400)
    contract_id = await execute_trade(symbol, stake)
    if contract_id:
        return JSONResponse({"status": f"Trade placed for {symbol}, Contract ID: {contract_id}"})
    return JSONResponse({"error": f"Failed to place trade for {symbol}: Check logs"}, status_code=500)

@app.get("/sell/{contract_id}")
async def trigger_sell(contract_id: int):
    if not bot_running or emergency_stop:
        return JSONResponse({"error": "Bot is stopped or in emergency mode"}, status_code=400)
    result = await sell_contract(contract_id)
    return JSONResponse({"status": result})

@app.get("/balance")
async def get_balance():
    result = await check_balance()
    return JSONResponse({"status": result})

async def main():
    global ws, candles_data, final_signals, request_counter, subscription_ids, bot_running
    try:
        candles_data = {symbol: [] for symbol in symbols}
        final_signals = {symbol: "HOLD" for symbol in symbols}
        success = await connect_websocket()
        if not success:
            print("Failed to initialize WebSocket")
            return
        asyncio.create_task(websocket_consumer())
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 120:
            if all(len(candles_data.get(s, [])) >= 50 for s in symbols if s in candles_data):
                break
            await asyncio.sleep(1)
        print("Finished collecting initial candles")
        bot_running = True
        print(await check_balance())
        if os.path.exists("trades.csv"):
            print("Trades saved in trades.csv")
        if auto_trading:
            asyncio.create_task(trade_loop())
        config = uvicorn.Config(app, host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()
    except Exception as e:
        print(f"Main loop error: {e}")
        if ws and not ws.closed:
            try:
                await ws.close()
                print("WebSocket closed gracefully")
            except Exception as ex:
                print(f"Error closing WebSocket: {ex}")
        if os.path.exists("trades.csv"):
            print("Trades saved in trades.csv")
    finally:
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.sleep(1)

if __name__ == "__main__":
    if not os.path.exists("trades.csv"):
        pd.DataFrame(columns=["symbol", "signal", "time", "contract_id", "balance_after", "error"]).to_csv("trades.csv", index=False)
    else:
        print("Existing trades.csv found")
    asyncio.run(main())