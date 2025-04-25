import os
import json
import time
import math
import hashlib
import uuid
import requests
import pandas as pd
import pyupbit as upbit
import zoneinfo
from datetime import datetime
from urllib.parse import urlencode, unquote
from collections import defaultdict
from dotenv import load_dotenv
import jwt

# ─── 설정 ─────────────────────────────────────────────────────────────
TICKERS      = ["KRW-BTC", "KRW-XRP", "KRW-MANA"]
MIN_ORDER    = 5000          # 최소 주문 금액(원)
FEE_RATE     = 0.0005        # 수수료율 0.05%

HOME_DIR     = os.path.expanduser("~")
TRADING_DIR  = os.path.join(HOME_DIR, "trading")
STATE_FILE   = os.path.join(TRADING_DIR, "state.json")
LOG_FILE     = os.path.join(TRADING_DIR, "rebalancing_log.csv")
TZ           = zoneinfo.ZoneInfo("Asia/Seoul")
os.makedirs(TRADING_DIR, exist_ok=True)

# .env 로드
DOTENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(DOTENV_PATH)
UPBIT_ACCESS  = os.getenv("UPBIT_ACCESS_KEY")
UPBIT_SECRET  = os.getenv("UPBIT_SECRET_KEY")
SERVER_URL    = os.getenv("UPBIT_OPEN_API_SERVER_URL", "https://api.upbit.com")

# 주문 수량 단위 설정
ORDER_UNITS = {
    "BTC": 0.000001,
    "XRP": 0.1,
    "MANA": 0.1
}

# ─── Upbit 주문 헬퍼 ────────────────────────────────────────────────────
def place_order(params: dict) -> dict:
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")
    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': UPBIT_ACCESS,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }
    jwt_token = jwt.encode(payload, UPBIT_SECRET, algorithm="HS256")
    headers = {'Authorization': f'Bearer {jwt_token}'}
    response = requests.post(f"{SERVER_URL}/v1/orders", json=params, headers=headers)
    return response.json()

def get_order_status(order_id: str) -> dict:
    params = {
        'uuids[]': [order_id],
        'states[]': ['done', 'cancel']
    }
    query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")
    m = hashlib.sha512()
    m.update(query_string)
    query_hash = m.hexdigest()

    payload = {
        'access_key': UPBIT_ACCESS,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512',
    }
    jwt_token = jwt.encode(payload, UPBIT_SECRET, algorithm="HS256")
    headers = {'Authorization': f'Bearer {jwt_token}'}
    resp = requests.get(f"{SERVER_URL}/v1/orders", params=params, headers=headers)
    data = resp.json()
    return data[0] if isinstance(data, list) and data else {}

# ─── Tick price 조정 ───────────────────────────────────────────────────
def get_tick_price(price: float) -> float:
    steps = [
        (2_000_000,    1_000),
        (1_000_000,      500),
        (   500_000,      100),
        (   100_000,       50),
        (    10_000,       10),
        (     1_000,        1),
        (       100,      0.1),
        (        10,     0.01),
        (         1,    0.001),
        (       0.1,   0.0001),
        (      0.01,  0.00001),
        (     0.001, 0.000001),
        (    0.0001,0.0000001),
    ]
    for threshold, tick in steps:
        if price >= threshold:
            return (price // tick) * tick
    return math.floor(price / 0.00000001) * 0.00000001

# ─── 과거 21일 일봉 데이터 ────────────────────────────────────────────
def calculate_weight(tickers):
    inv_vols = []
    for t in tickers:
        df = upbit.get_ohlcv(t, count=21, interval="day")
        df['pct_change'] = df['close'].pct_change()
        vol = df['pct_change'].rolling(20).std().iloc[-1]
        inv_vols.append(1/vol if pd.notna(vol) and vol>0 else 0)
    total = sum(inv_vols)
    return [v/total if total>0 else 0 for v in inv_vols]

# ─── 잔고 조회 ────────────────────────────────────────────────────────
def load_state():
    # get balances via API
    query_string = b''
    payload = {
        'access_key': UPBIT_ACCESS,
        'nonce': str(uuid.uuid4()),
        'query_hash_alg': 'SHA512',
    }
    jwt_token = jwt.encode(payload, UPBIT_SECRET, algorithm="HS256")
    headers = {'Authorization': f'Bearer {jwt_token}'}
    res = requests.get(f"{SERVER_URL}/v1/accounts", headers=headers).json()
    balances = defaultdict(float)
    for acc in res:
        currency = acc.get("currency")
        bal = float(acc.get("balance",0))
        if currency == "KRW":
            balances["KRW"] = bal
        else:
            balances[f"KRW-{currency}"] = bal
    return balances

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ─── 리밸런싱 ─────────────────────────────────────────────────────────
def rebalance():
    state = load_state()

    quantities = {t.split('-',1)[1]: state.get(t, 0.0) for t in TICKERS}
    cash = state.get("KRW", 0.0)

    # fetch real time prices and portfolio value
    prices = {t: upbit.get_current_price(t) for t in TICKERS}
    portfolio_value = cash + sum(quantities[a] * prices[f"KRW-{a}"] for a in quantities)

    # compute new weights
    new_weights = dict(zip(TICKERS, calculate_weight(TICKERS)))

    now = datetime.now(TZ)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    records = []

    print(f"[{ts}] Rebalance Start — PV: {portfolio_value:,.0f} KRW, Cash: {cash:,.0f} KRW")

    # compute actions
    actions = []
    for t in TICKERS:
        asset = t.split('-',1)[1]
        price = get_tick_price(prices[t])
        prev_val = quantities.get(asset, 0.0) * price
        target_val = portfolio_value * new_weights[t]
        diff_krw = target_val - prev_val

        if abs(diff_krw) < MIN_ORDER:
            print(f"  {t}: Skip — {abs(diff_krw):,.0f} KRW < {MIN_ORDER:,}")
            continue

        qty = diff_krw / price
        # truncate to unit
        unit = ORDER_UNITS.get(asset)
        if unit:
            qty = math.floor(abs(qty)/unit)*unit * (1 if qty>0 else -1)

        actions.append({
            "ticker": t,
            "asset": asset,
            "diff_qty": qty,
            "diff_krw": diff_krw
        })

    # execute SELL first
    sell_ids = []
    for act in actions:
        if act["diff_qty"] < 0:
            t = act["ticker"]; asset = act["asset"]; qty = abs(act["diff_qty"])
            params = {"market": t, "side": "ask", "ord_type": "market", "volume": str(qty)}
            resp = place_order(params)
            oid = resp.get("uuid")
            print(f"  {t}: SELL order submitted, uuid={oid}")
            if oid: sell_ids.append(oid)

    # wait for sell completion
    for oid in sell_ids:
        while True:
            status = get_order_status(oid)
            state_s = status.get("state")
            if state_s in ("done","cancel"):
                print(f"  SELL {oid} {state_s}")
                break
            time.sleep(1)

    # record sell results
    for oid in sell_ids:
        status = get_order_status(oid)
        t = status.get("market"); asset = t.split('-',1)[1]
        executed = float(status.get("executed_volume",0))
        fee = float(status.get("paid_fee",0))
        price = float(status.get("price",0))
        quantities[asset] -= executed
        cash += executed * price - fee
        records.append({
            "timestamp": ts, "ticker": t, "action": "SELL",
            "price": price, "qty": executed, "fee": fee,
            "cash_after": cash, "pv": portfolio_value
        })

    # execute BUY
    for act in actions:
        if act["diff_qty"] > 0:
            t = act["ticker"]; asset = act["asset"]; qty = act["diff_qty"]
            max_gross = cash/(1+FEE_RATE)
            max_qty = math.floor(max_gross/get_tick_price(prices[t])/ORDER_UNITS[asset])*ORDER_UNITS[asset]
            if qty > max_qty:
                print(f"  ⚠️ BUY capped {qty:.6f} → {max_qty:.6f}")
                qty = max_qty
            if qty <= 0:
                continue
            spend = qty * get_tick_price(prices[t])
            params = {"market": t, "side": "bid", "ord_type": "market", "price": str(int(spend))}
            resp = place_order(params)
            oid = resp.get("uuid")
            print(f"  {t}: BUY order submitted, uuid={oid}")
            if not oid: continue
            # wait fill
            while True:
                status = get_order_status(oid)
                state_s = status.get("state")
                if state_s in ("done","cancel"):
                    print(f"  BUY {oid} {state_s}")
                    break
                time.sleep(1)
            # record buy results
            status = get_order_status(oid)
            executed = float(status.get("executed_volume",0))
            fee = float(status.get("paid_fee",0))
            price = float(status.get("price",0))
            quantities[asset] += executed
            cash -= executed * price + fee
            records.append({
                "timestamp": ts, "ticker": t, "action": "BUY",
                "price": price, "qty": executed, "fee": fee,
                "cash_after": cash, "pv": portfolio_value
            })

    # save logs
    df = pd.DataFrame(records)
    df.to_csv(LOG_FILE, mode='a', header=not os.path.exists(LOG_FILE), index=False)

    # optionally git commit
    # subprocess.run([...])

    print(f"[{ts}] Rebalance Done — Cash: {cash:,.0f} KRW")

if __name__ == "__main__":
    rebalance()
