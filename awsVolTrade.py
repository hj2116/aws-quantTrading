import os
import json
from datetime import datetime
import zoneinfo
import pandas as pd
import pyupbit as upbit
import subprocess
from dotenv import load_dotenv
import jwt
import hashlib
import requests
import uuid
from urllib.parse import urlencode, unquote
from collections import defaultdict

# ─── 설정 ─────────────────────────────────────────────────────────────
TICKERS      = ["KRW-BTC", "KRW-XRP", "KRW-MANA"]
input_value  = 0     # 초기 자본(원)
DOTENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(DOTENV_PATH)

# 홈 디렉토리 기준 작업 경로 설정
HOME_DIR     = os.path.expanduser("~")
TRADING_DIR  = os.path.join(HOME_DIR, "trading")
STATE_FILE   = os.path.join(TRADING_DIR, "state.json")
LOG_FILE     = os.path.join(TRADING_DIR, "rebalancing_log.csv")
TZ           = zoneinfo.ZoneInfo("Asia/Seoul")

#API 접근 값 불러오기
UPBIT_ACCESS = os.getenv("UPBIT_ACCESS_KEY")
UPBIT_SECRET = os.getenv("UPBIT_SECRET_KEY")
SEVER_URL = "https://api.upbit.com"


# 작업 디렉토리 자동 생성
os.makedirs(TRADING_DIR, exist_ok=True)

def get_tick_price(price: float) -> float:

    #Upbit KRW 마켓의 호가 단위에 맞춰 버림.

    steps = [
        (2000000, 1000),
        (1000_000,   500),
        (  500000,   100),
        (  100000,    50),
        (   10000,    10),
        (    1000,     1),
        (      100,   0.1),
        (       10,  0.01),
        (        1, 0.001),
        (      0.1,0.0001),
    ]
    for threshold, tick in steps:
        if price >= threshold:
            return (price // tick) * tick
    return price


# ─── (1) 과거 데이터 불러오기 ───────────────────────────────────────────
def get_data(ticker, count=21):
    return upbit.get_ohlcv(ticker, count=count, interval="day")

# ─── (2) 변동성 역수 가중치 계산 ────────────────────────────────────────
def calculate_weight(tickers):
    inv_vols = []
    for t in tickers:
        df = get_data(t)
        df = df.drop(['open','high','low','volume','value'], axis=1)
        df['pct_change'] = df['close'].pct_change()
        df['vol20']      = df['pct_change'].rolling(20).std()
        vol  = df['vol20'].iloc[-1]
        inv  = 1/vol if pd.notna(vol) and vol>0 else 0
        inv_vols.append(inv)
    total = sum(inv_vols)
    return [inv/total if total>0 else 0 for inv in inv_vols]

# ─── (3) 상태 로드 / 저장 ─────────────────────────────────────────────
def load_state():
    """
    state.json 구조:
    {
      "positions": {"KRW-BTC": qty, ...},
      "cash": 남은 현금(원)
    }
    """
    payload = {
        'access_key': UPBIT_ACCESS,
        'nonce': str(uuid.uuid4()),
    }

    jwt_token = jwt.encode(payload, UPBIT_SECRET)
    authorization = 'Bearer {}'.format(jwt_token)
    headers = {
        'Authorization': authorization,
    }
    res = requests.get(SEVER_URL + '/v1/accounts', headers=headers).json()
    
    wallet = defaultdict()

    for i in range(len(res)):
        if(res[i]["currency"]=="KRW"):
            wallet["KRW"] = int(res[i]["balance"])
        elif(res[i]["currency"]=="BTC"):
            wallet["KRW-BTC"] = int(res[i]["balance"])
        elif(res[i]["currency"]=="XRP"):
            wallet["KRW-XRP"] = int(res[i]["balance"])
        elif(res[i]["currency"]=="SOL"):
            wallet["KRW-MANA"] = int(res[i]["balance"])
    return wallet


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ─── (4) 리밸런싱 & 로그 저장 ──────────────────────────────────────────
def rebalance():
    
    state = load_state()
    # 현재 보유 수량 및 현금 불러오기
    quantities = {t.split('-',1)[1]: state.get(t, 0.0) for t in TICKERS}
    cash = state.get("KRW", 0.0)
    cash = 500000

    # 실시간 가격 조회 및 포트폴리오 가치 계산
    prices = {t: upbit.get_current_price(t) for t in TICKERS}
    portfolio_value = cash + sum(quantities[t.split('-',1)[1]] * prices[t] for t in TICKERS)

    # 신규 비중 계산
    new_weights_list = calculate_weight(TICKERS)
    new_weights = dict(zip(TICKERS, new_weights_list))

    now = datetime.now(TZ)
    ts = now.strftime("%Y-%m-%d %H:%M:%S")
    records = []

    # 최소 주문 금액 (Upbit 기준 5000원, 필요시 조정)
    MIN_ORDER = 5000

    print(f"[{ts}] Rebalance Start — PV: {portfolio_value:,.0f} KRW, Cash: {cash:,.0f} KRW")

    # 0.05% 수수료 설정
    FEE_RATE = 0.0005

    # 매도와 매수 주문을 분리 처리하기 위한 리스트
    actions = []

    for t in TICKERS:
        asset = t.split('-',1)[1]
        price = get_tick_price(prices[t])
        prev_qty = quantities.get(asset, 0.0)
        prev_val = prev_qty * price
        target_val = portfolio_value * new_weights[t]
        diff_krw = target_val - prev_val

        # 최소 주문 금액 미만 스킵
        if abs(diff_krw) < MIN_ORDER:
            print(f"  {t}: Skip — {abs(diff_krw):,.0f} KRW < {MIN_ORDER:,} KRW")
            continue

        if diff_krw > 0:
            # 매수: 순수 투자금 확보를 위해
            gross_krw = diff_krw / (1 - FEE_RATE)
            diff_qty = gross_krw / price
            action = "BUY"
            fee = gross_krw * FEE_RATE
        else:
            # 매도: 순수 회수금 확보를 위해
            gross_net = -diff_krw
            gross_krw = gross_net / (1 - FEE_RATE)
            diff_qty = - (gross_krw / price)
            action = "SELL"
            fee = gross_krw * FEE_RATE

        actions.append({
            "ticker": t,
            "asset": asset,
            "action": action,
            "price": price,
            "diff_qty": diff_qty,
            "diff_krw": diff_krw,
            "fee": fee
        })

    # SELL 먼저 실행
    for act in actions:
        if act["action"] != "SELL":
            continue
        t = act["ticker"]; asset = act["asset"]
        diff_qty = act["diff_qty"]; price = act["price"]
        diff_krw = act["diff_krw"]; fee = act["fee"]

        quantities[asset] += diff_qty
        cash += -diff_krw  # 순회수금만 현금에 반영

        print(f"  {t}: SELL {abs(diff_qty):.6f} units @ {price:,.0f} KRW ({diff_krw:,.0f} KRW) Fee: {fee:,.0f} KRW")
        records.append({
            "timestamp": ts,
            "ticker": t,
            "action": "SELL",
            "price": price,
            "qty": round(diff_qty, 6),
            "fee": round(fee, 0),
            "cash_after": round(cash, 0),
            "pv": round(portfolio_value, 0)
        })

    # BUY 다음 실행
    for act in actions:
        if act["action"] != "BUY":
            continue
        t = act["ticker"]; asset = act["asset"]
        diff_qty = act["diff_qty"]; price = act["price"]
        diff_krw = act["diff_krw"]; fee = act["fee"]

        quantities[asset] += diff_qty
        cash -= (diff_krw / (1 - FEE_RATE))  # 총 지출 반영

        print(f"  {t}: BUY {abs(diff_qty):.6f} units @ {price:,.0f} KRW ({diff_krw:,.0f} KRW) Fee: {fee:,.0f} KRW")
        records.append({
            "timestamp": ts,
            "ticker": t,
            "action": "BUY",
            "price": price,
            "qty": round(diff_qty, 6),
            "fee": round(fee, 0),
            "cash_after": round(cash, 0),
            "pv": round(portfolio_value, 0)
        })

    # 로그 저장
    df = pd.DataFrame(records)
    df.to_csv(LOG_FILE, mode='a', header=not os.path.exists(LOG_FILE), index=False)

    # 상태 업데이트 및 저장
    for t in TICKERS:
        asset = t.split('-',1)[1]
        state[t] = quantities.get(asset, 0.0)
    state["KRW"] = cash
    save_state(state)

    print(f"[{ts}] Rebalance Done — New Cash: {cash:,.0f} KRW\n")
    

def commit():
    # ─── (5) Git 자동 커밋 & 푸시 ────────────────────────────────────────
    try:
        # 변경된 상태 파일과 로그 파일을 커밋
        subprocess.run(["git", "-C", TRADING_DIR, "add", STATE_FILE, LOG_FILE], check=True)
        subprocess.run(["git", "-C", TRADING_DIR, "commit", "-m", f"Auto update"], check=True)
        subprocess.run(["git", "-C", TRADING_DIR, "push"], check=True)
        print("Git commit & push completed")
    except subprocess.CalledProcessError as e:
        print("Git commit/push failed:", e)

if __name__ == "__main__":
    rebalance()
    # commit()