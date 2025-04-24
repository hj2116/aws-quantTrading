import os
import json
from datetime import datetime
import zoneinfo
import pandas as pd
import pyupbit as upbit

# ─── 설정 ─────────────────────────────────────────────────────────────
TICKERS      = ["KRW-BTC", "KRW-XRP", "KRW-MANA"]
INPUT_VALUE  = 10_000_000     # 초기 자본(원)

# 홈 디렉토리 기준 작업 경로 설정
HOME_DIR     = os.path.expanduser("~")
TRADING_DIR  = os.path.join(HOME_DIR, "trading")
STATE_FILE   = os.path.join(TRADING_DIR, "state.json")
LOG_FILE     = os.path.join(TRADING_DIR, "rebalancing_log.csv")
TZ           = zoneinfo.ZoneInfo("Asia/Seoul")

# 작업 디렉토리 자동 생성
os.makedirs(TRADING_DIR, exist_ok=True)

# ─── (1) 과거 데이터 불러오기 ───────────────────────────────────────────
def get_data(ticker, count=21):
    """
    pyupbit.get_ohlcv 로 count일치 일간 OHLCV 조회 후 반환.
    """
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
      "positions": {"KRW-BTC": qty, "KRW-XRP": qty, ...},
      "cash": 남은 현금(원)
    }
    """
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    # 최초 실행 시: 현금만 보유, 포지션 0
    return {
        "positions": {t: 0.0 for t in TICKERS},
        "cash": INPUT_VALUE
    }

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ─── (4) 리밸런싱 & 로그 저장 ──────────────────────────────────────────
def rebalance():
    state        = load_state()
    positions    = state["positions"]
    cash         = state["cash"]
    # 이전 포지션 가치 실시간 체결가로 계산
    old_weights = []
    for t in TICKERS:
        price = upbit.get_current_price(t)
        value = positions.get(t, 0) * price
        old_weights.append(value)
    # 포트폴리오 가치 계산
    last_prices = {}
    for t in TICKERS:
        last_prices[t] = upbit.get_current_price(t)

    pv = cash
    for t in TICKERS:
        pv += positions[t] * last_prices[t]
    # 실제 old_weights 비율
    temp_weights = []
    if pv > 0:
        for t in TICKERS:
            temp_weights.append((positions[t] * last_prices[t]) / pv)
    else:
        for t in TICKERS:
            temp_weights.append(0)
    old_weights = temp_weights

    # 새 가중치 및 목표 달러 배분
    new_weights   = calculate_weight(TICKERS)
    target_dollars = {}
    for t, w in zip(TICKERS, new_weights):
        target_dollars[t] = pv * w

    now  = datetime.now(TZ)
    ts   = now.strftime("%Y-%m-%d %H:%M:%S")
    records = []

    print(f"[{ts}] Rebalance Start — PV: {pv:,.0f} KRW  Cash: {cash:,.0f} KRW")
    for t, ow, nw in zip(TICKERS, old_weights, new_weights):
        current_val = positions[t] * last_prices[t]
        diff_krw    = target_dollars[t] - current_val
        qty         = diff_krw / last_prices[t] if last_prices[t]>0 else 0

        action = "BUY" if qty>0 else "SELL" if qty<0 else "HOLD"
        # 상태 업데이트
        positions[t] += qty
        cash        -= qty * last_prices[t]

        print(f"  {t}: {action} {abs(qty):.6f} units @ {last_prices[t]:,.0f} KRW")

        records.append({
            "timestamp":    ts,
            "ticker":       t,
            "action":       action,
            "price":        float(last_prices[t]),
            "qty":          float(qty),
            "cash_after":   float(cash),
            "portfolio_value": float(pv)
        })

    # 로그 CSV에 누적 저장
    df = pd.DataFrame(records)
    header = not os.path.exists(LOG_FILE)
    df.to_csv(LOG_FILE, mode='a', header=header, index=False)

    # 상태 저장
    state["positions"] = positions
    state["cash"]      = cash
    save_state(state)

    print(f"[{ts}] Rebalance Done — New Cash: {cash:,.0f} KRW\n")

if __name__ == "__main__":
    rebalance()