#!/usr/bin/env python3
# rebalance.py

import os
import json
from datetime import datetime
import zoneinfo       
import pandas as pd
import pyupbit as upbit

# ─── 설정 ─────────────────────────────────────────────────────────────
TICKERS      = ["KRW-BTC", "KRW-XRP", "KRW-MANA"]
INPUT_VALUE  = 10000000     # 테스트용 자본(원)

# 홈 디렉토리 기준 작업 경로 설정
HOME_DIR     = os.path.expanduser("~")
TRADING_DIR  = os.path.join(HOME_DIR, "trading")
STATE_FILE   = os.path.join(TRADING_DIR, "state.json")
LOG_FILE     = os.path.join(TRADING_DIR, "rebalance_log.csv")
TZ           = zoneinfo.ZoneInfo("Asia/Seoul")

# 작업 디렉토리 자동 생성
os.makedirs(TRADING_DIR, exist_ok=True)

# ─── (1) 과거 데이터 불러오기 ───────────────────────────────────────────
def get_data(ticker, count=21):
    """pyupbit.get_ohlcv 로 21일치 일간 OHLCV 조회"""
    return upbit.get_ohlcv(ticker, count=count, interval="day")

# ─── (2) 변동성 역수 가중치 계산 ────────────────────────────────────────
def calculate_weight(tickers):
    inv_vols = []
    for t in tickers:
        df = get_data(t)
        df = df.drop(['open','high','low','volume','value'], axis=1)
        df['pct_change'] = df['close'].pct_change()
        df['vol20']     = df['pct_change'].rolling(20).std()
        vol = df['vol20'].iloc[-1]
        inv_vols.append(1/vol if pd.notna(vol) and vol>0 else 0)
    total = sum(inv_vols)
    # 합이 0이면 모두 0, 아니면 비율 계산
    return [iv/total if total>0 else 0 for iv in inv_vols]

# ─── (3) 이전 비중 로드 / 초기화 ───────────────────────────────────────
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    # 최초 실행 시: 보유 포지션이 없으므로 초기 비중 모두 0
    n = len(TICKERS)
    return {"old_weights":[0]*n}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ─── (4) 리밸런싱 & 로그 저장 ──────────────────────────────────────────
def rebalance():

    state       = load_state()
    old_weights = state["old_weights"]
    new_weights = calculate_weight(TICKERS)

    now      = datetime.now(TZ)
    ts       = now.strftime("%Y-%m-%d %H:%M:%S")
    records  = []

    print(f"[{ts}] Rebalance Start")
    for tkr, ow, nw in zip(TICKERS, old_weights, new_weights):
        old_amt = round(INPUT_VALUE * ow)
        new_amt = round(INPUT_VALUE * nw)
        diff    = new_amt - old_amt
        action  = "BUY" if diff>0 else "SELL" if diff<0 else "HOLD"

        print(f"  {tkr}: {action} {abs(diff):,} KRW")

        records.append({
            "timestamp": ts,
            "ticker":    tkr,
            "old_weight":ow,
            "new_weight":nw,
            "old_amt":   old_amt,
            "new_amt":   new_amt,
            "diff":      diff,
            "action":    action
        })

    # 로그 CSV에 누적
    df = pd.DataFrame(records)
    if os.path.exists(LOG_FILE):
        df.to_csv(LOG_FILE, mode='a', header=False, index=False)
    else:
        df.to_csv(LOG_FILE, index=False)

    # 다음날 비교를 위해 비중 저장
    state["old_weights"] = new_weights
    save_state(state)

    print(f"[{ts}] Rebalance Done\n")

if __name__ == "__main__":
    rebalance()