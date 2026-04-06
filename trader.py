"""
실시간 틱 기반 자동매매 모듈

매수 조건 — 아래 3개 중 2개 이상 충족 시 진입:
  1. 거래량 급증:   현재 체결량 >= 직전 30틱 평균 × 2
  2. 체결강도:      누적 매수체결량 / 매도체결량 >= 110%
  3. 매도잔량 소진: 최우선 매도호가 잔량 < 직전 평균의 30%

청산 조건:
  - 손절:          매수가 대비 -2%
  - 트레일링 스탑: 고점 대비 -1.5%
  - 강제 청산:     15:20

매수 진입 허용 시간: 09:30 ~ 14:30
"""

import json
import time
import os
import websocket
import requests
from collections import deque
from datetime import datetime
from dotenv import load_dotenv
from api import get_token, get_stock_price

load_dotenv()

APP_KEY        = os.getenv("APP_KEY")
APP_SECRET     = os.getenv("APP_SECRET")
ACCOUNT_NO     = os.getenv("ACCOUNT_NO")
ACCOUNT_NO_SEQ = os.getenv("ACCOUNT_NO_SEQ")
BASE_URL = "https://openapivts.koreainvestment.com:29443"
WS_URL   = "ws://ops.koreainvestment.com:21000"

# ── 시간 설정 ──────────────────────────────────────────────
BUY_START  = (9, 30)
BUY_END    = (14, 30)
FORCE_CLOSE   = (15, 20)
MARKET_CLOSE  = (15, 30)

# ── 매수 조건 파라미터 ──────────────────────────────────────
VOL_WARMUP       = 30    # 거래량 판단 최소 틱 수
VOL_SPIKE_MULT   = 2.0   # 거래량 급증 배수
STRENGTH_MIN     = 110.0 # 체결강도 최소값 (%)
ASK_DEPLETE      = 0.3   # 소진 판정: 직전 평균 대비 이 비율 미만
ASK_RECOVER      = 0.8   # 회복 판정: 직전 평균 대비 이 비율 초과
BUY_COND_NEEDED  = 2     # 3개 조건 중 통과해야 할 최소 개수

# ── 청산 파라미터 ───────────────────────────────────────────
STOP_LOSS_PCT    = -2.0  # 손절 %
TRAILING_PCT     = -1.5  # 트레일링 스탑 %

# ── WebSocket 재연결 ────────────────────────────────────────
MAX_RECONNECT   = 5
RECONNECT_DELAY = 5

# 종목별 상태 (모듈 전역)
stock_state: dict = {}


# ── 상태 초기화 ─────────────────────────────────────────────

def init_stock_state(code: str, weight: float = 1.0) -> None:
    stock_state[code] = {
        "volumes":       deque(maxlen=60),
        "buy_vols":      deque(maxlen=60),
        "sell_vols":     deque(maxlen=60),
        "ask1_qty":      0,
        "ask1_qty_hist": deque(maxlen=5),
        "ask_depleted":  False,
        "holding":       False,
        "buy_price":     0,
        "high_price":    0,
        "weight":        weight,
    }


# ── 시간 헬퍼 ───────────────────────────────────────────────

def _hm(pair: tuple) -> tuple:
    return pair

def _now_hm() -> tuple:
    n = datetime.now()
    return (n.hour, n.minute)

def is_buy_hours() -> bool:
    """매수 진입 허용: 09:30 이상, 14:30 미만"""
    t = _now_hm()
    return BUY_START <= t < BUY_END

def is_force_close_time() -> bool:
    return _now_hm() >= FORCE_CLOSE

def is_market_closed() -> bool:
    return _now_hm() >= MARKET_CLOSE


# ── API 헬퍼 ────────────────────────────────────────────────

def _headers(token: str, tr_id: str) -> dict:
    return {
        "authorization": f"Bearer {token}",
        "appkey":        APP_KEY,
        "appsecret":     APP_SECRET,
        "tr_id":         tr_id,
        "content-type":  "application/json",
    }

def get_approval_key() -> str:
    res = requests.post(
        f"{BASE_URL}/oauth2/Approval",
        json={"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": APP_SECRET},
    )
    return res.json()["approval_key"]

def get_available_cash(token: str) -> int:
    res = requests.get(
        f"{BASE_URL}/uapi/domestic-stock/v1/trading/inquire-psbl-order",
        headers=_headers(token, "VTTC8908R"),
        params={
            "CANO": ACCOUNT_NO, "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
            "PDNO": "005930", "ORD_UNPR": "0", "ORD_DVSN": "01",
            "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N",
        },
    )
    data = res.json()
    if data.get("rt_cd") != "0":
        print(f"예수금 조회 실패: {data.get('msg1', '')}")
        return 0
    cash = int(data["output"]["ord_psbl_cash"])
    print(f"주문 가능 예수금: {cash:,}원")
    return cash

def get_holding_qty(token: str, code: str) -> int:
    res = requests.get(
        f"{BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance",
        headers=_headers(token, "VTTC8434R"),
        params={
            "CANO": ACCOUNT_NO, "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
            "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
            "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N", "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
        },
    )
    data = res.json()
    if data.get("rt_cd") != "0":
        print(f"잔고 조회 실패: {data.get('msg1', '')}")
        return 0
    for item in data.get("output1", []):
        if item.get("pdno") == code:
            return int(item.get("hldg_qty", 0))
    return 0

def buy_order(token: str, code: str, price: int) -> bool:
    """비중 기반 수량 계산 후 시장가 매수. 성공 여부 반환."""
    weight = stock_state.get(code, {}).get("weight", 1.0)
    cash = get_available_cash(token)
    if cash <= 0:
        print(f"{code} 예수금 부족 — 매수 취소")
        return False

    qty = int(cash * weight) // price
    if qty <= 0:
        print(f"{code} 배분금액({int(cash*weight):,}원)으로 {price:,}원짜리 매수 불가")
        return False

    print(f"{code} 매수: 비중 {weight*100:.1f}% | {price:,}원 × {qty}주")
    res = requests.post(
        f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash",
        headers=_headers(token, "VTTC0802U"),
        json={
            "CANO": ACCOUNT_NO, "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
            "PDNO": code, "ORD_DVSN": "01", "ORD_QTY": str(qty), "ORD_UNPR": "0",
        },
    )
    result = res.json()
    print(f"매수 결과: {result}")
    return result.get("rt_cd") == "0"

def sell_order(token: str, code: str, price: int) -> None:
    """실제 보유 수량 전량 시장가 매도."""
    qty = get_holding_qty(token, code)
    if qty <= 0:
        print(f"{code} 보유 수량 없음 — 매도 취소")
        return
    res = requests.post(
        f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash",
        headers=_headers(token, "VTTC0801U"),
        json={
            "CANO": ACCOUNT_NO, "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
            "PDNO": code, "ORD_DVSN": "01", "ORD_QTY": str(qty), "ORD_UNPR": "0",
        },
    )
    print(f"매도 결과 ({qty}주): {res.json()}")


# ── 체결강도 ────────────────────────────────────────────────

def calc_execution_strength(code: str) -> float:
    state = stock_state.get(code)
    if not state:
        return 0.0
    sell_sum = sum(state["sell_vols"])
    if sell_sum == 0:
        return 0.0
    return sum(state["buy_vols"]) / sell_sum * 100


# ── 호가창 처리 ─────────────────────────────────────────────

def on_orderbook_message(code: str, data: list) -> None:
    """H0STASP0 — 최우선 매도호가 잔량 소진 감지.

    필드 레이아웃 (KIS H0STASP0 ^구분):
      [0]  종목코드  [1] 영업시간
      [2~11]  매도호가1~10    [12~21] 매수호가1~10
      [22~31] 매도호가잔량1~10  ← [22] 최우선 매도호가 잔량
      [32~41] 매수호가잔량1~10
    """
    state = stock_state.get(code)
    if not state:
        return
    try:
        ask1_qty = int(data[22])
    except (IndexError, ValueError):
        return

    hist = state["ask1_qty_hist"]
    hist.append(ask1_qty)

    if len(hist) >= 3:
        prev_avg = sum(list(hist)[:-1]) / (len(hist) - 1)
        if prev_avg > 0:
            if ask1_qty < prev_avg * ASK_DEPLETE:
                if not state["ask_depleted"]:
                    print(f"{code} 매도잔량 소진 감지 | ask1: {ask1_qty} (직전평균: {prev_avg:.0f})")
                state["ask_depleted"] = True
            elif ask1_qty > prev_avg * ASK_RECOVER:
                state["ask_depleted"] = False

    state["ask1_qty"] = ask1_qty


# ── 매수 조건 ───────────────────────────────────────────────

def check_buy_condition(token: str, code: str, price: int, volume: int) -> None:
    """3개 조건 중 2개 이상 충족 시 매수."""
    state = stock_state.get(code)
    if not state or state["holding"]:
        return

    if not is_buy_hours():
        return

    state["volumes"].append(volume)
    if len(state["volumes"]) < VOL_WARMUP:
        return

    avg_vol  = sum(list(state["volumes"])[:-1]) / (len(state["volumes"]) - 1)
    strength = calc_execution_strength(code)

    cond_vol = avg_vol > 0 and volume >= avg_vol * VOL_SPIKE_MULT
    cond_str = strength >= STRENGTH_MIN
    cond_ask = state["ask_depleted"]

    met = sum([cond_vol, cond_str, cond_ask])
    if met < BUY_COND_NEEDED:
        return

    print(
        f"매수 시그널 | {code} | 현재가: {price:,} | 조건 {met}/3 | "
        f"거래량급증: {'O' if cond_vol else 'X'} ({volume} / avg {avg_vol:.0f}) | "
        f"체결강도: {'O' if cond_str else 'X'} ({strength:.1f}%) | "
        f"매도잔량소진: {'O' if cond_ask else 'X'}"
    )
    if buy_order(token, code, price):
        state["holding"]   = True
        state["buy_price"] = price
        state["high_price"] = price


# ── 청산 조건 ───────────────────────────────────────────────

def check_sell_condition(token: str, code: str, price: int) -> None:
    """손절 -2%, 트레일링 스탑 -1.5%."""
    state = stock_state[code]
    buy_price = state["buy_price"]

    if price > state["high_price"]:
        state["high_price"] = price

    change_rate   = (price - buy_price)         / buy_price         * 100
    trailing_rate = (price - state["high_price"]) / state["high_price"] * 100

    if change_rate <= STOP_LOSS_PCT:
        print(f"손절 | {code} | 수익률: {change_rate:.2f}%")
        sell_order(token, code, price)
        state["holding"] = False

    elif trailing_rate <= TRAILING_PCT:
        print(f"트레일링 스탑 | {code} | 고점 대비: {trailing_rate:.2f}%")
        sell_order(token, code, price)
        state["holding"] = False


# ── WebSocket 핸들러 ─────────────────────────────────────────

def on_message(ws, message: str, token: str) -> None:
    if not message:
        return

    if is_force_close_time():
        print("15:20 강제 청산 시각 도달 — WebSocket 종료")
        ws.close()
        return

    if message[0] in ('0', '1'):
        try:
            parts = message.split('|')
            trid  = parts[1]

            if trid == "H0STCNT0":
                data   = parts[3].split('^')
                code   = data[0]
                price  = int(data[2])
                volume = int(data[9])

                state = stock_state.get(code)
                if state and len(data) > 8:
                    # data[8]: '1'=매도체결, '2'=매수체결
                    if data[8] == '2':
                        state["buy_vols"].append(volume)
                    else:
                        state["sell_vols"].append(volume)

                print(f"{code} | 현재가: {price:,} | 체결량: {volume}")

                if state and state["holding"]:
                    check_sell_condition(token, code, price)
                else:
                    check_buy_condition(token, code, price, volume)

            elif trid == "H0STASP0":
                data = parts[3].split('^')
                on_orderbook_message(data[0], data)

        except (IndexError, ValueError) as e:
            print(f"메시지 파싱 오류 (무시): {e} | 원본: {message[:80]}")
    else:
        try:
            data = json.loads(message)
            print(f"응답: {data.get('body', {}).get('msg1', '')}")
        except json.JSONDecodeError:
            pass


def on_error(_ws, error) -> None:
    print(f"오류: {error}")

def on_close(_ws, _code, _msg) -> None:
    print("연결 종료")

def on_open(ws, approval_key: str, target_list: list) -> None:
    print("WebSocket 연결됨")
    for code in target_list:
        for tr_id in ("H0STCNT0", "H0STASP0"):
            ws.send(json.dumps({
                "header": {
                    "approval_key": approval_key,
                    "custtype": "P",
                    "tr_type": "1",
                    "content-type": "utf-8",
                },
                "body": {"input": {"tr_id": tr_id, "tr_key": code}},
            }))
            time.sleep(0.1)
        print(f"{code} 구독 등록 (체결 + 호가창)")


# ── 재연결 후 보유 종목 즉시 점검 ──────────────────────────────

def check_holdings_after_reconnect(token: str) -> None:
    holding = [c for c, s in stock_state.items() if s["holding"]]
    if not holding:
        return
    print(f"재연결 후 보유 종목 점검: {holding}")
    for code in holding:
        price_data = get_stock_price(token, code)
        if not price_data:
            print(f"  {code} 현재가 조회 실패 — 건너뜀")
            continue
        check_sell_condition(token, code, price_data["price"])
        time.sleep(0.3)


def _make_on_open(approval_key: str, target_list: list, token: str, is_reconnect: bool):
    def handler(ws):
        on_open(ws, approval_key, target_list)
        if is_reconnect:
            check_holdings_after_reconnect(token)
    return handler


# ── 장 마감 강제 청산 ────────────────────────────────────────

def sell_all_holdings(token: str) -> None:
    holding = [c for c, s in stock_state.items() if s["holding"]]
    if not holding:
        print("보유 종목 없음 — 청산 생략")
        return
    print(f"보유 종목 전량 청산: {holding}")
    for code in holding:
        price_data = get_stock_price(token, code)
        price = price_data["price"] if price_data else stock_state[code]["buy_price"]
        sell_order(token, code, price)
        stock_state[code]["holding"] = False
        time.sleep(0.5)


# ── 진입점 ──────────────────────────────────────────────────

def start_trading(target_list: list, weights: list = None, token: str = None) -> None:
    """매매 시작.

    Args:
        target_list: 종목코드 리스트
        weights:     종목별 자금 배분 비중 (None 이면 균등 배분)
        token:       한투 API 액세스 토큰 (None 이면 자동 발급)
    """
    if token is None:
        token = get_token()

    n = len(target_list)
    if weights is None:
        weights = [round(1.0 / n, 4)] * n if n > 0 else []

    for code, w in zip(target_list, weights):
        init_stock_state(code, weight=w)
        print(f"{code} 등록 | 비중: {w*100:.1f}%")

    reconnect_count = 0

    while not is_force_close_time() and not is_market_closed():
        approval_key = get_approval_key()
        is_reconnect = reconnect_count > 0

        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=_make_on_open(approval_key, target_list, token, is_reconnect),
            on_message=lambda ws, msg, t=token: on_message(ws, msg, t),
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever()

        if is_force_close_time():
            print("15:20 강제 청산 시각 — 종료")
            break
        if is_market_closed():
            print("장 마감 — 종료")
            break

        reconnect_count += 1
        if reconnect_count > MAX_RECONNECT:
            print(f"재연결 {MAX_RECONNECT}회 초과 — 매매 종료")
            break

        print(f"연결 끊김. {RECONNECT_DELAY}초 후 재연결... ({reconnect_count}/{MAX_RECONNECT})")
        time.sleep(RECONNECT_DELAY)

    sell_all_holdings(token)
    print("자동매매 종료")
