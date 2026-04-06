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

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
ACCOUNT_NO = os.getenv("ACCOUNT_NO")
ACCOUNT_NO_SEQ = os.getenv("ACCOUNT_NO_SEQ")
BASE_URL = "https://openapivts.koreainvestment.com:29443"
WS_URL = "ws://ops.koreainvestment.com:21000"

MARKET_CLOSE_HOUR = 15
MARKET_CLOSE_MINUTE = 30

FORCE_CLOSE_HOUR = 15
FORCE_CLOSE_MINUTE = 20

MAX_RECONNECT = 5       # 최대 재연결 시도 횟수
RECONNECT_DELAY = 5     # 재연결 대기 초

# 종목별 상태 관리
stock_state = {}

def init_stock_state(code, open_price, prev_close, weight=1.0, bb_upper=0):
    """종목 초기 상태 설정"""
    gap = (open_price - prev_close) / prev_close * 100
    stock_state[code] = {
        "volumes": deque(maxlen=60),
        "buy_vols": deque(maxlen=60),   # 매수 체결량 롤링 윈도우
        "sell_vols": deque(maxlen=60),  # 매도 체결량 롤링 윈도우
        "ask1_qty": 0,                  # 최우선 매도호가 잔량
        "ask1_qty_hist": deque(maxlen=5),  # 매도1호가 잔량 히스토리
        "ask_depleted": False,          # 매도 잔량 소진 플래그
        "open_price": open_price,
        "prev_close": prev_close,
        "gap": gap,
        "holding": False,
        "buy_price": 0,
        "high_price": 0,
        "gap_ok": gap <= 10.0,
        "weight": weight,
        "bb_upper": bb_upper,           # 볼린저밴드 상단 (트레일링 스탑 홀드 기준)
    }

def is_market_closed():
    """장 마감 여부 확인 (15:30 이후)"""
    now = datetime.now()
    return now.hour > MARKET_CLOSE_HOUR or (
        now.hour == MARKET_CLOSE_HOUR and now.minute >= MARKET_CLOSE_MINUTE
    )

def is_force_close_time():
    """15:20 강제 청산 시각 도달 여부"""
    now = datetime.now()
    return now.hour > FORCE_CLOSE_HOUR or (
        now.hour == FORCE_CLOSE_HOUR and now.minute >= FORCE_CLOSE_MINUTE
    )

def get_available_cash(token):
    """주문 가능 예수금 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "VTTC8908R",
        "content-type": "application/jsoㅏn"
    }
    params = {
        "CANO": ACCOUNT_NO,
        "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
        "PDNO": "005930",
        "ORD_UNPR": "0",
        "ORD_DVSN": "01",
        "CMA_EVLU_AMT_ICLD_YN": "N",
        "OVRS_ICLD_YN": "N"
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()
    if data.get("rt_cd") != "0":
        print(f"예수금 조회 실패: {data.get('msg1', '')}")
        return 0
    cash = int(data["output"]["ord_psbl_cash"])
    print(f"주문 가능 예수금: {cash:,}원")
    return cash

def get_approval_key():
    url = f"{BASE_URL}/oauth2/Approval"
    data = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "secretkey": APP_SECRET
    }
    res = requests.post(url, json=data)
    return res.json()["approval_key"]

def buy_order(token, code, price):
    """모의투자 매수 주문 (비중 기반 수량 계산). 성공 여부 반환."""
    state = stock_state.get(code, {})
    weight = state.get("weight", 1.0)

    cash = get_available_cash(token)
    if cash <= 0:
        print(f"{code} 예수금 부족으로 매수 취소")
        return False

    alloc_amount = int(cash * weight)
    qty = alloc_amount // price
    if qty <= 0:
        print(f"{code} 배분 금액({alloc_amount:,}원)으로 {price:,}원짜리 매수 불가")
        return False

    print(f"{code} 매수: 비중 {weight*100:.1f}% | 배분금액 {alloc_amount:,}원 | {price:,}원 × {qty}주")

    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "VTTC0802U",
        "content-type": "application/json"
    }
    data = {
        "CANO": ACCOUNT_NO,
        "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0"
    }
    res = requests.post(url, headers=headers, json=data)
    result = res.json()
    print(f"매수 주문 결과: {result}")
    return result.get("rt_cd") == "0"

def get_holding_qty(token, code):
    """실제 보유 수량 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "VTTC8434R",
        "content-type": "application/json"
    }
    params = {
        "CANO": ACCOUNT_NO,
        "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()
    if data.get("rt_cd") != "0":
        print(f"잔고 조회 실패: {data.get('msg1', '')}")
        return 0
    for item in data.get("output1", []):
        if item.get("pdno") == code:
            return int(item.get("hldg_qty", 0))
    return 0

def sell_order(token, code, price):
    """모의투자 매도 주문 (실제 보유 수량 전량)"""
    qty = get_holding_qty(token, code)
    if qty <= 0:
        print(f"{code} 보유 수량 없음 — 매도 취소")
        return

    url = f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "VTTC0801U",
        "content-type": "application/json"
    }
    data = {
        "CANO": ACCOUNT_NO,
        "ACNT_PRDT_CD": ACCOUNT_NO_SEQ,
        "PDNO": code,
        "ORD_DVSN": "01",
        "ORD_QTY": str(qty),
        "ORD_UNPR": "0"
    }
    res = requests.post(url, headers=headers, json=data)
    print(f"매도 주문 ({qty}주): {res.json()}")

def sell_all_holdings(token):
    """장 마감 시 보유 종목 처리: 조건 체크 후 미청산 포지션만 강제 청산"""
    holding_codes = [code for code, s in stock_state.items() if s["holding"]]
    if not holding_codes:
        print("보유 종목 없음. 청산 생략.")
        return

    print(f"장 마감 포지션 점검: {holding_codes}")
    for code in holding_codes:
        price_data = get_stock_price(token, code)
        if not price_data:
            print(f"  {code} 현재가 조회 실패 — 청산 건너뜀")
            time.sleep(0.5)
            continue

        price = price_data["price"]
        state = stock_state[code]
        buy_price = state["buy_price"]
        change_rate = (price - buy_price) / buy_price * 100
        trailing_rate = (price - state["high_price"]) / state["high_price"] * 100

        if change_rate <= -2.0:
            print(f"  {code} 손절 청산 | 수익률: {change_rate:.2f}%")
            sell_order(token, code, price)
            state["holding"] = False
        elif trailing_rate <= -1.5:
            print(f"  {code} 트레일링 스탑 청산 | 고점 대비: {trailing_rate:.2f}%")
            sell_order(token, code, price)
            state["holding"] = False
        else:
            # 조건 미달이지만 장 마감이므로 강제 청산
            print(f"  {code} 장마감 강제 청산 | 현재가: {price:,} | 수익률: {change_rate:.2f}%")
            sell_order(token, code, price)
            state["holding"] = False

        time.sleep(0.5)

def check_holdings_after_reconnect(token):
    """재연결 후 보유 종목 현재가 조회 → 손절/청산 조건 즉시 체크"""
    holding_codes = [code for code, s in stock_state.items() if s["holding"]]
    if not holding_codes:
        return

    print(f"재연결 후 보유 종목 점검: {holding_codes}")
    for code in holding_codes:
        price_data = get_stock_price(token, code)
        if not price_data:
            print(f"  {code} 현재가 조회 실패 — 점검 건너뜀")
            continue
        price = price_data["price"]
        print(f"  {code} 재연결 후 현재가: {price:,}")
        check_sell_condition(token, code, price)
        time.sleep(0.3)

def calc_execution_strength(code):
    """체결강도 계산: 매수체결량 합계 / 매도체결량 합계 × 100"""
    state = stock_state.get(code)
    if not state:
        return 0.0
    buy_sum = sum(state["buy_vols"])
    sell_sum = sum(state["sell_vols"])
    if sell_sum == 0:
        return 0.0
    return buy_sum / sell_sum * 100


def on_orderbook_message(code, data):
    """H0STASP0 호가 데이터 처리 및 매도1호가 잔량 소진 감지.

    H0STASP0 ^ 구분 필드 레이아웃 (KIS 기준):
      [0]  종목코드
      [1]  영업시간
      [2~11]  매도호가1~10
      [12~21] 매수호가1~10
      [22~31] 매도호가잔량1~10  ← [22] = 최우선 매도호가 잔량
      [32~41] 매수호가잔량1~10
      [42] 매도호가총잔량
      [43] 매수호가총잔량
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
            if ask1_qty < prev_avg * 0.3:
                if not state["ask_depleted"]:
                    print(f"{code} 매도잔량 소진 감지 | ask1: {ask1_qty} (직전평균: {prev_avg:.0f})")
                state["ask_depleted"] = True
            elif ask1_qty > prev_avg * 0.8:
                state["ask_depleted"] = False

    state["ask1_qty"] = ask1_qty


def check_buy_condition(_ws, token, code, price, volume):
    """매수 조건 체크"""
    state = stock_state.get(code)
    if not state:
        return

    if state["holding"]:
        check_sell_condition(token, code, price)
        return

    if not state["gap_ok"]:
        return

    state["volumes"].append(volume)

    if len(state["volumes"]) < 30:
        return

    avg_volume = sum(list(state["volumes"])[:-1]) / (len(state["volumes"]) - 1)

    if avg_volume > 0 and volume >= avg_volume * 2:
        change_rate = (price - state["open_price"]) / state["open_price"] * 100
        if change_rate > 0:
            # 체결강도 110% 이상 조건
            strength = calc_execution_strength(code)
            if strength < 110.0:
                print(f"  {code} 체결강도 미달: {strength:.1f}% (최소 110%)")
                return

            # 매도 잔량 소진 조건
            if not state["ask_depleted"]:
                print(f"  {code} 매도잔량 소진 미감지 (ask1: {state['ask1_qty']})")
                return

            print(
                f"매수 시그널 | {code} | 현재가: {price} | "
                f"거래량 급증: {volume} (평균: {avg_volume:.0f}) | "
                f"체결강도: {strength:.1f}% | 매도잔량 소진"
            )
            if buy_order(token, code, price):
                state["holding"] = True
                state["buy_price"] = price
                state["high_price"] = price

def check_sell_condition(token, code, price):
    """매도 조건 체크 (손절 -2%, 트레일링 스탑 -1.5%).
    BB 상단 터치 구간(price >= bb_upper)에서는 트레일링 스탑 비활성화.
    """
    state = stock_state[code]
    buy_price = state["buy_price"]

    if price > state["high_price"]:
        state["high_price"] = price

    change_rate = (price - buy_price) / buy_price * 100
    trailing_rate = (price - state["high_price"]) / state["high_price"] * 100

    if change_rate <= -2.0:
        print(f"손절 | {code} | 수익률: {change_rate:.2f}%")
        sell_order(token, code, price)
        state["holding"] = False

    elif trailing_rate <= -1.5:
        bb_upper = state["bb_upper"]
        if bb_upper > 0 and price >= bb_upper:
            print(f"트레일링 스탑 홀드 | {code} | BB상단({bb_upper:,}) 터치 구간 — 매도 보류")
        else:
            print(f"트레일링 스탑 | {code} | 고점 대비: {trailing_rate:.2f}%")
            sell_order(token, code, price)
            state["holding"] = False

def on_message(ws, message, token):
    if not message:
        return

    if is_force_close_time():
        print("15:20 강제 청산 시각 도달 — WebSocket 종료")
        ws.close()
        return

    if message[0] in ('0', '1'):
        try:
            recvstr = message.split('|')
            trid = recvstr[1]

            if trid == "H0STCNT0":
                data = recvstr[3].split('^')
                code = data[0]
                price = int(data[2])
                volume = int(data[9])
                # 체결구분 추적: data[8] 기준 '1'=매도, '2'=매수
                state = stock_state.get(code)
                if state and len(data) > 8:
                    if data[8] == '2':
                        state["buy_vols"].append(volume)
                    else:
                        state["sell_vols"].append(volume)
                print(f"{code} | 현재가: {price} | 체결량: {volume}")
                check_buy_condition(ws, token, code, price, volume)

            elif trid == "H0STASP0":
                data = recvstr[3].split('^')
                code = data[0]
                on_orderbook_message(code, data)
        except (IndexError, ValueError) as e:
            print(f"메시지 파싱 오류 (무시): {e} | 원본: {message[:80]}")
    else:
        try:
            data = json.loads(message)
            print(f"응답: {data.get('body', {}).get('msg1', '')}")
        except json.JSONDecodeError as e:
            print(f"JSON 파싱 오류 (무시): {e}")

def on_error(_ws, error):
    print(f"오류: {error}")

def on_close(_ws, _close_status_code, _close_msg):
    print("연결 종료")

def on_open(ws, approval_key, target_list):
    print("WebSocket 연결됨")
    for code in target_list:
        for tr_id in ("H0STCNT0", "H0STASP0"):
            msg = {
                "header": {
                    "approval_key": approval_key,
                    "custtype": "P",
                    "tr_type": "1",
                    "content-type": "utf-8"
                },
                "body": {
                    "input": {
                        "tr_id": tr_id,
                        "tr_key": code
                    }
                }
            }
            ws.send(json.dumps(msg))
            time.sleep(0.1)
        print(f"{code} 구독 등록 (체결 + 호가창)")

def make_on_open_handler(approval_key, target_list, token, is_reconnect):
    """on_open 핸들러 생성 (클로저 캡처 + 재연결 시 보유 종목 즉시 점검)"""
    def handler(ws):
        on_open(ws, approval_key, target_list)
        if is_reconnect:
            check_holdings_after_reconnect(token)
    return handler

def start_trading(target_list, open_prices, prev_closes, weights=None, bb_uppers=None, token=None):
    """매매 시작 (장 마감 자동 종료 + WebSocket 재연결 포함)"""
    if token is None:
        token = get_token()

    if weights is None:
        n = len(target_list)
        weights = [round(1.0 / n, 4)] * n if n > 0 else []

    if bb_uppers is None:
        bb_uppers = [0] * len(target_list)

    for code, open_p, prev_c, w, bb_u in zip(target_list, open_prices, prev_closes, weights, bb_uppers):
        init_stock_state(code, open_p, prev_c, weight=w, bb_upper=bb_u)
        gap = stock_state[code]["gap"]
        print(f"{code} | 시가: {open_p} | 갭: {gap:.2f}% | 비중: {w*100:.1f}% | BB상단: {bb_u:,} | {'통과' if gap <= 10 else '제외'}")

    reconnect_count = 0

    while not is_force_close_time() and not is_market_closed():
        approval_key = get_approval_key()
        is_reconnect = reconnect_count > 0

        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=make_on_open_handler(approval_key, target_list, token, is_reconnect),
            on_message=lambda ws, msg, t=token: on_message(ws, msg, t),
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()

        if is_force_close_time():
            print("15:20 강제 청산 시각 — 청산 후 종료")
            break

        if is_market_closed():
            print("장 마감으로 WebSocket 종료")
            break

        reconnect_count += 1
        if reconnect_count > MAX_RECONNECT:
            print(f"재연결 {MAX_RECONNECT}회 초과 — 매매 종료")
            break

        print(f"연결 끊김. {RECONNECT_DELAY}초 후 재연결 시도... ({reconnect_count}/{MAX_RECONNECT})")
        time.sleep(RECONNECT_DELAY)

    # 청산 (15:20 강제 청산 또는 장 마감 청산)
    sell_all_holdings(token)
    print("자동매매 종료")
