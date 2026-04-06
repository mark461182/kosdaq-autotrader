import requests
import os
import time
import numpy as np
from dotenv import load_dotenv

load_dotenv()

APP_KEY = os.getenv("APP_KEY")
APP_SECRET = os.getenv("APP_SECRET")
BASE_URL = "https://openapivts.koreainvestment.com:29443"  # 모의투자 서버

def get_token():
    """토큰 발급 또는 저장된 토큰 불러오기 (24시간 만료 시 자동 재발급)"""
    if os.path.exists("token.txt"):
        with open("token.txt", "r") as f:
            lines = f.read().strip().splitlines()
        if len(lines) == 2:
            token, issued_at_str = lines
            try:
                issued_at = float(issued_at_str)
                if time.time() - issued_at < 86400:
                    return token
            except ValueError:
                pass

    url = f"{BASE_URL}/oauth2/tokenP"
    data = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    res = requests.post(url, json=data)
    token = res.json()["access_token"]

    with open("token.txt", "w") as f:
        f.write(token + "\n")
        f.write(str(time.time()))

    return token

def get_headers(token, tr_id):
    """API 요청 헤더 생성"""
    return {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "content-type": "application/json"
    }

def get_stock_price(token, stock_code):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = get_headers(token, "FHKST01010100")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()
    
    if data["rt_cd"] != "0" or "output" not in data:
        print(f"{stock_code} 응답: {data}")  # 어떤 응답인지 확인
        return None
    
    output = data["output"]
    return {
        "code": stock_code,
        "price": int(output["stck_prpr"]),
        "change_rate": float(output["prdy_ctrt"]),
        "volume": int(output["acml_vol"]),
        "vwap": float(output["wghn_avrg_stck_prc"])
    }
def get_minute_candles(token, stock_code, start_hour="153000"):
    """오늘 분봉 데이터 조회 (FHKST03010200, 최대 120행)
    start_hour: 조회 시작 시간 HHMMSS (이 시간부터 과거 방향으로 반환)
    """
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    headers = get_headers(token, "FHKST03010200")
    params = {
        "FID_ETC_CLS_CODE": "",
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_INPUT_HOUR_1": start_hour,
        "FID_PW_DATA_INCU_YN": "Y",
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data["rt_cd"] != "0":
        print(f"에러: {data['msg1']}")
        return None

    candles = []
    for item in data["output2"]:
        candles.append({
            "time":   item["stck_bsop_date"] + item["stck_cntg_hour"],
            "open":   int(item["stck_oprc"]),
            "high":   int(item["stck_hgpr"]),
            "low":    int(item["stck_lwpr"]),
            "close":  int(item["stck_prpr"]),
            "volume": int(item["cntg_vol"]),
        })
    return candles


def get_minute_candles_by_date(token, stock_code, date, delay=0.4):
    """특정 날짜의 1분봉 전체 조회 (FHKST03010230, VTS 지원)
    date: YYYYMMDD 문자열
    반환: 시간 오름차순 list[dict] (keys: time, open, high, low, close, volume)
    """
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
    all_candles = []
    cursor = "160000"

    for _ in range(20):  # 최대 20페이지 (1분봉 하루치 ~390개, 120개씩)
        headers = get_headers(token, "FHKST03010230")
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_DATE_1": date,
            "FID_INPUT_HOUR_1": cursor,
            "FID_PW_DATA_INCU_YN": "Y",
            "FID_FAKE_TICK_INCU_YN": "N",
        }
        res = requests.get(url, headers=headers, params=params)
        data = res.json()

        if data.get("rt_cd") != "0" or not data.get("output2"):
            break

        rows = data["output2"]
        for item in rows:
            if item["stck_bsop_date"] != date:
                continue
            all_candles.append({
                "time":   item["stck_bsop_date"] + item["stck_cntg_hour"],
                "open":   int(item["stck_oprc"]),
                "high":   int(item["stck_hgpr"]),
                "low":    int(item["stck_lwpr"]),
                "close":  int(item["stck_prpr"]),
                "volume": int(item["cntg_vol"]),
            })

        # 이전 날 데이터가 섞이거나 커서가 진행 안 되면 종료
        next_cursor = rows[-1]["stck_cntg_hour"]
        has_prev = any(r["stck_bsop_date"] < date for r in rows)
        if has_prev or next_cursor >= cursor:
            break
        cursor = next_cursor
        time.sleep(delay)

    # 시간 오름차순 정렬
    all_candles.sort(key=lambda x: x["time"])
    return all_candles

def get_prev_vwap_daily(token, stock_code):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    headers = get_headers(token, "FHKST01010400")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_ORG_ADJ_PRC": "0",
        "FID_PERIOD_DIV_CODE": "D"
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data["rt_cd"] != "0" or "output" not in data or len(data["output"]) < 2:
        return None

    prev = data["output"][1]
    high = int(prev["stck_hgpr"])
    low = int(prev["stck_lwpr"])
    close = int(prev["stck_clpr"])
    return round((high + low + close) / 3, 2)


def get_open_and_prev_close(token, stock_code):
    """오늘 시가 + 전일 종가 반환"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    headers = get_headers(token, "FHKST01010400")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_ORG_ADJ_PRC": "0",
        "FID_PERIOD_DIV_CODE": "D"
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data["rt_cd"] != "0" or "output" not in data or len(data["output"]) < 2:
        return None, None

    today = data["output"][0]
    prev = data["output"][1]
    open_price = int(today["stck_oprc"])
    prev_close = int(prev["stck_clpr"])
    return open_price, prev_close


def get_top_trading_value_stocks(token, top_n=50):
    """거래대금 상위 종목 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/volume-rank"
    headers = get_headers(token, "FHPST01710000")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_COND_SCR_DIV_CODE": "20171",
        "FID_INPUT_ISCD": "0000",
        "FID_DIV_CLS_CODE": "1",       # 0=거래량, 1=거래대금
        "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "111111111",
        "FID_TRGT_EXLS_CLS_CODE": "000000",
        "FID_INPUT_PRICE_1": "",
        "FID_INPUT_PRICE_2": "",
        "FID_VOL_CNT": "",
        "FID_INPUT_DATE_1": ""
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data.get("rt_cd") != "0" or "output" not in data:
        print(f"거래대금 상위 조회 실패: {data.get('msg1', '')}")
        return [], []

    codes = []
    names = []
    for item in data["output"][:top_n]:
        codes.append(item["mksc_shrn_iscd"])
        names.append(item["hts_kor_isnm"])

    return codes, names


def get_top_market_cap_stocks(token, top_n=50):
    """시가총액 상위 종목 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/volume-rank"
    headers = get_headers(token, "FHPST01720000")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_COND_SCR_DIV_CODE": "20172",
        "FID_INPUT_ISCD": "0000",
        "FID_DIV_CLS_CODE": "0",
        "FID_BLNG_CLS_CODE": "0",
        "FID_TRGT_CLS_CODE": "111111111",
        "FID_TRGT_EXLS_CLS_CODE": "000000",
        "FID_INPUT_PRICE_1": "",
        "FID_INPUT_PRICE_2": "",
        "FID_VOL_CNT": "",
        "FID_INPUT_DATE_1": ""
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data.get("rt_cd") != "0" or "output" not in data:
        print(f"시가총액 상위 조회 실패: {data.get('msg1', '')}")
        return [], []

    codes = []
    names = []
    for item in data["output"][:top_n]:
        codes.append(item["mksc_shrn_iscd"])
        names.append(item["hts_kor_isnm"])

    return codes, names


def get_today_ohlc(token, stock_code):
    """오늘 시가/고가/저가/종가 반환"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    headers = get_headers(token, "FHKST01010400")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_ORG_ADJ_PRC": "0",
        "FID_PERIOD_DIV_CODE": "D"
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data["rt_cd"] != "0" or "output" not in data or not data["output"]:
        return None

    today = data["output"][0]
    return {
        "open":  int(today["stck_oprc"]),
        "high":  int(today["stck_hgpr"]),
        "low":   int(today["stck_lwpr"]),
        "close": int(today["stck_clpr"]),
    }


def get_kosdaq_market_cap_range(token, min_cap_bil=500, max_cap_bil=5000):
    """코스닥 전체 종목 중 시가총액 min_cap_bil억~max_cap_bil억 범위 종목 페이지네이션 조회"""
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/volume-rank"

    all_codes, all_names = [], []
    ctx_fk, ctx_nk = "", ""

    while True:
        headers = get_headers(token, "FHPST01720000")
        if ctx_fk:
            headers["CTX_AREA_FK100"] = ctx_fk
            headers["CTX_AREA_NK100"] = ctx_nk

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20172",
            "FID_INPUT_ISCD": "0000",              # 전체 (KOSDAQ은 BLNG 코드로 필터)
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "2",              # 2 = 코스닥
            "FID_TRGT_CLS_CODE": "111111111",
            "FID_TRGT_EXLS_CLS_CODE": "000000",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": "",
            "FID_RANK_SORT_CLS_CODE": "0"          # 0 = 내림차순
        }

        res = requests.get(url, headers=headers, params=params)
        data = res.json()

        if data.get("rt_cd") != "0" or "output" not in data:
            print(f"코스닥 시가총액 조회 실패: {data.get('msg1', '')}")
            break

        done = False
        for item in data["output"]:
            cap = int(item.get("stck_avls", 0))  # 단위: 억원
            if cap < min_cap_bil:
                done = True
                break
            if cap <= max_cap_bil:
                all_codes.append(item["mksc_shrn_iscd"])
                all_names.append(item["hts_kor_isnm"])
            # cap > max_cap_bil 이면 스킵 (너무 큰 종목)

        if done:
            break

        ctx_fk = data.get("ctx_area_fk100", "").strip()
        ctx_nk = data.get("ctx_area_nk100", "").strip()
        if not ctx_fk:
            break

        time.sleep(0.3)

    print(f"코스닥 시가총액 {min_cap_bil}억~{max_cap_bil}억 종목 수: {len(all_codes)}개")
    return all_codes, all_names


def get_bollinger_band(token, stock_code, period=20):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-price"
    headers = get_headers(token, "FHKST01010400")
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": stock_code,
        "FID_ORG_ADJ_PRC": "0",
        "FID_PERIOD_DIV_CODE": "D"
    }
    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if data["rt_cd"] != "0" or "output" not in data:
        return None

    output = data["output"]
    closes = [int(d["stck_clpr"]) for d in output]  # [:period] 제거
    closes = closes[:period]  # 이렇게 분리
    
    if len(closes) < period:
        return None
    
    ma20 = np.mean(closes)
    std = np.std(closes)
    return {
        "upper": round(float(ma20 + 2 * std), 2),
        "middle": round(float(ma20), 2),
        "lower": round(float(ma20 - 2 * std), 2)
    }