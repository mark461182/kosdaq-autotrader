"""
KOSDAQ 중소형주 분봉 백테스트
전략: VWAP + Bollinger Bands + 거래량 급증 조건 (1분봉)
"""

from __future__ import annotations

import warnings
warnings.filterwarnings("ignore")

import os
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pykrx import stock
from api import get_token, get_minute_candles_by_date

try:
    import FinanceDataReader as fdr
    _HAS_FDR = True
except ImportError:
    _HAS_FDR = False

try:
    import pyarrow  # noqa: F401
    _HAS_PARQUET = True
except ImportError:
    try:
        import fastparquet  # noqa: F401
        _HAS_PARQUET = True
    except ImportError:
        _HAS_PARQUET = False


# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────
END_DATE   = datetime.today().strftime("%Y%m%d")
START_DATE = (datetime.today() - timedelta(days=90)).strftime("%Y%m%d")

# Bollinger Bands (분봉 기준)
BB_PERIOD  = 20
BB_STD     = 2.0

# 거래량 급증
VOL_SPIKE  = 2.0
VOL_MA     = 20

# 진입/청산 기준
STOP_LOSS        = -0.03   # -3% 손절
MAX_HOLD_MINUTES = 120     # 최대 보유 시간 (분)

# 장 시간 필터 (개장 초반 / 동시호가 제외)
MARKET_OPEN  = "09:05"
MARKET_CLOSE = "15:19"

# 시가총액 필터 (억 원)
MKTCAP_MIN = 300
MKTCAP_MAX = 5_000

# 캐시 / 성능
CACHE_DIR   = "cache_minute"
API_DELAY   = 0.15    # pykrx 부하 방지 (초)
MAX_TICKERS = 200     # None 이면 전체 유니버스 사용

os.makedirs(CACHE_DIR, exist_ok=True)

# 한투 API 토큰 (main()에서 초기화)
_KIS_TOKEN: str = ""


# ─────────────────────────────────────────────
# 유니버스 구성: KOSDAQ 중소형주
# ─────────────────────────────────────────────
def _filter_by_volume(tickers: list[str], base_date: str) -> list[str]:
    """pykrx 일봉으로 거래량=0 종목 제거 (거래정지·관리종목 등)"""
    # 가장 최근 거래일 데이터를 한 번에 조회
    ohlcv_df = None
    for days_back in range(0, 6):
        date = (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=days_back)).strftime("%Y%m%d")
        try:
            df = stock.get_market_ohlcv_by_ticker(date, market="KOSDAQ")
            if df is not None and not df.empty and "거래량" in df.columns:
                ohlcv_df = df
                break
        except Exception:
            continue

    if ohlcv_df is None:
        return tickers  # 조회 실패 시 필터링 없이 반환

    liquid = set(ohlcv_df[ohlcv_df["거래량"] > 0].index.tolist())
    before = len(tickers)
    result = [t for t in tickers if t in liquid]
    removed = before - len(result)
    if removed > 0:
        print(f"    거래량=0 종목 제외: {removed}개 (거래정지·관리종목 등)")
    return result


def get_kosdaq_small_mid_cap(base_date: str) -> list[str]:
    print(f"[*] KOSDAQ 종목 시가총액 조회 중... ({base_date})")

    if _HAS_FDR:
        try:
            listing = fdr.StockListing("KOSDAQ")
            if not listing.empty and "Marcap" in listing.columns:
                listing["시가총액_억"] = listing["Marcap"] / 1e8
                filtered = listing[
                    (listing["시가총액_억"] >= MKTCAP_MIN) &
                    (listing["시가총액_억"] <= MKTCAP_MAX)
                ]
                tickers = filtered["Code"].tolist()
                print(f"    대상 종목 수: {len(tickers)}개 (시총 {MKTCAP_MIN}~{MKTCAP_MAX}억) [FDR]")
                tickers = _filter_by_volume(tickers, base_date)
                print(f"    최종 종목 수: {len(tickers)}개 (거래량 필터 적용)")
                return tickers
        except Exception as e:
            print(f"    FDR 조회 실패: {e}")

    cap_df = None
    for days_back in range(0, 6):
        date = (datetime.strptime(base_date, "%Y%m%d") - timedelta(days=days_back)).strftime("%Y%m%d")
        try:
            df = stock.get_market_cap_by_ticker(date, market="KOSDAQ")
            if df is not None and not df.empty and "시가총액" in df.columns:
                if days_back > 0:
                    print(f"    → 휴장일 감지, {date} 기준 대체 조회")
                cap_df = df
                break
        except Exception:
            continue

    if cap_df is None:
        print("    시가총액 조회 실패")
        return []

    cap_df["시가총액_억"] = cap_df["시가총액"] / 1e8
    filtered = cap_df[
        (cap_df["시가총액_억"] >= MKTCAP_MIN) &
        (cap_df["시가총액_억"] <= MKTCAP_MAX)
    ]
    tickers = filtered.index.tolist()
    print(f"    대상 종목 수: {len(tickers)}개 (시총 {MKTCAP_MIN}~{MKTCAP_MAX}억) [pykrx]")
    tickers = _filter_by_volume(tickers, base_date)
    print(f"    최종 종목 수: {len(tickers)}개 (거래량 필터 적용)")
    return tickers


# ─────────────────────────────────────────────
# 거래일 목록 조회
# ─────────────────────────────────────────────
def get_trading_dates(start: str, end: str) -> list[str]:
    """삼성전자 일봉 기준으로 거래일 목록 반환 (YYYYMMDD 문자열)"""
    try:
        df = stock.get_market_ohlcv_by_date(start, end, "005930")
    except Exception:
        return []
    if df is None or df.empty:
        return []
    return [d.strftime("%Y%m%d") for d in df.index]


# ─────────────────────────────────────────────
# 분봉 데이터 수집 (캐시 우선)
# ─────────────────────────────────────────────
def _cache_path(ticker: str, date: str) -> str:
    ext = "parquet" if _HAS_PARQUET else "csv"
    return os.path.join(CACHE_DIR, f"{ticker}_{date}.{ext}")


def _read_cache(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(path, index_col=0, parse_dates=True)


def _write_cache(df: pd.DataFrame, path: str) -> None:
    if path.endswith(".parquet"):
        df.to_parquet(path)
    else:
        df.to_csv(path)


def fetch_minute_data(ticker: str, date: str) -> pd.DataFrame:
    path = _cache_path(ticker, date)

    if os.path.exists(path):
        try:
            return _read_cache(path)
        except Exception:
            os.remove(path)

    candles = get_minute_candles_by_date(_KIS_TOKEN, ticker, date, delay=API_DELAY)
    if not candles:
        return pd.DataFrame()

    df = pd.DataFrame(candles)
    df["datetime"] = pd.to_datetime(df["time"], format="%Y%m%d%H%M%S")
    df = df.set_index("datetime").drop(columns=["time"])
    df = df.rename(columns={
        "open":   "시가",
        "high":   "고가",
        "low":    "저가",
        "close":  "종가",
        "volume": "거래량",
    })

    if df.empty:
        return pd.DataFrame()

    try:
        _write_cache(df, path)
    except Exception:
        pass

    return df


# ─────────────────────────────────────────────
# 지표 계산 (하루치 분봉)
# ─────────────────────────────────────────────
def calc_indicators_day(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 인트라데이 누적 VWAP (매일 리셋)
    tp = (df["고가"] + df["저가"] + df["종가"]) / 3
    df["vwap"] = (tp * df["거래량"]).cumsum() / df["거래량"].cumsum().replace(0, np.nan)

    # Bollinger Bands
    df["bb_mid"]   = df["종가"].rolling(BB_PERIOD).mean()
    bb_std         = df["종가"].rolling(BB_PERIOD).std()
    df["bb_upper"] = df["bb_mid"] + BB_STD * bb_std
    df["bb_lower"] = df["bb_mid"] - BB_STD * bb_std

    # 거래량 급증
    df["vol_ma"]    = df["거래량"].rolling(VOL_MA).mean()
    df["vol_spike"] = df["거래량"] >= df["vol_ma"] * VOL_SPIKE

    return df


# ─────────────────────────────────────────────
# 시그널 생성
# ─────────────────────────────────────────────
def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["entry"] = (
        (df["종가"] >= df["vwap"]) &
        (df["저가"] <= df["bb_lower"]) &
        (df["vol_spike"])
    )
    return df


# ─────────────────────────────────────────────
# 종목별 분봉 백테스트 (인트라데이, 오버나이트 없음)
# ─────────────────────────────────────────────
def backtest_ticker(ticker: str, trading_dates: list[str]) -> list[dict]:
    trades = []

    for date in trading_dates:
        df = fetch_minute_data(ticker, date)
        if df.empty:
            continue

        df = df.between_time(MARKET_OPEN, MARKET_CLOSE)
        if len(df) < BB_PERIOD + 5:
            continue

        df = calc_indicators_day(df)
        df = generate_signals(df)
        df = df.dropna()
        if df.empty:
            continue

        in_position = False
        entry_price = None
        entry_time  = None

        for j, (dt, row) in enumerate(df.iterrows()):
            is_last = (j == len(df) - 1)

            if in_position:
                hold_min = int((dt - entry_time).total_seconds() / 60)
                ret = (row["종가"] - entry_price) / entry_price

                exit_reason = None
                exit_price  = row["종가"]

                if ret <= STOP_LOSS:
                    exit_reason = "손절"
                    exit_price  = entry_price * (1 + STOP_LOSS)
                elif row["종가"] >= row["bb_upper"]:
                    exit_reason = "BB상단"
                elif hold_min >= MAX_HOLD_MINUTES:
                    exit_reason = f"보유{MAX_HOLD_MINUTES}분"
                elif is_last:
                    exit_reason = "장마감"

                if exit_reason:
                    final_ret = (exit_price - entry_price) / entry_price
                    trades.append({
                        "ticker":       ticker,
                        "entry_time":   entry_time.strftime("%Y-%m-%d %H:%M"),
                        "exit_time":    dt.strftime("%Y-%m-%d %H:%M"),
                        "hold_minutes": hold_min,
                        "entry_price":  round(entry_price, 2),
                        "exit_price":   round(exit_price, 2),
                        "return_pct":   round(final_ret * 100, 2),
                        "exit_reason":  exit_reason,
                    })
                    in_position = False
                    continue  # 동일 바에서 재진입 방지

            if not in_position and row["entry"] and not is_last:
                in_position = True
                entry_price = row["종가"]
                entry_time  = dt

    return trades


# ─────────────────────────────────────────────
# 손절 건 진입 시간대 분포
# ─────────────────────────────────────────────
def print_stoploss_time_dist(sl_df: pd.DataFrame, sep: str) -> None:
    sl_df = sl_df.copy()
    sl_df["entry_dt"]  = pd.to_datetime(sl_df["entry_time"])
    sl_df["entry_min"] = sl_df["entry_dt"].dt.hour * 60 + sl_df["entry_dt"].dt.minute

    def to_bucket(m: int) -> str:
        h    = m // 60
        half = "00" if (m % 60) < 30 else "30"
        return f"{h:02d}:{half}"

    sl_df["bucket"] = sl_df["entry_min"].apply(to_bucket)

    # 09:05 ~ 15:00 범위 버킷
    buckets = []
    for h in range(9, 16):
        for half in ("00", "30"):
            b = f"{h:02d}:{half}"
            if "09:00" <= b <= "15:00":
                buckets.append(b)

    counts  = sl_df["bucket"].value_counts().reindex(buckets, fill_value=0)
    max_cnt = counts.max() if counts.max() > 0 else 1

    print(f"\n  [손절 건 진입 시간대 분포]  (총 {len(sl_df)}건)")
    print(f"  {'시간대':<9} {'건수':>5}  분포")
    print(f"  {'─'*9} {'─'*5}  {'─'*32}")
    for bucket, cnt in counts.items():
        bar = "█" * int(cnt / max_cnt * 32)
        print(f"  {bucket:<9} {cnt:>5}건  {bar}")

    # 요일별
    sl_df["요일"] = sl_df["entry_dt"].dt.day_name().map({
        "Monday": "월", "Tuesday": "화", "Wednesday": "수",
        "Thursday": "목", "Friday": "금",
    })
    dow_order  = ["월", "화", "수", "목", "금"]
    dow_counts = sl_df["요일"].value_counts().reindex(dow_order, fill_value=0)
    max_dow    = dow_counts.max() if dow_counts.max() > 0 else 1

    print(f"\n  [손절 건 진입 요일 분포]")
    print(f"  {'요일':<5} {'건수':>5}  분포")
    print(f"  {'─'*5} {'─'*5}  {'─'*32}")
    for day, cnt in dow_counts.items():
        bar = "█" * int(cnt / max_dow * 32)
        print(f"  {day}요일  {cnt:>5}건  {bar}")

    print(sep)


# ─────────────────────────────────────────────
# 결과 출력
# ─────────────────────────────────────────────
def print_results(trades: list[dict]) -> None:
    if not trades:
        print("\n[결과] 조건을 만족하는 거래 없음")
        return

    df = pd.DataFrame(trades)

    total    = len(df)
    wins     = (df["return_pct"] > 0).sum()
    losses   = (df["return_pct"] <= 0).sum()
    win_rate = wins / total * 100

    avg_ret  = df["return_pct"].mean()
    avg_win  = df[df["return_pct"] > 0]["return_pct"].mean() if wins > 0 else 0
    avg_loss = df[df["return_pct"] <= 0]["return_pct"].mean() if losses > 0 else 0
    max_win  = df["return_pct"].max()
    max_loss = df["return_pct"].min()
    avg_hold = df["hold_minutes"].mean()

    gross_profit = df[df["return_pct"] > 0]["return_pct"].sum()
    gross_loss   = abs(df[df["return_pct"] <= 0]["return_pct"].sum())
    pf = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    sep = "=" * 60

    print(f"\n{sep}")
    print(f"  VWAP + BB + 거래량급증 전략  분봉 백테스트 결과")
    print(f"  기간: {START_DATE} ~ {END_DATE}")
    print(f"  장 시간: {MARKET_OPEN} ~ {MARKET_CLOSE}")
    print(f"  시장: KOSDAQ 중소형주 (시총 {MKTCAP_MIN}~{MKTCAP_MAX}억)")
    print(sep)
    print(f"  총 거래 수      : {total:>6}건")
    print(f"  승리 / 패배     : {wins:>4}건 / {losses:>4}건")
    print(f"  승률            : {win_rate:>6.1f}%")
    print(f"  평균 수익률     : {avg_ret:>+6.2f}%")
    print(f"  평균 수익 (승)  : {avg_win:>+6.2f}%")
    print(f"  평균 손실 (패)  : {avg_loss:>+6.2f}%")
    print(f"  최대 수익       : {max_win:>+6.2f}%")
    print(f"  최대 손실       : {max_loss:>+6.2f}%")
    print(f"  Profit Factor   : {pf:>6.2f}")
    print(f"  평균 보유 시간  : {avg_hold:>6.1f}분")
    print(sep)

    reason_counts = df["exit_reason"].value_counts()
    print("  [청산 사유]")
    for reason, cnt in reason_counts.items():
        print(f"    {reason:<16}: {cnt}건")
    print(sep)

    # 손절 건 진입 시간대 분포
    sl_df = df[df["exit_reason"] == "손절"]
    if not sl_df.empty:
        print_stoploss_time_dist(sl_df, sep)
    else:
        print("\n  [손절 건 없음]")
        print(sep)

    # 수익률 TOP 10
    top10 = df.nlargest(10, "return_pct")[
        ["ticker", "entry_time", "exit_time", "hold_minutes",
         "entry_price", "exit_price", "return_pct", "exit_reason"]
    ]
    print("\n  [수익률 TOP 10]")
    print(top10.to_string(index=False))

    # 손실 BOTTOM 5
    bot5 = df.nsmallest(5, "return_pct")[
        ["ticker", "entry_time", "exit_time", "hold_minutes",
         "entry_price", "exit_price", "return_pct", "exit_reason"]
    ]
    print("\n  [손실 BOTTOM 5]")
    print(bot5.to_string(index=False))

    out_path = "backtest_minute_result.csv"
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n  [*] 전체 결과 저장: {out_path}")


# ─────────────────────────────────────────────
# 메인
# ─────────────────────────────────────────────
def main():
    global _KIS_TOKEN
    _KIS_TOKEN = get_token()
    print("[*] 한투 API 토큰 발급 완료")

    base = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
    tickers = get_kosdaq_small_mid_cap(base)
    if not tickers:
        print("종목 조회 실패. 종료합니다.")
        return

    if MAX_TICKERS is not None:
        tickers = tickers[:MAX_TICKERS]
        print(f"[*] 종목 수 제한: {MAX_TICKERS}개")

    print(f"[*] 거래일 조회 중...")
    trading_dates = get_trading_dates(START_DATE, END_DATE)
    if not trading_dates:
        print("거래일 조회 실패. 종료합니다.")
        return
    print(f"    거래일: {len(trading_dates)}일  ({trading_dates[0]} ~ {trading_dates[-1]})")

    total_calls = len(tickers) * len(trading_dates)
    cached = sum(
        1 for t in tickers for d in trading_dates
        if os.path.exists(_cache_path(t, d))
    )
    print(f"[*] 분봉 데이터: 총 {total_calls}건 필요 / 캐시 {cached}건")
    if total_calls - cached > 0:
        est_sec = (total_calls - cached) * API_DELAY
        print(f"    신규 API 호출 예상 시간: 약 {est_sec/60:.0f}분")

    all_trades: list[dict] = []
    n = len(tickers)

    print(f"[*] 백테스트 시작: {n}개 종목")
    for i, ticker in enumerate(tickers, 1):
        trades = backtest_ticker(ticker, trading_dates)
        all_trades.extend(trades)
        if i % 10 == 0 or i == n:
            print(f"    진행: {i}/{n}  누적 거래: {len(all_trades)}건", end="\r")

    print()
    print_results(all_trades)


if __name__ == "__main__":
    main()
