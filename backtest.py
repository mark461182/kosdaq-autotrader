"""
KOSDAQ 중소형주 1분봉 백테스트
전략: 실시간 틱 2-of-3 조건의 분봉 근사 재현

실제 trader.py 조건 → 분봉 프록시:
  1. 거래량 급증   → 현재 봉 거래량 >= 직전 30봉 평균 × 2       (직접 계산)
  2. 체결강도 110% → 캔들 매수 우위: (종가-시가)/(고가-저가) >= 0.5  (프록시)
  3. 매도잔량 소진 → 직전 5봉 고가 돌파: 종가 > max(고가[-5:-1])     (프록시)

  3개 중 2개 이상 충족 & 09:30~14:30 내 진입

청산:
  - 손절:          매수가 대비 -2%
  - 트레일링 스탑: 고점 대비 -1.5%
  - 강제 청산:     15:20 (혹은 데이터 마지막 봉)
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

# 매수 조건 파라미터 (trader.py 와 동일)
VOL_WARMUP      = 30    # 거래량 이동평균 기준 봉 수
VOL_SPIKE_MULT  = 2.0   # 거래량 급증 배수
STRENGTH_RATIO  = 0.5   # 캔들 매수 우위 판정 비율 (체결강도 프록시)
BUY_COND_NEEDED = 2     # 3개 조건 중 최소 통과 수

# 청산 파라미터 (trader.py 와 동일)
STOP_LOSS_PCT  = -2.0   # 손절 %
TRAILING_PCT   = -1.5   # 트레일링 스탑 %

# 거래 시간 (trader.py 와 동일)
MARKET_OPEN  = "09:30"  # 데이터 수집 시작 (between_time 용)
MARKET_CLOSE = "15:20"  # 강제 청산 / 데이터 수집 종료
BUY_START    = "09:30"  # 매수 진입 허용 시작
BUY_END      = "14:30"  # 매수 진입 허용 종료

# 시가총액 필터 (억 원)
MKTCAP_MIN = 500
MKTCAP_MAX = 5_000

# 캐시 / 성능
CACHE_DIR   = "cache_minute"
API_DELAY   = 0.15
MAX_TICKERS = 200   # None 이면 전체 유니버스 사용

os.makedirs(CACHE_DIR, exist_ok=True)

_KIS_TOKEN: str = ""


# ─────────────────────────────────────────────
# 유니버스 구성
# ─────────────────────────────────────────────

def _filter_by_volume(tickers: list[str], base_date: str) -> list[str]:
    """pykrx 일봉으로 거래량=0 종목 제거 (거래정지·관리종목 등)"""
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
        return tickers

    liquid = set(ohlcv_df[ohlcv_df["거래량"] > 0].index.tolist())
    before = len(tickers)
    result = [t for t in tickers if t in liquid]
    removed = before - len(result)
    if removed:
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
# 거래일 목록
# ─────────────────────────────────────────────

def get_trading_dates(start: str, end: str) -> list[str]:
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
        "open": "시가", "high": "고가", "low": "저가",
        "close": "종가", "volume": "거래량",
    })

    if df.empty:
        return pd.DataFrame()

    try:
        _write_cache(df, path)
    except Exception:
        pass

    return df


# ─────────────────────────────────────────────
# 지표 계산 (분봉 기반 3개 조건 프록시)
# ─────────────────────────────────────────────

def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """3개 매수 조건의 분봉 프록시 계산.

    cond_vol      : 거래량 급증 (직접) — vol >= 직전 30봉 평균 × 2
    cond_strength : 체결강도 프록시   — 캔들 매수 우위 (종가-시가)/(고가-저가) >= 0.5
    cond_breakout : 매도잔량 소진 프록시 — 직전 5봉 고가 돌파 (저항 돌파)
    """
    df = df.copy()

    # ── 조건 1: 거래량 급증 ──────────────────────
    df["vol_ma"] = df["거래량"].rolling(VOL_WARMUP).mean()
    df["cond_vol"] = df["거래량"] >= df["vol_ma"] * VOL_SPIKE_MULT

    # ── 조건 2: 캔들 매수 우위 (체결강도 프록시) ──
    body  = df["종가"] - df["시가"]
    range_ = (df["고가"] - df["저가"]).replace(0, np.nan)
    df["cond_strength"] = (body / range_) >= STRENGTH_RATIO

    # ── 조건 3: 직전 5봉 고가 돌파 (매도잔량 소진 프록시) ──
    df["prev5_high"] = df["고가"].shift(1).rolling(5).max()
    df["cond_breakout"] = df["종가"] > df["prev5_high"]

    # 충족 조건 수 (편의용)
    df["cond_count"] = (
        df["cond_vol"].astype(int) +
        df["cond_strength"].astype(int) +
        df["cond_breakout"].astype(int)
    )

    return df


# ─────────────────────────────────────────────
# 종목별 백테스트 (인트라데이, 오버나이트 없음)
# ─────────────────────────────────────────────

def backtest_ticker(ticker: str, trading_dates: list[str]) -> list[dict]:
    trades = []

    for date in trading_dates:
        df = fetch_minute_data(ticker, date)
        if df.empty:
            continue

        df = df.between_time(MARKET_OPEN, MARKET_CLOSE)
        if len(df) < VOL_WARMUP + 10:
            continue

        df = calc_indicators(df)
        df = df.dropna(subset=["vol_ma", "prev5_high"])
        if df.empty:
            continue

        in_position = False
        entry_price = 0.0
        entry_time  = None
        high_water  = 0.0
        entry_conds = {}

        rows = list(df.iterrows())
        for j, (dt, row) in enumerate(rows):
            is_last   = (j == len(rows) - 1)
            time_str  = dt.strftime("%H:%M")

            # ── 포지션 보유 중: 청산 조건 체크 ──────
            if in_position:
                price = row["종가"]

                if price > high_water:
                    high_water = price

                change_pct   = (price - entry_price) / entry_price * 100
                trailing_pct = (price - high_water)  / high_water  * 100

                exit_reason = None
                exit_price  = price

                if change_pct <= STOP_LOSS_PCT:
                    exit_reason = "손절"
                    exit_price  = entry_price * (1 + STOP_LOSS_PCT / 100)
                elif trailing_pct <= TRAILING_PCT:
                    exit_reason = "트레일링스탑"
                elif time_str >= MARKET_CLOSE or is_last:
                    exit_reason = "강제청산"

                if exit_reason:
                    hold_min  = int((dt - entry_time).total_seconds() / 60)
                    final_ret = (exit_price - entry_price) / entry_price * 100
                    trades.append({
                        "ticker":       ticker,
                        "date":         date,
                        "entry_time":   entry_time.strftime("%Y-%m-%d %H:%M"),
                        "exit_time":    dt.strftime("%Y-%m-%d %H:%M"),
                        "hold_minutes": hold_min,
                        "entry_price":  round(entry_price, 2),
                        "exit_price":   round(exit_price, 2),
                        "high_water":   round(high_water, 2),
                        "return_pct":   round(final_ret, 2),
                        "exit_reason":  exit_reason,
                        "cond_vol":     entry_conds.get("vol", False),
                        "cond_strength":entry_conds.get("strength", False),
                        "cond_breakout":entry_conds.get("breakout", False),
                        "cond_count":   entry_conds.get("count", 0),
                    })
                    in_position = False
                    continue  # 동일 봉에서 재진입 방지

            # ── 미보유 중: 매수 조건 체크 (09:30~14:30) ──
            if not in_position and BUY_START <= time_str < BUY_END and not is_last:
                if row["cond_count"] >= BUY_COND_NEEDED:
                    in_position  = True
                    entry_price  = row["종가"]
                    entry_time   = dt
                    high_water   = row["종가"]
                    entry_conds  = {
                        "vol":      bool(row["cond_vol"]),
                        "strength": bool(row["cond_strength"]),
                        "breakout": bool(row["cond_breakout"]),
                        "count":    int(row["cond_count"]),
                    }

    return trades


# ─────────────────────────────────────────────
# 결과 출력 및 저장
# ─────────────────────────────────────────────

def _bar(val: float, max_val: float, width: int = 32) -> str:
    return "█" * int(val / max_val * width) if max_val > 0 else ""


def print_results(trades: list[dict]) -> None:
    sep = "=" * 65

    if not trades:
        print(f"\n{sep}")
        print("  [결과] 조건을 만족하는 거래 없음")
        print(sep)
        return

    df = pd.DataFrame(trades)

    total  = len(df)
    wins   = (df["return_pct"] > 0).sum()
    losses = (df["return_pct"] <= 0).sum()
    wr     = wins / total * 100

    avg_ret  = df["return_pct"].mean()
    avg_win  = df[df["return_pct"] > 0]["return_pct"].mean() if wins  else 0.0
    avg_loss = df[df["return_pct"] <= 0]["return_pct"].mean() if losses else 0.0
    max_win  = df["return_pct"].max()
    max_loss = df["return_pct"].min()
    avg_hold = df["hold_minutes"].mean()

    gross_p = df[df["return_pct"] > 0]["return_pct"].sum()
    gross_l = abs(df[df["return_pct"] <= 0]["return_pct"].sum())
    pf = gross_p / gross_l if gross_l > 0 else float("inf")

    # 단순 자본 곡선 (거래 순서대로 수익률 누적, 슬리피지 0 가정)
    cumret = (1 + df["return_pct"] / 100).cumprod()
    total_ret = (cumret.iloc[-1] - 1) * 100
    dd_series = cumret / cumret.cummax() - 1
    max_dd = dd_series.min() * 100

    print(f"\n{sep}")
    print(f"  실시간 틱 2-of-3 전략  분봉 백테스트 결과")
    print(f"  기간: {START_DATE} ~ {END_DATE}")
    print(f"  매수 시간: {BUY_START} ~ {BUY_END}  |  강제청산: {MARKET_CLOSE}")
    print(f"  시장: KOSDAQ 중소형주 (시총 {MKTCAP_MIN}~{MKTCAP_MAX}억)")
    print(f"  매수 조건: 3개 중 {BUY_COND_NEEDED}개 이상  |  손절 {STOP_LOSS_PCT}%  |  트레일링 {TRAILING_PCT}%")
    print(sep)
    print(f"  총 거래 수        : {total:>7,}건")
    print(f"  승리 / 패배       : {wins:>5,}건 / {losses:>5,}건")
    print(f"  승률              : {wr:>7.1f}%")
    print(f"  평균 수익률       : {avg_ret:>+7.2f}%")
    print(f"  평균 수익 (승)    : {avg_win:>+7.2f}%")
    print(f"  평균 손실 (패)    : {avg_loss:>+7.2f}%")
    print(f"  최대 수익         : {max_win:>+7.2f}%")
    print(f"  최대 손실         : {max_loss:>+7.2f}%")
    print(f"  Profit Factor     : {pf:>7.2f}")
    print(f"  평균 보유 시간    : {avg_hold:>7.1f}분")
    print(f"  누적 수익률(단순) : {total_ret:>+7.2f}%")
    print(f"  최대 낙폭 (MDD)   : {max_dd:>+7.2f}%")
    print(sep)

    # 청산 사유별 통계
    reason_grp = df.groupby("exit_reason")["return_pct"]
    print("  [청산 사유별 통계]")
    print(f"  {'사유':<14} {'건수':>5}  {'평균수익률':>10}  {'승률':>6}")
    print(f"  {'─'*14} {'─'*5}  {'─'*10}  {'─'*6}")
    for reason, grp in reason_grp:
        cnt = len(grp)
        avg = grp.mean()
        wr_ = (grp > 0).sum() / cnt * 100
        print(f"  {reason:<14} {cnt:>5,}건  {avg:>+9.2f}%  {wr_:>5.1f}%")
    print(sep)

    # 조건 조합별 거래 분포
    combos = df.groupby(["cond_vol", "cond_strength", "cond_breakout"])["return_pct"]
    print("  [매수 조건 조합별 통계]  (V=거래량, S=체결강도, B=저항돌파)")
    print(f"  {'V S B':<7} {'건수':>5}  {'평균수익률':>10}  {'승률':>6}")
    print(f"  {'─'*7} {'─'*5}  {'─'*10}  {'─'*6}")
    for (cv, cs, cb), grp in combos:
        label = f"{'O' if cv else 'X'} {'O' if cs else 'X'} {'O' if cb else 'X'}"
        cnt  = len(grp)
        avg  = grp.mean()
        wr_  = (grp > 0).sum() / cnt * 100
        print(f"  {label:<7} {cnt:>5,}건  {avg:>+9.2f}%  {wr_:>5.1f}%")
    print(sep)

    # 시간대별 진입 분포
    df["entry_hm"] = pd.to_datetime(df["entry_time"]).dt.strftime("%H:%M")
    buckets = []
    for h in range(9, 15):
        for m in (0, 30):
            b = f"{h:02d}:{m:02d}"
            if BUY_START <= b < BUY_END:
                buckets.append(b)

    def _bucket(hm: str) -> str:
        h, m = int(hm[:2]), int(hm[3:])
        return f"{h:02d}:{'00' if m < 30 else '30'}"

    df["bucket"] = df["entry_hm"].apply(_bucket)
    time_counts = df["bucket"].value_counts().reindex(buckets, fill_value=0)
    max_tc = time_counts.max() if time_counts.max() > 0 else 1

    print("  [진입 시간대 분포]")
    print(f"  {'시간대':<7} {'건수':>5}  분포")
    print(f"  {'─'*7} {'─'*5}  {'─'*32}")
    for b, cnt in time_counts.items():
        print(f"  {b:<7} {cnt:>5,}건  {_bar(cnt, max_tc)}")
    print(sep)

    # 수익률 TOP 10 / BOTTOM 5
    cols = ["ticker", "entry_time", "exit_time", "hold_minutes",
            "entry_price", "exit_price", "return_pct", "exit_reason",
            "cond_vol", "cond_strength", "cond_breakout"]

    top10 = df.nlargest(10, "return_pct")[cols]
    print("\n  [수익률 TOP 10]")
    print(top10.to_string(index=False))

    bot5 = df.nsmallest(5, "return_pct")[cols]
    print("\n  [손실 BOTTOM 5]")
    print(bot5.to_string(index=False))
    print()

    # ── CSV 저장 ──────────────────────────────
    save_cols = [
        "ticker", "date", "entry_time", "exit_time", "hold_minutes",
        "entry_price", "exit_price", "high_water", "return_pct", "exit_reason",
        "cond_vol", "cond_strength", "cond_breakout", "cond_count",
    ]
    out_path = "backtest_result.csv"
    df[save_cols].to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"  [*] 전체 결과 저장: {out_path}  ({total}건)")

    # ── 요약 JSON 저장 ────────────────────────
    import json
    summary = {
        "strategy":     "tick_2of3_proxy",
        "start_date":   START_DATE,
        "end_date":     END_DATE,
        "buy_window":   f"{BUY_START}~{BUY_END}",
        "stop_loss":    STOP_LOSS_PCT,
        "trailing":     TRAILING_PCT,
        "total_trades": int(total),
        "win_rate":     round(wr, 2),
        "avg_return":   round(avg_ret, 2),
        "profit_factor":round(pf, 4),
        "cumulative_return": round(total_ret, 2),
        "max_drawdown": round(max_dd, 2),
        "avg_hold_min": round(avg_hold, 1),
    }
    with open("backtest_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"  [*] 요약 저장: backtest_summary.json")
    print(sep)


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

    print("[*] 거래일 조회 중...")
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
    print(f"[*] 분봉 데이터: 총 {total_calls:,}건 필요 / 캐시 {cached:,}건")
    uncached = total_calls - cached
    if uncached > 0:
        print(f"    신규 API 호출 예상 시간: 약 {uncached * API_DELAY / 60:.0f}분")

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
