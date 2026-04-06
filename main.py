import time
from api import get_token, get_kosdaq_market_cap_range, get_open_and_prev_close
from screener import screen_stocks
from trader import start_trading
from log_utils import get_logger

logger = get_logger()


def main():
    logger.info("=" * 60)
    logger.info("자동매매 시작")

    token = get_token()

    # 1. 코스닥 시가총액 500억~5000억 종목 조회
    logger.info("[1] 코스닥 시가총액 500억~5000억 종목 조회 중...")
    codes, names = get_kosdaq_market_cap_range(token, min_cap_bil=500, max_cap_bil=5000)

    if not codes:
        logger.error("종목 조회 실패. 종료합니다.")
        return
    logger.info(f"코스닥 500억~5000억 종목 {len(codes)}개 조회 완료")

    # 2. 스크리닝 (VWAP + BB)
    logger.info("[2] 스크리닝 시작 (VWAP + BB)")
    candidates = screen_stocks(token, codes, names)

    if not candidates:
        logger.warning("조건을 통과한 종목 없음. 종료합니다.")
        return

    logger.info(f"최종 후보 {len(candidates)}개:")
    for c in candidates:
        logger.info(
            f"  {c['code']} {c['name']} | 현재가: {c['price']:,} | "
            f"종합점수: {c['composite_score']} | 비중: {c['weight']*100:.1f}%"
        )

    # 3. 시가/전일종가 조회 (실패 종목은 trading 대상에서 제외)
    logger.info("[3] 시가 및 전일종가 조회 중...")
    valid = []
    for c in candidates:
        open_p, prev_c = get_open_and_prev_close(token, c["code"])
        if not open_p or not prev_c:
            logger.warning(f"  {c['code']} 시가/전일종가 조회 실패 — 제외")
        else:
            logger.info(f"  {c['code']} 시가: {open_p:,} | 전일종가: {prev_c:,}")
            valid.append({**c, "_open_p": open_p, "_prev_c": prev_c})
        time.sleep(0.5)

    if not valid:
        logger.warning("시가 조회 성공 종목 없음. 종료합니다.")
        return

    # 4. 실시간 매매 시작
    logger.info("[4] 실시간 매매 시작...")
    target_codes = [c["code"]    for c in valid]
    open_prices  = [c["_open_p"] for c in valid]
    prev_closes  = [c["_prev_c"] for c in valid]
    weights      = [c["weight"]  for c in valid]
    bb_uppers    = [c["bb_upper"] for c in valid]
    start_trading(target_codes, open_prices, prev_closes, weights, bb_uppers=bb_uppers, token=token)


if __name__ == "__main__":
    main()
