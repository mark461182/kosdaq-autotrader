import time
from api import get_stock_price, get_prev_vwap_daily, get_bollinger_band, get_today_ohlc
from log_utils import get_logger

logger = get_logger()


def calc_composite_score(current_price, prev_vwap, bb):
    """
    종합점수 계산 (0~1)
    - VWAP 괴리율 50%: VWAP 대비 아래일수록 높은 점수 (최대 5% 기준 정규화)
    - BB 위치     50%: 밴드 하단에 가까울수록 높은 점수
    """
    vwap_deviation = (prev_vwap - current_price) / prev_vwap * 100
    vwap_score = min(max(vwap_deviation / 5.0, 0.0), 1.0)

    band_width = bb["upper"] - bb["lower"]
    if band_width > 0:
        bb_position = (current_price - bb["lower"]) / band_width
    else:
        bb_position = 0.5
    bb_score = 1.0 - min(max(bb_position, 0.0), 1.0)

    composite = vwap_score * 0.5 + bb_score * 0.5
    return round(composite, 4), round(vwap_deviation, 2), round(bb_position, 4)


def screen_stocks(token, stock_codes, stock_names):
    candidates = []
    total = len(stock_codes)
    reject_vwap = reject_bb = reject_data = 0

    logger.info(f"=== 스크리닝 시작: 총 {total}개 종목 ===")

    for idx, (code, name) in enumerate(zip(stock_codes, stock_names), 1):
        try:
            logger.info(f"[{idx}/{total}] {code} ({name}) 스크리닝 시작")

            price_data = get_stock_price(token, code)
            if not price_data:
                logger.warning(f"  {code} ({name}) SKIP - 현재가 조회 실패")
                reject_data += 1
                continue
            time.sleep(1.0)

            prev_vwap = get_prev_vwap_daily(token, code)
            if not prev_vwap:
                logger.warning(f"  {code} ({name}) SKIP - 전일 VWAP 조회 실패")
                reject_data += 1
                continue
            time.sleep(1.0)

            bb = get_bollinger_band(token, code)
            if not bb:
                logger.warning(f"  {code} ({name}) SKIP - 볼린저밴드 조회 실패")
                reject_data += 1
                continue
            time.sleep(1.0)

            current_price = price_data["price"]
            vwap_diff = round((current_price - prev_vwap) / prev_vwap * 100, 2)
            bb_diff   = round((current_price - bb["middle"]) / bb["middle"] * 100, 2)

            vwap_condition = current_price < prev_vwap
            bb_condition   = current_price < bb["middle"]

            logger.info(
                f"  현재가: {current_price:,} | 전일VWAP: {prev_vwap:,} ({vwap_diff:+.2f}%) | "
                f"BB중간: {bb['middle']:,} ({bb_diff:+.2f}%) | BB하단: {bb['lower']:,} | BB상단: {bb['upper']:,}"
            )
            logger.info(
                f"  VWAP 조건: {'통과' if vwap_condition else f'탈락 (현재가가 VWAP보다 {abs(vwap_diff):.2f}% 높음)'} | "
                f"BB 조건: {'통과' if bb_condition else f'탈락 (현재가가 BB중간선보다 {abs(bb_diff):.2f}% 높음)'}"
            )

            if not vwap_condition:
                logger.info(f"  REJECT [{code} {name}] 사유: VWAP 조건 미달 (현재가 VWAP 대비 {vwap_diff:+.2f}%)")
                reject_vwap += 1
                continue
            if not bb_condition:
                logger.info(f"  REJECT [{code} {name}] 사유: BB 조건 미달 (현재가 BB중간선 대비 {bb_diff:+.2f}%)")
                reject_bb += 1
                continue

            composite_score, vwap_deviation, bb_pos = calc_composite_score(
                current_price, prev_vwap, bb
            )
            logger.info(
                f"  종합점수: {composite_score} | VWAP괴리율: {vwap_deviation:.2f}% | BB위치: {bb_pos:.4f}"
            )

            ohlc = get_today_ohlc(token, code)
            time.sleep(1.0)
            if ohlc:
                logger.info(
                    f"  오늘 시가: {ohlc['open']:,} | 고가: {ohlc['high']:,} | "
                    f"저가: {ohlc['low']:,} | 종가: {ohlc['close']:,} | 등락률: {price_data['change_rate']:+.2f}%"
                )

            candidates.append({
                "code": code,
                "name": name,
                "price": current_price,
                "prev_vwap": prev_vwap,
                "bb_lower": bb["lower"],
                "bb_middle": bb["middle"],
                "bb_upper": bb["upper"],
                "change_rate": price_data["change_rate"],
                "open":  ohlc["open"]  if ohlc else None,
                "high":  ohlc["high"]  if ohlc else None,
                "low":   ohlc["low"]   if ohlc else None,
                "close": ohlc["close"] if ohlc else None,
                "vwap_deviation": vwap_deviation,
                "bb_position": bb_pos,
                "composite_score": composite_score,
                "weight": 0.0
            })
            logger.info(f"  PASS [{code} {name}] 최종 통과")

        except Exception as e:
            logger.exception(f"  {code} ({name}) 처리 중 오류: {e}")
            continue

    # 종합점수 비율로 자금 배분 비중 계산
    if candidates:
        total_score = sum(c["composite_score"] for c in candidates)
        for c in candidates:
            c["weight"] = round(c["composite_score"] / total_score, 4)

    # 스크리닝 결과 요약 로그
    logger.info("=" * 60)
    logger.info(f"스크리닝 완료: 전체 {total}개 → 통과 {len(candidates)}개")
    logger.info(
        f"탈락 내역: 데이터조회실패 {reject_data} | "
        f"VWAP조건 {reject_vwap} | BB조건 {reject_bb}"
    )
    for c in candidates:
        ohlc_str = ""
        if c["open"] is not None:
            ohlc_str = (
                f" | 시가: {c['open']:,} 고가: {c['high']:,} "
                f"저가: {c['low']:,} 종가: {c['close']:,} 등락률: {c['change_rate']:+.2f}%"
            )
        logger.info(
            f"  FINAL {c['code']} {c['name']} | 종합점수: {c['composite_score']} | "
            f"비중: {c['weight']*100:.1f}%{ohlc_str}"
        )
    logger.info("=" * 60)

    return candidates


if __name__ == "__main__":
    from api import get_token, get_kosdaq_market_cap_range

    token = get_token()

    logger.info("코스닥 시가총액 500억~5000억 종목 조회 중...")
    codes, names = get_kosdaq_market_cap_range(token, min_cap_bil=500, max_cap_bil=5000)

    if not codes:
        logger.error("종목 조회 실패. 종료합니다.")
    else:
        logger.info(f"{len(codes)}개 종목 스크리닝 시작")
        screen_stocks(token, codes, names)
