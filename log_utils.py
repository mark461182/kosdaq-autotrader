import logging
import os
from datetime import datetime


def get_logger(name="trading"):
    """날짜별 로그 파일 + 콘솔 동시 출력 로거 반환 (중복 핸들러 방지)"""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    os.makedirs("logs", exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    log_path = os.path.join("logs", f"{today}.log")

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger
