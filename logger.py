import logging
import os
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler


def _read_int_env(name, default):
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(str(raw).strip())
    except (ValueError, TypeError):
        return int(default)


def _resolve_log_file():
    log_dir = os.getenv("LOG_DIR", "logs").strip() or "logs"
    log_file = os.getenv("LOG_FILE", "").strip()
    if not log_file:
        log_file = os.path.join(log_dir, "run.log")
    parent = os.path.dirname(log_file)
    if parent:
        os.makedirs(parent, exist_ok=True)
    return log_file


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        log_file = _resolve_log_file()
        rotate_type = os.getenv("LOG_ROTATE_TYPE", "time").strip().lower()
        
        if rotate_type == "time":
            when = os.getenv("LOG_ROTATE_WHEN", "midnight").strip()
            interval = _read_int_env("LOG_ROTATE_INTERVAL", 1)
            backup_count = _read_int_env("LOG_ROTATE_BACKUP_COUNT", 7)
            handler = TimedRotatingFileHandler(
                log_file, when=when, interval=interval, backupCount=backup_count
            )
        else:
            max_bytes = _read_int_env("LOG_MAX_BYTES", 5 * 1024 * 1024)
            backup_count = _read_int_env("LOG_BACKUP_COUNT", 3)
            handler = RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count
            )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    return logger
