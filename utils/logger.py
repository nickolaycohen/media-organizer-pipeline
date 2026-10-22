import hashlib
import os
import logging
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

def setup_logger(log_file, module_tag, extra_fields=None, include_console=True, rotation="size", max_bytes=10*1024*1024, backup_count=10):
    # Ensure the log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(module_tag)
    logger.setLevel(logging.INFO)

    # Determine dynamic format string based on extra_fields
    parts = []
    if extra_fields:
        if "triage_id" in extra_fields:
            parts.append("[Triage ID: %(triage_id)s]")
        if "session_id" in extra_fields:
            parts.append("[Session: %(session_id)s]")
        for k in extra_fields:
            if k not in ("triage_id", "session_id"):
                parts.append(f"[{k.capitalize()}: %({k})s]")

    extra_str = f" {' '.join(parts)}" if parts else ""
    formatter = logging.Formatter(f'%(asctime)s [{module_tag}]{extra_str} - %(message)s')

    if not logger.handlers:
        if include_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        if rotation == "time":
            file_handler = TimedRotatingFileHandler(
                log_file,
                when='midnight',
                interval=1,
                backupCount=backup_count,
                encoding='utf-8'
            )
        else:
            # RotatingFileHandler: appends to existing file ('mode=a') and rotates after max_bytes, keeping last 10 files
            file_handler = RotatingFileHandler(
                log_file,
                mode='a',
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    else:
        # Update formatters on existing handlers to reflect extra_fields
        for h in logger.handlers:
            h.setFormatter(formatter)

    # Attach extra fields to the logger
    if extra_fields:
        class ContextFilter(logging.Filter):
            def filter(self, record):
                for k, v in extra_fields.items():
                    setattr(record, k, v)
                return True
        logger.addFilter(ContextFilter())

    return logger

def close_logger(logger):
    """Safely close all handlers attached to the logger."""
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)
        
def compute_file_hash(filepath):
    hasher = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hasher.update(chunk)
    return hasher.hexdigest()