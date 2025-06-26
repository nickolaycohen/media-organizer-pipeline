import hashlib
import os
import logging

def setup_logger(log_file, module_tag, extra_fields=None):
    # Ensure the log directory exists
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger(module_tag)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        if extra_fields and "session_id" in extra_fields:
            formatter = logging.Formatter(f'%(asctime)s [{module_tag}] [Session: %(session_id)s] - %(message)s')
        else:
            formatter = logging.Formatter(f'%(asctime)s [{module_tag}] - %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

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