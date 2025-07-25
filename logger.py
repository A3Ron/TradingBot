
from datetime import datetime
from dotenv import load_dotenv
from data import DataFetcher
load_dotenv()


class Logger:
    def __init__(self, config=None):
        self.config = config or {}

    def log_to_db(self, level, source, message):
        DataFetcher(self.config or {}).save_log_to_db(level, source, message)
