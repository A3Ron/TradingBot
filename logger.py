import csv
import os
from datetime import datetime

class Logger:
    def __init__(self, log_path):
        self.log_path = log_path
        if not os.path.exists(log_path):
            with open(log_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['timestamp','symbol','entry_price','exit_price','stop_loss','take_profit','volume','outcome'])

    def log_trade(self, symbol, entry, exit, stop_loss, take_profit, volume, outcome):
        with open(self.log_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.now().isoformat(), symbol, entry, exit, stop_loss, take_profit, volume, outcome
            ])
