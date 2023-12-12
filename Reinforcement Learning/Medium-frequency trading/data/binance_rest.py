import threading
import time
import requests
import config as cfg
import itertools

class BinanceREST(threading.Thread):
    def __init__(self):
        super().__init__()
        self.base_url = cfg.BINANCE_REST_CONFIG.get('base_url')
        self.interval = cfg.BINANCE_REST_CONFIG.get('rest_interval')
        self.requests_cycle = itertools.cycle(cfg.BINANCE_REST_CONFIG['requests'])
        self.symbols = cfg.SYMBOLS
        self.stop_flag = threading.Event()
        self.request_handlers = {
            'trades': self.fetch_recent_trades,
            'openInterest': self.fetch_open_interest,
        }
        
    def run(self):
        while not self.stop_flag.is_set():
            for symbol in self.symbols:
                data = self.fetch_all_queue(symbol)
                print(data)

                # Sleep to stay within Rate Limits
                time.sleep(self.interval)

    def fetch_all_queue(self, symbol):
        """
        Fetches different types of data in a cycle as specified in the configuration.
        """
        request_type = next(self.requests_cycle)
        handler = self.request_handlers.get(request_type)

        if handler:
            return handler(symbol)
        else:
            raise ValueError(f"Unknown request type: {request_type}")

    def fetch_recent_trades(self, symbol, limit=5):
        """
        Fetch recent trades from Binance Futures API.

        :param symbol: The symbol to fetch trades for (e.g., 'BTCUSDT').
        :param limit: The number of trades to fetch (max 1000). Default is 500.
        :return: A list of recent trades.
        """
        url = self.base_url + "/fapi/v1/trades"
        params = {
            'symbol': symbol,
            'limit': limit
        }
        response = requests.get(url, params=params)
        response.raise_for_status()  # This will raise an exception for non-200 responses
        trades_data = response.json()
        return {
            'e': 'trades',          # Event type
            'trades': trades_data   # The trades
        }
        
    def fetch_open_interest(self, symbol):
        """
        Fetch open interest from Binance Futures API.

        :param symbol: The symbol to fetch open interest for (e.g., 'BTCUSDT').
        :return: Open interest data.
        """
        url = self.base_url + "/fapi/v1/openInterest"
        params = {'symbol': symbol}
        response = requests.get(url, params=params)
        response.raise_for_status()
        open_interest_data = response.json()
        return {
            'e': 'openInterest',    # Event type
            'oi': open_interest_data  # Open interest data
        }

    def stop(self):
        self.stop_flag.set()

