import config as cfg
from data.data_interval import Interval

class DataBlock:
    def __init__(self):
        self.interval_size = cfg.DATABLOCK_CONFIG.get('interval_size')
        self.max_intervals = cfg.DATABLOCK_CONFIG.get('max_intervals')
        self.binance_api_requests = cfg.BINANCE_REST_CONFIG.get('requests')
        self.data = {}
        self.last_interval_key = None

    def add_data(self, data):
        """
        Add data to the correct interval based on its price and event type.
        If the price is not available, use the last used interval.

        :param data: The data to be added.
        """
        
        price = float(data['price']) if 'price' in data.keys() else None

        if price is not None:
            interval_key = self.get_interval_key(price)
        else:
            interval_key = self.last_interval_key

        if interval_key is None:
            # TODO - handle data that is received before we get a price
            return

        # Ensure the interval exists
        if interval_key not in self.data.keys():
            self.data[interval_key] = Interval()

        # Update the last used interval key
        self.last_interval_key = interval_key

        # Add data to the appropriate interval
        interval = self.data[interval_key]
        interval.add_data(data)

    def get_interval_key(self, price):
        """
        Determine the interval key for a given price.

        :param price: The price to categorize.
        :return: The interval key.
        """
        interval_start = (price // self.interval_size) * self.interval_size
        return interval_start

    def in_block(self, price):
        """
        Check if the given price is within the range of any existing intervals in the data block.

        :param price: The price to check.
        :return: True if the price is within an existing interval, False otherwise.
        """
        if price is None:
            # If price is None, check if there is a last interval key to use
            return self.last_interval_key in self.data if self.last_interval_key is not None else False

        interval_key = self.get_interval_key(price)
        if interval_key in self.data:
            return True
        elif len(self.data) < self.max_intervals:
            return True
        else:
            return False
        
    def add_interval(self, price):
        """
        Add a new interval to the data block if the maximum number of intervals is not exceeded.
        """
        # Check if the current number of intervals is less than the maximum allowed
        if self.get_intervals() < self.max_intervals:
            # Create a new interval
            new_interval_key = self.get_interval_key(price)
            self.data[new_interval_key] = Interval()
        else:
            raise ValueError("Max intervals exceeded!")

    def get_block(self):
        block = {}
        for key in self.get_intervals():
            block[key] = self.data[key].get_all()
        return block

    def get_intervals(self):
        return self.data.keys()
    
    def get_symbol(self):
        return self.symbol