import threading
import queue
import config as cfg
from data.datablock import DataBlock

# process functions should not return anything, rather add data to the datablock.
# once the price moves past a block we need to initiate a new block. 


class DataProcessor(threading.Thread):
    def __init__(self, data_queue, processed_data_queue):
        super().__init__()
        self.data_queue = data_queue
        self.processed_data_queue = processed_data_queue
        self.stop_flag = threading.Event()
        self.datablocks ={}
        
        # Initialize datablocks for each symbol
        for symbol in cfg.SYMBOLS:
            self.datablocks[symbol.lower()] = DataBlock()

        # Dispatch dictionary mapping event types to handler methods
        self.event_dispatch = {
            'aggTrade': self.process_aggTrade,
            'depthUpdate': self.process_depth_update,
            'kline': self.process_kline,
            'forceOrder': self.process_force_order,
            'openInterest': self.process_open_interest
        }

    def run(self):
        while not self.stop_flag.is_set():
            try:
                # Get data from the queue
                data = self.data_queue.get(timeout=1)
                self.handle_data(data)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error processing data: {e} response: {data}")

    def handle_data(self, data):
        """
        Main method to handle incoming JSON data.

        :param data: JSON data from the WebSocket.
        """
        handler = None
        if 'e' in data:
            event_type = data['e']
            handler = self.event_dispatch.get(event_type)
        
        if handler:
            processed_data = handler(data)
            # Get the symbol of the data and price if it exists
            symbol = processed_data.get('symbol')
            price = data.get('p') if 'p' in data.keys() else None

            if not symbol:
                print("No symbol found in the data. Skipping.")
                return

            # Determine if a new interval should be started
            if price is not None:
                price = float(price)
                if not self.datablocks[symbol].in_block(price):
                    # Save the current DataBlock and start a new one
                    self.save_block(symbol)
                    self.new_block(symbol)
                    
            # Add data to the DataBlock
            self.datablocks[symbol].add_data(processed_data)
        else:
            print(f"Unhandled response: {data}")

    def process_aggTrade(self, data):
        """
        Process aggregated trade data.

        :param data: Trade data JSON.
        """
        processed_trade = {
            'e': 'aggTrade',
            'symbol': str(data['s']).lower(),
            'price': float(data['p']),
            'quantity': float(data['q']),
            'is_buyer_maker': int(data['m'])
        }
        return processed_trade

    def process_depth_update(self, data):
        """
        Process depth update data.

        :param data: Depth update data JSON.
        """
        depth_update = {
            'e': 'depthUpdate',
            'symbol': str(data['s']).lower(),
            'bids': [[float(price), float(quantity)] for price, quantity in data['b']],
            'asks': [[float(price), float(quantity)] for price, quantity in data['a']]
        }
        return depth_update

    def process_kline(self, data):
        """
        Process kline (candlestick) data.

        :param data: Kline data JSON.
        """
        event_time = data['E']
        kline_data = data['k']

        # Calculate the position in the current candle
        time_into_candle = event_time - kline_data['t']
        segment_duration = (kline_data['T'] - kline_data['t']) // 6  # Integer division
        position_in_candle = min(time_into_candle // segment_duration, 5)  # Integer division
        current_time = [0] * 6
        current_time[position_in_candle] = 1

        kline = {
            'symbol': str(kline_data['s']).lower(),
            'open_price': float(kline_data['o']),
            'close_price': float(kline_data['c']),
            'high_price': float(kline_data['h']),
            'low_price': float(kline_data['l']),
            'volume': float(kline_data['v']),
            'number_of_trades': kline_data['n'],
            'quote_asset_volume': float(kline_data['q']),
            'taker_buy_base_asset_volume': float(kline_data['V']),
            'taker_buy_quote_asset_volume': float(kline_data['Q']),
            'current_time': current_time
        }
        return kline

    def process_force_order(self, data):
        """
        Process forced orders (liquidations)

        :param data: forced orders data JSON
        """
        liquidation_orders = {
            'e': 'forceOrder',
            'symbol': str(data['o'].get('s')).lower(),
            'price': float(data['o'].get('p')),
            'S': str(data['o'].get('S')).upper(),
            'q': float(data['o'].get('q')),
        }

        return liquidation_orders

    def process_open_interest(self, data):
        """
        Process Open Interest data

        :param data: OpenInterest data JSON
        """
        open_interest_data = {
            'e': 'openInterest',
            'symbol': str(data['oi'].get('symbol')).lower(),
            'openInterest': float(data['oi'].get('openInterest'))
        }
        return open_interest_data
    
    def save_block(self, symbol):
        """
        Sends the dataBlock to the processed data queue for saving

        :param symbol: the symbol block to save
        """
        self.processed_data_queue.put(self.datablocks[symbol].get_block())

    def new_block(self, symbol):
        self.datablocks[symbol] = DataBlock()

    def stop(self):
        self.stop_flag.set()
    