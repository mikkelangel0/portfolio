import config as cfg

class Interval:
    def __init__(self):
        self.maker = 0
        self.taker = 0
        self.liquidations = {'sell_vol': 0, 'buy_vol': 0}
        self.open_interest = {'open': 0, 'high': 0, 'low': 0, 'close': 0}

        # Dispatch dictionary mapping event types to handler methods
        self.event_dispatch = {
            'aggTrade': self.add_agg_trade,
            'forceOrder': self.add_force_order,
            'openInterest': self.add_open_interest
        }

    def add_data(self, data):
        """
        Adds the given data to the data interval.

        :param data: Dictionary containing various data
        """
        event_type = data['e']
        handler = self.event_dispatch.get(event_type)

        # Handle the event
        if handler:
            try:
                handler(data)
            except Exception as e:
                print("Add to datainterval exception ", e)
        else:
            print(f"Unhandled event type: {event_type}")

    def add_agg_trade(self, agg_trade_data):
        """
        Update the volume for maker or taker based on the trade data.

        :param data: Dictionary containing the aggregated trade data
        """
        if agg_trade_data['is_buyer_maker']:
            self.maker += agg_trade_data['quantity']
        else:
            self.taker += agg_trade_data['quantity']
    
    def add_force_order(self, liq_order):
        """
        Update the volume for liquidation orders

        :param data: Dictionary containing a new liquidation order 
        """
        if liq_order['S'] == "SELL":
            self.liquidations['sell_vol'] += liq_order['q']
        else:
            self.liquidations['buy_vol'] += liq_order['q']

    def add_open_interest(self, open_interest_data):
        """
        Update the open interest data with new incoming data.

        This function updates the open, high, low, and close values of open interest.
        'open' is set only once, the first time data is received.
        'high' and 'low' are updated based on the new data.
        'close' is always set to the value of the latest data.

        :param open_interest_data: Dictionary containing the new open interest data.
        """
        current_oi = open_interest_data['openInterest']

        # Initialize 'open' on the first update
        if self.open_interest['open'] == 0:
            self.open_interest['open'] = current_oi

        # Update 'high' and 'low' using max and min functions
        self.open_interest['high'] = max(self.open_interest['high'], current_oi)
        self.open_interest['low'] = min(self.open_interest['low'] if self.open_interest['low'] != 0 else current_oi, current_oi)

        # Always update 'close' with the current value
        self.open_interest['close'] = current_oi

    def get_all(self, array=False):
        if array:
            return [self.maker, self.taker, self.liquidations, self.open_interest]
        else:
            return {'maker': self.maker, 
                    'taker': self.taker,
                    'liquidations': self.liquidations,
                    'open_interest': self.open_interest
                    }