import json
import websocket
import threading
import config as cfg

class BinanceWebSocket(threading.Thread):
    def __init__(self, data_queue):
        """
        Initialize the BinanceWebSocket class.

        This class is responsible for connecting to the Binance WebSocket API and 
        subscribing to specified streams. It runs as a separate thread to handle 
        incoming WebSocket messages.

        :param symbols: List of symbols to subscribe to (e.g., ['btcusdt', 'ethusdt']).
        :param streams: List of stream types to subscribe to (e.g., ['trade', 'kline_1m']).
        :param data_queue: Queue to which the received data will be put for further processing.
        """
        super().__init__()

        self.socket = "wss://stream.binance.com:9443/ws"
        self.data_queue = data_queue
        self.stop_flag = threading.Event()


    def on_message(self, ws, message):
        """
        Callback function for handling incoming messages.

        :param ws: WebSocket object.
        :param message: Message received from the WebSocket.
        """
        data = json.loads(message)
        self.data_queue.put(data)


    def on_error(self, ws, error):
        """
        Callback function for handling errors.

        :param ws: WebSocket object.
        :param error: Error encountered.
        """
        print("WS Error: ", error)


    def on_close(self, ws, close_status_code, close_msg):
        """
        Callback function for handling the closing of the WebSocket.

        :param ws: WebSocket object.
        :param close_status_code: Status code for the closure.
        :param close_msg: Close message.
        """
        print("### WebSocket Closed ###")


    def on_open(self, ws):
        """
        Callback function for handling the opening of the WebSocket.

        :param ws: WebSocket object.
        """
        # Construct the subscription message
        stream_names = [f"{symbol}@{stream}" for symbol in cfg.SYMBOLS for stream in cfg.BINANCE_WS_CONFIG.get('streams')]
        params = {
            "method": "SUBSCRIBE",
            "params": stream_names,
            "id": 1
        }
        ws.send(json.dumps(params))
        print("WebSocket Connected")


    def stop(self):
        """
        Signal the thread to stop.

        Sets the stop_flag to True, which will cause the main loop in the run method 
        to exit and the thread to stop.
        """
        self.stop_flag.set()


    def run(self):
        """
        The main method of the thread.

        This method sets up the WebSocket connection and enters a loop that keeps 
        running until the stop_flag is set to True. It handles incoming messages 
        by putting them into the data_queue.
        """
        self.ws = websocket.WebSocketApp(self.socket,
                                    on_open=self.on_open,
                                    on_message=self.on_message,
                                    on_error=self.on_error,
                                    on_close=self.on_close)

        # Run the WebSocket in a loop that can be interrupted
        while not self.stop_flag.is_set():
            # The ping_interval and ping_timeout will allow the loop to be interrupted
            # and check the stop_flag. Adjust these values as needed.
            self.ws.run_forever(ping_interval=10, ping_timeout=5)
            
        # If stop_flag is True, the loop will exit here
        self.ws.close()  # Close the WebSocket connection


