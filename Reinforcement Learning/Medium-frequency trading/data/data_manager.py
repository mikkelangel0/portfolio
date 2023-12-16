import threading
import pandas as pd
import time
from datetime import datetime
import queue
import config as cfg

class DataManager(threading.Thread):
    def __init__(self, data_queue) -> None:
        super().__init__()
        self.data_queue = data_queue
        self.stop_flag = threading.Event()
        self.start_time = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
        self.datablocks = pd.DataFrame()
        self.data_history = pd.DataFrame()
        self.datablock_filename = f"datablocks_{self.start_time}_{cfg.DATABLOCK_CONFIG['interval_size']}_{cfg.DATABLOCK_CONFIG['max_intervals']}.csv"

    def run(self):
        first_save = True
        while not self.stop_flag.is_set():
            try:
                # Get data from the queue
                data = self.data_queue.get(timeout=1)
                self.add_block_data(data)

                # Update the CSV file
                self.to_csv(append=not first_save)
                if first_save:
                    first_save = False

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error managing data: {e}")
        
    def add_block_data(self, block_data):
        """
        Add new block data to the datablocks DataFrame in a flattened format.

        :param block_data: The block data to be added.
        """
        # Flatten the block data
        flattened_data = {}
        for i, (price, data) in enumerate(block_data.items(), start=1):
            flattened_data[f'price_{i}'] = price
            flattened_data[f'maker_{i}'] = data['maker']
            flattened_data[f'taker_{i}'] = data['taker']
            flattened_data[f'liq_sell_{i}'] = data['liquidations']['sell_vol']
            flattened_data[f'liq_buy_{i}'] = data['liquidations']['buy_vol']
            flattened_data[f'oi_open_{i}'] = data['open_interest']['open']
            flattened_data[f'oi_high_{i}'] = data['open_interest']['high']
            flattened_data[f'oi_low_{i}'] = data['open_interest']['low']
            flattened_data[f'oi_close_{i}'] = data['open_interest']['close']

        # Convert to DataFrame and append to self.datablocks
        block_df = pd.DataFrame([flattened_data])
        self.datablocks = pd.concat([self.datablocks, block_df], ignore_index=True)

    def to_csv(self, append=False):
        """
        Update the CSV file with the current data.

        :param append: If True, append data to the existing file. If False, overwrite the file.
        """
        mode = 'a' if append else 'w'
        header = not append  # Write header if not appending

        if not self.datablocks.empty:
            self.datablocks.to_csv(self.datablock_filename, mode=mode, index=False, header=header)

    def stop(self):
        self.stop_flag.set()