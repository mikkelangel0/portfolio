
# Global
SYMBOLS = ["btcusdt"]

MAIN_CONFIG = {
    'runtime': 72 # 8 hours
}


BINANCE_WS_CONFIG = {
    'streams': [
        "aggTrade", 
        "forceOrder"
        ]
}

BINANCE_REST_CONFIG = {
    'rest_interval': 0.5,
    'base_url': "https://testnet.binancefuture.com",
    'requests': [
        "openInterest"
        #"trades"
        ]
}

DATABLOCK_CONFIG = {
    'interval_size': 10,
    'max_intervals': 2
}

DATAPROCESSOR_CONFIG = {}

