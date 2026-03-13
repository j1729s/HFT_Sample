import asyncio
from HFT_Sample.Bybit import run_ticker, run_orderbook

if __name__ == "__main__":
    asyncio.run(run_ticker("BTCUSDT", run_tag="mm_test_01", market="spot"))
    #asyncio.run(run_orderbook("BTCUSDT", run_tag="mm_test_01", market="linear"))