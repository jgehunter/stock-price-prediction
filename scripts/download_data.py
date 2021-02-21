import fire
from stock_price_prediction.download.ticker_download import GetData
if __name__ == "__main__":
    fire.Fire(GetData)