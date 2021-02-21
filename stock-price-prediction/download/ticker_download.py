import yfinance as yf
import pandas as pd

data = yf.download(tickers="SPY",
                   period="1d",
                   auto_adjust=True,
                   )

print(data.to_string())

