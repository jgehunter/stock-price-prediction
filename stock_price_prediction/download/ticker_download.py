import yfinance as yf
import pandas as pd
from pathlib import Path
import datetime
from loguru import logger


class GetData:

    def _download_data(
        self, file_name, target_dir: [Path, str], tickers, start=None, end=None, period=None, interval="1d", group_by="column", auto_adjust=True, prepost=False, threads=True, proxy=None,
    ):
        # create directory
        target_dir = Path(target_dir).expanduser()
        target_dir.mkdir(exist_ok=True, parents=True)
        # name of saved file
        _target_file_name = datetime.datetime.now().strftime("%Y%m%d%H%M%S") + "_" + file_name
        target_path = target_dir.joinpath(_target_file_name)

        logger.warning(
            f"The data is downloaded from Yahoo Finance. Take note that the quality of the data may not be perfect."
        )
        logger.info(f"{file_name} downloading...")

        df = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            period=period,
            interval=interval,
            group_by=group_by,
            auto_adjust=auto_adjust,
            prepost=prepost,
            threads=threads,
            proxy=proxy
        )

        
        df.columns = [' '.join(col).strip() for col in df.columns.values]
        df.to_parquet(target_path, compression="GZIP")
