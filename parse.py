import os, sys, io, libarchive
import pyarrow as pa
import pandas as pd
import numpy as np
from attr.validators import instance_of
from pandas.io.json._json import JsonReader


def parse_data(file_path, columns, verbose=1, nrows=1000):
    """
    Чтение файла (csv,gz,7z) в pandas (ts,open,high,low,close,vol,count)
    """

    if columns in ('ts_last_vol', 'ts_open_hi_low_close_vol'):
        data = pd.read_csv(file_to_pandas(file_path), nrows=nrows,
            parse_dates=[['<DATE>', '<TIME>']])
        if columns == 'ts_last_vol': data = data.rename(columns={'<DATE>_<TIME>': 'ts', '<LAST>': 'close', '<VOL>': 'vol'})
        else: data = data.rename(columns={'<DATE>_<TIME>': 'ts', '<OPEN>': 'open', '<HIGH>': 'high', '<LOW>': 'low', '<CLOSE>': 'close', '<VOL>': 'vol'})

    elif columns in ('json_interval_hi_vol'):
        data = pd.read_json(file_to_pandas(file_path), lines=True, chunksize=nrows,
            convert_dates=['time_period_start', 'time_period_end', 'time_open', 'time_close'], orient='records')
        if isinstance(data, JsonReader): data = next(data)  # todo: read all
        data = data.rename(columns={'time_open': 'ts', 'price_open': 'open', 'price_high': 'high', 'price_low': 'low',
                             'price_close': 'close', 'volume_traded': 'vol', 'trades_count': 'count'}, errors="raise")

    else:
        data = pd.read_csv(file_to_pandas(file_path), engine='python', nrows=nrows)

    if verbose:
        print(f'read file {file_path}')
        print(f'lines: {data.shape[0]}')
        print(data.columns)
        print(data.head(3).values)
        print()

    data = data.set_index('ts')
    return data


class GeneratorToFileReader(io.TextIOBase):
    def __init__(self, it): self.it = it
    def read(self, size): return next(self.it)


def file_to_pandas(file_path):
    if file_path.endswith('.7z'):
        def iterator7z():
            import libarchive
            with libarchive.file_reader(file_path) as e:
                for entry in e:
                    for block in entry.get_blocks():
                        yield block
        return GeneratorToFileReader(iterator7z())
    else:
        return file_path


def save_time_series(data, file_path):
    # table = pa.Table.from_pandas(data) # pandas.core.frame.DataFrame to pyarrow.lib.Table
    # with os.open(file_path, "wb") as file: pa.
    data.to_csv(file_path, index=False, compression='gz', line_terminator='\r\n')


def resample_time_series(data, interval='24H'):
    return data.resample(interval).agg({
        "high": {"high": np.max},
        "low": {"low": np.min},
        "vol": {"vol_plus": np.sum, "vol_minus": np.min, "vol": np.sum},
        "count": {"count": np.sum}
        })


# <TICKER>,<PER>,<DATE>,<TIME>,<LAST>,<VOL>
# BTC-PERPETUAL,0,20190508,221239,5881.500000000,1080
parse_data('data/2019-05-01_2019-06-01_BTC_PERP_DER_TICKS.txt', 'ts_last_vol')

# <TICKER>,<PER>,<DATE>,<TIME>,<OPEN>,<HIGH>,<LOW>,<CLOSE>,<VOL>
# bitstamp 1 2011-09-13 00:00:00 135300 5.8 6.0 5.8 6.0 25.0
parse_data('data/BITSTAMP_SPOT_BTC_USD_1MIN_tslab (1).txt.gz', 'ts_open_hi_low_close_vol')

# bitfinex,1,20190526,144300,7998.0,8000.0,7998.0,7998.4,0.3683383\r\n
parse_data('data/TSLAB_1m_BTUSD_bitfinex.7z', 'ts_open_hi_low_close_vol')
parse_data('data/TSLAB_1m_ETHUSD_bitfinex (1).7z', 'ts_open_hi_low_close_vol')

# time_period_start, time_period_end, time_open, time_close, price_open, price_high, price_low, price_close, volume_traded, trades_count
# 2011-09-13T13:53 2011-09-13T13:54 2011-09-13T13:53:36 2011-09-13T13:53:54 5.8 6.0 5.8 6.0 25.0 4
data = parse_data('data/BITSTAMP_SPOT_BTC_USD_1MIN.txt.gz', 'json_interval_hi_vol')


data = data[['high', 'low', 'vol', 'count']]
print(data)
print(resample_time_series(data, '7D'))

