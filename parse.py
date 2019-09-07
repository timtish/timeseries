import io, libarchive
import pandas as pd
import numpy as np
from pandas.io.json._json import JsonReader


def parse_data(file_path, columns, verbose=1, nrows=1000):
    """
    Чтение файла (csv,gz,7z) в pandas

    :param columns кодовое название формата файла - csv/json и список колонок
    :param nrows ограничение количества читаемых строк

    :return  (ts,open,high,low,close,vol,count,ts_end,...)
        ts - дата+время точки или начала интервала
        ts - дата+время закрытия интервала
        open,high,low,close - значения
        vol - сумма за интервал или значение тика
        count - количество за интервал
        остальные поля, если есть, остаются без переименования
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
                             'price_close': 'close', 'volume_traded': 'vol', 'trades_count': 'count', 'time_close': 'ts_end'}, errors="raise")

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
            with libarchive.file_reader(file_path) as e:
                for entry in e:
                    for block in entry.get_blocks():
                        yield block
        return GeneratorToFileReader(iterator7z())
    else:
        return file_path


def save_time_series(data, file_path, form='csv', verbose=1):
    """
    Сохранение файла на диск, с перезаписью!
    :param data: данные
    :param file_path: куда сохранять
    :param form: 'csv' для читаемости или 'parquet' для малого размера
    :param verbose:
    """

    if form == 'csv':
        data.to_csv(file_path,
                    index=True,
                    compression='gzip',  # автоматическое сжатие
                    line_terminator='\r\n')  # так универсальнее

    # формат .parquet в несколько раз компактнее и достаточно распространён при обработке "больших данных"
    # но сжимается стандартным алгоритмом хуже
    if form == 'parquet':
        data.to_parquet(file_path)

    # вывод того что сохраняли
    if verbose:
        print(f"file saved to {file_path}")
        print(data.head())


def resample_time_series(data, interval='24H', form='bar'):
    """
    Приведение к общему виду
    :param data: pandas DataFrame from parse_data()
    :param interval: можно указать None чтобы не реземплить, а только поправить порядок колонок (оставив только нужные)
    :param form: формат результата (набор колонок)
            'bar' - свеча/бар (агрегированые ресемплингом по заданному интервалу)
                (ts, open, high, low, close, vol, count)
            'tick' - тик (можно получить не из всех датасетов)
                (ts, 'bid', 'ask', last, vol, flags)
            'funnel' - какая-то урезанная свеча, встречается в некоторых примерах
                (ts, open, high, low)
        колонка ts присутствует в наборе данных в качестве индекса а не отдельной колонки
    """

    def first(x): return x[0]
    def last(x): return x[-1]
    def count(x): return len(x)

    if 'bar' == form:

        if interval is None:  # только пересборка порядка столбцов
            return data[['open', 'high', 'low', 'close', 'vol']]

        if 'open' not in data.columns:  # конвертация из тиков
            return data.resample(interval).agg({
                'vol': {
                    'vol': np.sum,
                    'count': count},
                'close': {
                    'open': first,
                    'high': np.max,
                    'low': np.min,
                    'close': last}
            })

        else:
            return data.resample(interval).agg({
                'open': lambda x: x[0],
                'high': 'max',
                'low': 'min',
                'close': lambda x: x[-1],
                'vol': 'sum',
                'count': 'sum'})

    if 'tick' == form:
        if interval is None:
            return data[['vol']]
        else:
            return data.resample(interval).agg({'vol': 'sum'})

    if 'funnel' == form:
        if interval is None:
            return data[['open', 'high', 'low']]
        else:
            return data.resample(interval).agg({
                'open': {'open': np.min},
                'high': {'high': np.max},
                'low': {'low': np.min}
                })

    # тут можно экспериментировать с другими агрегатами
    return data.resample(interval).agg({
        'open': {'open': first},
        'high': {'high': np.max},
        'low': {'low': np.min},
        'close': {'close': last},
        'vol': {'vol': np.sum,
                'vol_plus': np.sum,  # отдельно сумма только положительных
                'vol_minus': np.min},
        'count': {'count': np.sum}
        })

def debug_parse():

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


    # а теперь, после тестовых чтений разных файлов, само преобразование всего нужного файла
    # исходные файлы в каталоге data лежат только локально, т.к. в guthub не желательно закачивать большие файлы

    data_2 = parse_data('data/BITSTAMP_SPOT_BTC_USD_1MIN_tslab (1).txt.gz', 'ts_open_hi_low_close_vol', nrows=None, verbose=0)
    data_2 = resample_time_series(data_2, form='bar', interval=None)
    save_time_series(data_2, "data/BITSTAMP_SPOT_BTC_USD_1MIN_tslab.candlesticks.csv.gz")

    # проверка
    df = pd.read_csv('data/BITSTAMP_SPOT_BTC_USD_1MIN_tslab.candlesticks.csv.gz', nrows=100, index_col=0, parse_dates=[0])
    print(df.index.values.dtype)
    print(df.head())


# тиковый бар (агрегация по времени)

def parse_ticks():
    # тест
    data_3 = parse_data('data/2019-05-01_2019-06-01_BTC_PERP_DER_TICKS.txt', 'ts_last_vol', verbose=0)
    print(data_3.head(60))
    data_3 = resample_time_series(data_3, form='bar', interval='1min')
    print(data_3.head(3))

    # сохранение
    save_time_series(parse_data('data/2019-05-01_2019-06-01_BTC_PERP_DER_TICKS.txt', 'ts_last_vol', nrows=None)[['close', 'vol']], "data/ticks1.csv.gz")
    # проверка
    print(pd.read_csv('data/ticks1.csv.gz', nrows=10, index_col=0, parse_dates=[0]))


parse_ticks()

