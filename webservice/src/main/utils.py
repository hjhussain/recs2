import datetime as dt
import glob
import os
import os.path as path
import pickle
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def flatten(coll):
    return [item for sublist in coll for item in sublist]


def str_to_datetime(s, date_format="%Y/%m/%d"):
    return dt.datetime.strptime(s, date_format)


def datetime_to_str(d, date_format="%Y/%m/%d"):
    return d.strftime(date_format)


def now():
    return int(time.time())


def measure_time(fn):
    starttime = now()
    fn()
    endtime = now()
    return endtime - starttime


def days_in_range(start_date, days_range):
    return list(map(lambda d: datetime_to_str(start_date - dt.timedelta(days=d)), days_range))


def mk_path(base, *args):
    return os.path.join(base, *args)


def paths(base, *args):
    return glob.glob(mk_path(base, *args))


def empty_dir(dirname):
    return len(paths(dirname, "*")) == 0


def paths_from_date(base_dir, end_date, delta_days):
    dates = days_in_range(end_date, range(0, delta_days))
    return flatten(list(map(lambda d: paths(base_dir, d, '*'), dates)))


def format_path(name, date):
    if "%" in name:
        return name % (date.year, date.month, date.day)
    return name


def mk_dataframe(names, values):
    return pd.DataFrame.from_items(zip(names, values))


def read_parquet_data(base_dir, date, delta=0, nthreads=1):
    files = paths_from_date(base_dir, date, delta)
    return pq.ParquetDataset(files).read(nthreads=nthreads).to_pandas()


def save_parquet_data(filename, df, nthreads=1):
    table = pa.Table.from_pandas(df=df, nthreads=nthreads)
    pq.write_table(table, filename, flavor='spark')


def read_csv(path, headers):
    return pd.read_csv(path, names=headers)


def save_csv(path, df, delimiter=",", header=False):
    df.to_csv(path, sep=delimiter, header=header, index=False)


def resource_path(name):
    return mk_path(os.path.dirname(os.path.realpath(__file__)), 'resources', name)


def relative_path(name):
    return mk_path(os.path.dirname(os.path.realpath(__file__)), name)


def mk_dir(dirname):
    try:
        os.makedirs(dirname, 0o777, True)
    except FileExistsError:
        pass
    except FileNotFoundError:
        pass


def load_data(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)


def save_data(data, filename):
    if path.sep in filename:
        mk_dir(path.dirname(filename))
    with open(filename, "wb") as f:
        pickle.dump(data, f)
