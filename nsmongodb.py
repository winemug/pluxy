import time
from pymongo import MongoClient
import pandas as pd


def get_bg_series(mongo_uri: str,
                  ts_start: float,
                  ts_end: float = None,
                  db_name: str = "nightscout", collection_name: str = "entries",
                  lowest_valid_bg: int = 40, highest_valid_bg: int = 400,
                  freq: str = 'T', max_fill: int = None,
                  include_manual_entries: bool = True) -> pd.Series:
    if ts_end is None:
        ts_end = time.time() + 2*60*60

    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        db_filter = {
            '$and': [
                {'date': {'$gte': int(ts_start * 1000)}},
                {'date': {'$lt': int(ts_end * 1000)}},
                {'$or': [
                    {'$and': [
                        {'sgv': {'$gte': lowest_valid_bg}},
                        {'sgv': {'$lte': highest_valid_bg}}
                    ]},
                    {'mbg': {'$ne': None}}
                ]},
            ]
        }

        project = {
            'date': 1,
            'sgv': 1,
            'mbg': 1,
            '_id': 0
        }
        entries = db.get_collection(collection_name).find(db_filter, project).sort([("date", 1)])
        df = pd.DataFrame(entries)

    index = pd.to_datetime(df['date'].convert_dtypes(convert_integer=True), unit='ms', utc=True)
    sgv = pd.Series(df['sgv'].array, index)
    if include_manual_entries:
        mbg = pd.Series(df['mbg'].array, index)
        sgv = sgv.fillna(0) + mbg.fillna(0)
    else:
        sgv.dropna()

    return sgv.resample(freq).mean() \
        .interpolate(method='polynomial', order=2, limit_area='inside', limit=max_fill, limit_direction='backward')
