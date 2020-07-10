from pymongo import MongoClient
import pandas as pd


def get_bg_series(ts_start: float, ts_end: float,
                  mongo_uri: str,
                  db_name: str = "nightscout", collection_name: str = "entries",
                  lowest_valid_bg: int = 40, highest_valid_bg: int = 400) -> pd.Series:
    with MongoClient(mongo_uri) as client:
        db = client[db_name]
        db_filter = {
            '$and': [
                {'date': {'$gte': int(ts_start * 1000)}},
                {'date': {'$lt': int(ts_end * 1000)}},
                {'sgv': {'$gte': lowest_valid_bg}},
                {'sgv': {'$lte': highest_valid_bg}}
            ]
        }

        project = {
            'date': 1,
            'sgv': 1,
            '_id': 0
        }
        entries = db.get_collection(collection_name).find(db_filter, project).sort([("date", 1)])
        df = pd.DataFrame(entries)

    return pd.Series(df['sgv'].array,
                     pd.to_datetime(df['date'].convert_dtypes(convert_integer=True), unit='ms', utc=True))
