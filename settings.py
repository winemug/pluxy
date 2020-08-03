import simplejson as json


def _get_settings():
    with open("settings.json", "r") as stream:
        return json.load(stream)


def get_mongo_uri():
    return _get_settings()["mongo_uri"]


def get_db_name():
    return _get_settings()["mongo_db_name"]


def get_ns_bg_collection_name():
    return _get_settings()["mongo_bg_entries"]


def get_ns_treatments_collection_name():
    return _get_settings()["mongo_treatments"]


def get_omnipy_entries_collection_name():
    return _get_settings()["mongo_omnipy_entries"]


def get_omnipy_pods_collection_name():
    return _get_settings()["mongo_omnipy_pods"]