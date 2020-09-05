from app_monitor import Monitor
from utils import *

CONFIG = {
    'storage_meta': read_csv(resource_path('storageaccountmetadata.csv'), ["name", "subscription", "resource_group"]),
    'subscriptions': read_csv(resource_path('subscriptions.csv'), ["subscription", "id"]),
    'monitor': Monitor()}


def monitor():
    return CONFIG['monitor']


def update_monitor(value):
    return CONFIG.update({'monitor': value})


def get_storage_meta(name):
    meta = CONFIG['storage_meta']
    subscriptions = CONFIG['subscriptions']
    row = meta[meta['name'] == name]
    return row.merge(subscriptions, left_on='subscription', right_on='subscription').values[0]
