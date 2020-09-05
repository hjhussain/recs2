from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.storage import StorageManagementClient

from app_config import get_storage_meta
import os


def env(key):
    return os.environ.get(key)


def __get_credentials():
    return ServicePrincipalCredentials(client_id=env('SERVICE_APP_CLIENT_ID'),
                                       secret=env('SERVICE_APP_CLIENT_SECRET'),
                                       tenant=env('SERVICE_APP_TENANT'))


def __get_storage_account_key(credentials, subscription_id, rg_name, sa_name):
    storage_client = StorageManagementClient(credentials, subscription_id)
    storage_keys = storage_client.storage_accounts.list_keys(rg_name, sa_name)
    return {v.key_name: v.value for v in storage_keys.keys}


def get_key(name):
    row = get_storage_meta(name)
    return __get_storage_account_key(__get_credentials(), row[3], row[2], name)['key1']
