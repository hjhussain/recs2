import logging as log
import os.path
import re

from azure.storage.blob import BlockBlobService

from key_utils import get_key
from utils import *


def __blob_service(name, key):
    return BlockBlobService(account_name=name, account_key=key)


def extract_path_data(source_path):
    regex = re.compile('wasb(s)?://([a-zA-Z0-9]+)@([a-zA-Z0-9]+).blob.core.windows.net/(.*)')
    return regex.findall(source_path)[0][1:]


def get_connection_str(name):
    key = get_key(name)
    conn = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net" % (name, key)
    return conn


def upload_blob(blob_path, from_dir):
    container, storage, prefix = extract_path_data(blob_path)
    upload(storage, container, from_dir, prefix)


def upload(storage, container, base, prefix):
    key = get_key(storage)
    service = __blob_service(name=storage, key=key)
    service.create_container(container)
    log.info("Uploading from path=%s to %s@%s/%s...", base, container, storage, prefix)
    for filename in os.listdir(base):
        blob_name = mk_path(prefix, filename)
        fullname = mk_path(base, filename)
        service.create_blob_from_path(container, blob_name, fullname)
    log.info("Done Uploading from %s", base)


def download_blob(blob_path, to_dir):
    container, storage, prefix = extract_path_data(blob_path)
    download(storage, container, prefix, to_dir)


def download(storage, container, prefix, to_dir):
    key = get_key(storage)
    service = __blob_service(name=storage, key=key)

    log.info("Downloading from %s@%s/%s to %s", container, storage, prefix, to_dir)
    blobs = service.list_blobs(container_name=container, prefix=prefix)
    for b in blobs:
        filename = os.path.basename(b.name)
        if filename != "":
            service.get_blob_to_path(container, b.name, mk_path(to_dir, filename))
    log.info("Done downloading %s", to_dir)


def download_from_date(end_date, days_to_fetch, input_blob, input_dir, overwrite=False):
    for i in range(0, days_to_fetch):
        date = end_date - dt.timedelta(i)
        current_dir = mk_path(input_dir, "%04d" % date.year, "%02d" % date.month, "%02d" % date.day)
        mk_dir(current_dir)
        if overwrite or empty_dir(current_dir):
            download_blob(format_path(input_blob, date), current_dir)
