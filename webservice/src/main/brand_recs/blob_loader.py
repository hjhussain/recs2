import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from multiprocessing import Pool
from azure.storage.blob import BlockBlobService


class BlobLoader(object):
    def __init__(self, account_name, account_key, container_name, blob_path):
        self.service = BlockBlobService(account_name=account_name,
                                        account_key=account_key)
        self.container_name = container_name
        self.blob_path = blob_path

    def load_parquet_to_dataframe(self):
        try:
            parquet = pq.read_table("file.parquet").to_pandas()
        except:
            p = Pool(16)
            parquet = pd.concat(
                p.map(self._blob_to_dataframe, self.service.list_blobs(self.container_name, self.blob_path)))

            table = pa.Table.from_pandas(df=parquet)
            pq.write_table(table, "file.parquet", flavor='spark')

        return parquet

    # TODO needs to be generalised to load other datasets
    def _blob_to_dataframe(self, blob):
        if not blob.name.endswith(".parquet"):
            return pd.DataFrame()

        stream = BytesIO()
        self.service.get_blob_to_stream(self.container_name, blob.name, stream)
        reader = pa.BufferReader(stream.getvalue())
        part = pq.read_table(reader).to_pandas()

        return part.set_index("id")['features'].apply(pd.Series)
