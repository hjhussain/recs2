# This script generates the scoring and schema files necessary to operationalize your model
import json

import numpy as np
import pandas as pd
from numpy.random import choice

from brand_recs.blob_loader import BlobLoader
from brand_recs.cosmos_client import CosmosClient
from utils import relative_path



def init():
    global model
    with open(relative_path("brand_recs/model.json")) as f:
        model = json.load(f)
    global config
    with open(relative_path("brand_recs/configuration.json")) as f:
        config = json.load(f)

    product_vectors_path = model['product_vectors_path']

    global cosmos_client
    #
    # config = {
    #     "blobAccountName": "asosrecsbatchprod",
    #     "blobAccountKey": "Mya0556qP3pA/NuAnAHzNawbxc4506R6CKOksY687Fn1qrvNX1i1DKlofjcrrdRDsW7Sv93ZTYozqotpvYJubw==",
    #     "cosmosDbHost": "https://asds-brandrecsaml-db-dev.documents.azure.com:443/",
    #     "cosmosDbMasterKey": "aaTI5okPbzvnpyD411kbHXy6Yps78uNo3xAJdjd4wFIcL2uWd6DRl67Qz4go2EA8V8NLqMsYRrtMxyNfCjgacA==",
    #     "topProducts": 100,
    #     "topBrands": 20
    # }

    cosmos_client = CosmosClient(config['cosmosDbHost'], config['cosmosDbMasterKey'])

    products_blob_loader = BlobLoader(account_name=config['blobAccountName'],
                                      account_key=config['blobAccountKey'],
                                      container_name='models',
                                      blob_path=product_vectors_path)

    global product_vectors
    product_vectors = __load_product_vectors(products_blob_loader)
    #
    global prod2brand
    prod2brand = __load_products2brand()

    product_vectors_X = product_vectors.values.copy(order='C')
    print(product_vectors_X.flags)

    print("training ann for: product_vectors.shape=%s" % str(product_vectors_X.shape))
    global product_model
    product_model = RPForest(leaf_size=50, no_trees=20)
    product_model.fit(product_vectors_X)
    print("done training ann")


def score(customer_id):
    customer_vector = cosmos_client.retrieve_customer_vector(customer_id)
    top_products = _compute_top_products(customer_vector, product_vectors, model['num_top_products'])

    # top_brands = _compute_top_brands(prod2brand, top_products, config['topBrands'])
    print(top_products.values)
    return {"recommendedBrands": top_products.values.tolist()}


def score_ann(customer_id):
    customer_vector = cosmos_client.mock_retrieve_customer_vector(customer_id)

    nns = product_model.query(customer_vector, model['num_top_products'])
    return {"ann": nns.tolist()}


def score_dot_only(customer_id):
    customer_vector = cosmos_client.mock_retrieve_customer_vector(customer_id)
    top_products = _compute_top_products(customer_vector, product_vectors, model['num_top_products'])
    return {"dot_only": top_products.tolist()}


def score_db_only(customer_id):
    customer_vector = cosmos_client.retrieve_customer_vector2(customer_id)
    return {"db_only": customer_vector.size}


def score_db_query(customer_id):
    customer_vector = cosmos_client.retrieve_customer_vector(customer_id)
    return {"db_only": customer_vector.size}


def __load_product_vectors(blob_loader):
    return blob_loader.load_parquet_to_dataframe()


# TODO do this properly
def __load_products2brand():
    n_prods = product_vectors.shape[0]
    n_prods_brands = n_prods
    n_brands = 200
    return pd.DataFrame(index=choice(product_vectors.index.tolist(), n_prods_brands, replace=False),
                        data=choice(n_brands, n_prods_brands, replace=True),
                        columns=['brand_id'])


def _compute_top_products(customer_vector, product_vectors, num_top_products):
    return product_vectors.dot(customer_vector.T).nlargest(num_top_products)


def _compute_top_brands(prod2brand, top_products, num_top_brands):
    try:
        top_brands = prod2brand.loc[top_products].dropna() \
            .applymap(int) \
            .groupby('brand_id')['brand_id'] \
            .count() \
            .sort_values(ascending=False) \
            .head(num_top_brands) \
            .index
    except KeyError:
        top_brands = np.array([])

    return top_brands
