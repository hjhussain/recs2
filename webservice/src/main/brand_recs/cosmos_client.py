import numpy as np
import pydocumentdb.document_client as document_client
import pydocumentdb.errors as errors


class CosmosClient(object):
    def __init__(self, host, key):
        self.client = document_client.DocumentClient(host, {'masterKey': key})

        database_id = "recommendation_engine"
        collection_id = "product_recs_None_f"
        database_link = 'dbs/' + database_id
        self.collection_link = database_link + '/colls/' + collection_id

        self.collection = self.client.ReadCollection(collection_link=self.collection_link)

    def retrieve_customer_vector(self, user_id):
        try:
            # query = {
            #     "query": "SELECT * FROM server s WHERE s.userId=@userId",
            #     "parameters": [{"name": "@userId", "value": str(user_id)}]
            # }
            query = {'query': 'SELECT * FROM server s WHERE s.userId = ' + str(user_id)}

            # options = {}
            # options['partitionKey'] = str(user_id)
            # options['maxItemCount'] = 1
            # options['enableCrossPartitionQuery'] = False

            result_iterable = self.client.QueryDocuments(self.collection['_self'], query)

            results = list(result_iterable)[0]['features']

            customer_vector = np.array(results)

            return customer_vector
        except errors.DocumentDBError as e:
            if e.status_code == 404:
                print("Document doesn't exist")
            elif e.status_code == 400:
                # Can occur when we are trying to query on excluded paths
                print("Bad Request exception occurred: ", e)
                pass
            else:
                raise
        finally:
            print()

    def retrieve_customer_vector2(self, user_id):
        try:
            doc_id = str(user_id)
            doc_link = self.collection_link + '/docs/' + doc_id
            response = self.client.ReadDocument(doc_link, {'partitionKey': str(user_id)})
            results = response['features']

            customer_vector = np.array(results)

            return customer_vector
        except errors.DocumentDBError as e:
            if e.status_code == 404:
                print("Document doesn't exist")
            elif e.status_code == 400:
                # Can occur when we are trying to query on excluded paths
                print("Bad Request exception occurred: ", e)
                pass
            else:
                raise
        finally:
            print()
