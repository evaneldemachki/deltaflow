import hashlib
import pandas
import json

# Return UTF-8 encode name or dataframe column values object
def colencode(data):
    if isinstance(data, pandas.Series):
        return data.name.encode('utf-8')
    else:
        return str(data.columns.values).encode('utf-8')

# Generate SHA1 hash of pandas.DataFrame object
def hash_data(data):
    data_hash = hashlib.sha1()
    data_hash.update(colencode(data)) # p1: data columns
    data_hash.update( # p2: data content
        pandas.util.hash_pandas_object(
            data, index=True).values
    )

    return data_hash.hexdigest()

# Convert json dumped node to SHA1 hash
def hash_node(node_str: str) -> str:
    node_hash = hashlib.sha1()
    node_hash.update(node_str.encode('utf-8'))

    return node_hash.hexdigest()

# Combine node_hash and deltahash to create node_id
def hash_pair(node_hash, data_hash):
    node_id = hashlib.sha1()
    node_id.update(node_hash.encode('utf-8'))
    node_id.update(data_hash.encode('utf-8'))
    
    return node_id.hexdigest()