import hsfs
import pandas as pd
from hsfs.core import storage_connector_api

conn = hsfs.connection(
    host='localhost',                 # DNS of your Feature Store instance
    port=8181,                           # Port to reach your Hopsworks instance, defaults to 443
    project='test_3',               # Name of your Hopsworks Feature Store project
    api_key_value='j2IW03oL8bhcrdKk.Vt1CLvxyrRoXtERDtBXhWk2buwu0DeY0K2zbf7M1cbdeOg1oKWznAjoeX8e2bEPP',             # The API key to authenticate with Hopsworks
    hostname_verification=True,          # Disable for self-signed certificates
    engine="python"
)

fs = conn.get_feature_store()

"""
# create
fg_data = []
fg_data.append(("144", "132"))

fg_df = pd.DataFrame(fg_data, columns=["id", "text"])

fg = fs.get_or_create_feature_group(name="fg", version=2, online_enabled=True, primary_key=["id"])

fg.insert(fg_df)
"""

_storage_connector_api = storage_connector_api.StorageConnectorApi()
sc = _storage_connector_api.get_kafka_connector(fs._id)

print(sc.kafka_options())