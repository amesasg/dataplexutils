"""
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""Dataplex Utils Metadata Wizard Dataplex operations
   2024 Google
"""
# Standard library imports
import logging
import toml
import pkgutil
import datetime
import uuid

# Cloud imports
from google.cloud import dataplex_v1
from google.protobuf import field_mask_pb2, struct_pb2, json_format
import google.api_core.exceptions

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])

class DataProductOperations:
    """Dataplex-specific operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def create_product_description(self,table_fqn):
        try:
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            #aspect_type = f"""projects/dataplex-types/locations//aspectTypes/overview"""
            #aspect_types = [aspect_type]
            #old_overview = None
            #aspect_content = None
            request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""product-initial-metadata"""):
                    if  "external-documentation" in aspect.data:
                        return self._client._table_ops.generate_table_description(table_fqn,aspect.data['external-documentation'])
                    return self._client._table_ops.generate_table_description(table_fqn)
                        # logger.info(f"Reading existing aspect {i} of table {table_fqn}")
                        # old_overview = dict(current_entry.aspects[i].data)
                        # logger.info(f"""old_overview: {old_overview["content"][1:50]}...""")

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
