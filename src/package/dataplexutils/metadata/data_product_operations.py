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
import json
import requests


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
            request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""product-initial-metadata"""):
                    if  "external-documentation" in aspect.data:
                        return self._client._table_ops.generate_table_description(table_fqn,aspect.data['external-documentation'])
                    return self._client._table_ops.generate_table_description(table_fqn)
                 
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_valid_json_from_url(self,json_url):
        with open(json_url, 'r') as file:
                json_data = json.load(file)
        return json_data

        

    def initialize_contract_aspect(self,json_url,table_fqn):
        try:
            json_file = self._get_valid_json_from_url(json_url)          
            self._client._dataplex_ops._attach_aspect_from_json(json_file,table_fqn)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def create_contract_aspects(self,json_url, table_fqn):
        """
        Processes the 'contract_tems' array from a JSON URL.

        Loads JSON from the provided URL, then for each item, it checks if an aspect type
        with the specified 'aspect_name' exists. If not, it creates the aspect type
        with the provided fields.

        Args:
            json_url: The URL of the JSON data.
        """
        try:
            #TODO define where is th json comming from 
            # response = requests.get(json_url)
            # response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            # json_data = response.json()

            with open(json_url, 'r') as file:
                json_data = json.load(file)


            # if "contract_tems" not in json_data:
            #     print("Error: 'contract_tems' not found in JSON.")
            #     return

            contract_items = json_data["contract_terms"]

            for item in contract_items:
                aspect_name = item["aspect_name"]
                existing_aspect_type = self._client._dataplex_ops._check_if_exists_aspect_type(aspect_name)
                print(existing_aspect_type)
                if not existing_aspect_type:
                    self._client._dataplex_ops._create_aspect_type_from_json(item)
                self._client._dataplex_ops._update_table_aspect_from_json(item,table_fqn)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")


