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
from google.cloud import storage


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

    def get_product_status(self, table_fqn):
        project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
        client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

        entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
        entry = client.get_entry(request=request)
        for aspect_key, aspect in entry.aspects.items():
            if aspect_key.endswith(f"""product-initial-metadata"""):
                return aspect.data['status']

    def update_product_status(self,table_fqn,status):
        
        client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
        aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/product-initial-metadata"""

        new_aspect = dataplex_v1.Aspect()
        aspect_name = None

        entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
        entry = client.get_entry(request=request)
        for aspect_key, aspect in entry.aspects.items():
            if aspect_key.endswith(f"""product-initial-metadata"""):
                logger.info(f"Updating existing aspect {aspect.data}")
                new_aspect.data = aspect.data
                new_aspect.data.update({
                        "status":status,
                                       })
                new_aspect.aspect_type = aspect_key
                aspect_name = aspect_key
                break
             
        # Create new entry with updated aspect
        
        new_entry = dataplex_v1.Entry()
        new_entry.name = entry_name
        new_entry.aspects[aspect_key] = new_aspect
        # Update entry
        request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
        )

        # Make the request
        response = client.update_entry(request=request)
        logger.info(f"Successfully updated status {status} in table {table_fqn} ")
        return True


    def initialize_contract_aspect(self,json_url,table_fqn):
        try:
            json_file = self._get_valid_json_from_url(json_url)          
            self._client._dataplex_ops._attach_aspect_from_json(json_file,table_fqn)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def get_contract(self, table_fqn):
        try:
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.ALL)
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""product-initial-metadata"""):
                    if  "external-contract" in aspect.data:
                                # Create a client to interact with Google Cloud Storage
                        storage_client = storage.Client()
                        # Get the bucket and blob names from the URI
                        contract_uri =aspect.data['external-contract']
                        logger.info(f"Contract used  {contract_uri}")

                        bucket_name, blob_name =  contract_uri.split("/", 3)[2:]

                        # Get the bucket and blob objects
                        bucket = storage_client.get_bucket(bucket_name)
                        blob = bucket.blob(blob_name)

                        # Download the json file as a string
                        json_data = blob.download_as_text()
                        logger.info (f"json contract to be used. Location : {contract_uri}, data : {json_data}")
                        json_file = json.loads(json_data)
                        return json_file
                        
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None
      
    
    def initialize_contract_aspect_product(self,json_data,table_fqn):
        try:
            contract_items = json_data["contract_terms"]

            for item in contract_items:
                aspect_name = item["aspect_name"]
                #existing_aspect_type = self._client._dataplex_ops._check_if_exists_aspect_type(aspect_name)
                self._client._dataplex_ops._attach_aspect_from_json(item,table_fqn)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def create_contract_aspects_types(self,json_data, table_fqn):
        """
        Processes the 'contract_tems' array from a JSON URL.

        Loads JSON from the provided URL, then for each item, it checks if an aspect type
        with the specified 'aspect_name' exists. If not, it creates the aspect type
        with the provided fields.

        Args:
            json data: Json object.
        """
        #try:

        contract_items = json_data["contract_terms"]

        for item in contract_items:
                aspect_name = item["aspect_name"]
                existing_aspect_type = self._client._dataplex_ops._check_if_exists_aspect_type(aspect_name)
                print(existing_aspect_type)
                if not existing_aspect_type:
                    self._client._dataplex_ops._create_aspect_type_from_json(item)

        #except Exception as e:
        #    print(f"An unexpected error occurred: {e}")

    def validate_contract(self,json_contract, table_fqn):
        #TODO retreive all the aspect and for each of them validate agains the initial contract 
        contract_items = json_contract["contract_terms"]

        # Create a client
        client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

        #aspect_types = [f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]
        project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

        entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.ALL
            )
        overview = None
            
        entry = client.get_entry(request=request)
        report = "{["
        validate_prompt = constants["PROMPTS"]["ASPECT_VALIDATION"]
        for aspect_key, aspect in entry.aspects.items():
            logger.info(f"Processing aspect: {aspect_key}")
            logger.info(f"Aspect path: {aspect.path}")
            for aspect_json in contract_items:

                if aspect_key.endswith(aspect_json.get("aspect_name")):
                    print(aspect_json.get("aspect_name"))
                    #print (aspect_json)
                    #print(aspect)
                    aspect_prompt = f"{validate_prompt} contract json : {aspect_json} contract value : {aspect} "
                    status = self._client._utils.llm_inference_validate_field(aspect_prompt)
                    status = status.replace("```json","").replace("```","")

                    report += status + ","
        report += "]}"
        report = report.replace(",]}","]}")


        return report