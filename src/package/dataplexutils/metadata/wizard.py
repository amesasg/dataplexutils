# pylint: disable=line-too-long
# !/usr/bin/env python
# -*- coding: utf-8 -*-
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
"""Dataplex Utils Metadata Wizard main logic
   2024 Google
"""
from .version import __version__

# OS Imports
import logging
import toml
import pkgutil
import re
import json
import pandas
import time
import datetime
from enum import Enum
import uuid

# Cloud imports
import vertexai
from google.cloud import bigquery
from google.cloud import dataplex_v1
from google.cloud.dataplex_v1 import (
    GetDataScanRequest,
    ListDataScanJobsRequest,
    GetDataScanJobRequest,
)
from google.cloud import datacatalog_lineage_v1

from google.cloud.dataplex_v1.types.datascans import DataScanJob
from google.cloud.exceptions import NotFound
from vertexai.generative_models import GenerationConfig, GenerativeModel, Part
import vertexai.preview.generative_models as generative_models
from google.protobuf import field_mask_pb2, struct_pb2,json_format
import google.api_core.exceptions
import random
from google.cloud import storage

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])


class PromtType(Enum):
    PROMPT_TYPE_TABLE = 0
    PROMPT_TYPE_COLUMN = 1


class PromptManager:
    """Represents a prompt manager."""

    def __init__(self, prompt_type, client_options):
        self._prompt_type = prompt_type
        self._client_options = client_options

    def get_promtp(self):
        try:
            if self._prompt_type == PromtType.PROMPT_TYPE_TABLE:
                return self._get_prompt_table()
            elif self._prompt_type == PromtType.PROMPT_TYPE_COLUMN:
                return self._get_prompt_columns()
            else:
                return None
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_prompt_table(self):
        try:
            # System
            table_description_prompt = constants["PROMPTS"]["SYSTEM_PROMPT"]
            # Base
            table_description_prompt = (
                table_description_prompt
                + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_BASE"]
            )
            # Additional metadata information
            if self._client_options._use_profile:
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_PROFILE"]
                )
            if self._client_options._use_data_quality:
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_QUALITY"]
                )
            if self._client_options._use_lineage_tables:
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_LINEAGE_TABLES"]
                )
            if self._client_options._use_lineage_processes:
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_LINEAGE_PROCESSES"]
                )
            if self._client_options._use_ext_documents:
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_DOCUMENT"]
                )
            if self._client_options._use_human_comments:
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_HUMAN_COMMENTS"]
                )
            # Generation base
            table_description_prompt = (
                table_description_prompt
                + constants["PROMPTS"]["TABLE_DESCRIPTION_GENERATION_BASE"]
            )
            # Generation with additional information
            if (
                self._client_options._use_lineage_tables
                or self._client_options._use_lineage_processes
            ):
                table_description_prompt = (
                    table_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_GENERATION_LINEAGE"]
                )
            # Output format
            table_description_prompt = (
                table_description_prompt + constants["PROMPTS"]["OUTPUT_FORMAT_PROMPT"]
            )
            return table_description_prompt
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_prompt_columns(self):
        try:
            # System
            column_description_prompt = constants["PROMPTS"]["SYSTEM_PROMPT"]
            # Base
            if self._client_options._top_values_in_description==True:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["COLUMN_DESCRIPTION_PROMPT_BASE_WITH_EXAMPLES"]
                )
            else:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["COLUMN_DESCRIPTION_PROMPT_BASE"]
                )
                
            # Additional metadata information
            if self._client_options._use_profile:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_PROFILE"]
                )
            if self._client_options._use_data_quality:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_QUALITY"]
                )
            if self._client_options._use_lineage_tables:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_LINEAGE_TABLES"]
                )
            if self._client_options._use_lineage_processes:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["TABLE_DESCRIPTION_PROMPT_LINEAGE_PROCESSES"]
                )
            if self._client_options._use_human_comments:
                column_description_prompt = (
                    column_description_prompt
                    + constants["PROMPTS"]["COLUMN_DESCRIPTION_PROMPT_HUMAN_COMMENTS"]
                )
            # Output format
            column_description_prompt = (
                column_description_prompt + constants["PROMPTS"]["OUTPUT_FORMAT_PROMPT"]
            )
            return column_description_prompt
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e


class ClientOptions:
    """Represents the client options for the metadata wizard client."""

    def __init__(
        self,
        use_lineage_tables=False,
        use_lineage_processes=False,
        use_profile=False,
        use_data_quality=False,
        use_ext_documents=False,
        persist_to_dataplex_catalog=True,
        stage_for_review=False,
        add_ai_warning=True,
        use_human_comments=False,
        regenerate=False,
        top_values_in_description=True,
        description_handling=constants["DESCRIPTION_HANDLING"]["APPEND"],
        description_prefix=constants["OUTPUT_CLAUSES"]["AI_WARNING"]
    ):
        self._use_lineage_tables = use_lineage_tables
        self._use_lineage_processes = use_lineage_processes
        self._use_profile = use_profile
        self._use_data_quality = use_data_quality
        self._use_ext_documents = use_ext_documents
        self._persist_to_dataplex_catalog = persist_to_dataplex_catalog
        self._stage_for_review = stage_for_review
        self._add_ai_warning = add_ai_warning
        self._use_human_comments = use_human_comments
        self._regenerate = regenerate
        self._top_values_in_description = top_values_in_description
        self._description_handling = description_handling
        self._description_prefix = description_prefix

class Client:
    """Represents the main metadata wizard client."""

    def __init__(
        self,
        project_id: str,
        llm_location: str,
        dataplex_location: str,
        # Removed documentatino uri at options level, will provide URI at method generate_table_description level
        #documentation_uri: str,
        client_options: ClientOptions = None,
    ):
        
        if client_options:
            self._client_options = client_options
        else:
            self._client_options = ClientOptions()
        self._project_id = project_id
        self._dataplex_location = dataplex_location
        self.llm_location = llm_location
        # Removed documentatino uri at options level, will provide URI at method generate_table_description level
        #self._documentation_uri = documentation_uri

        self._cloud_clients = {
            constants["CLIENTS"]["BIGQUERY"]: bigquery.Client(),
            constants["CLIENTS"][
                "DATAPLEX_DATA_SCAN"
            ]: dataplex_v1.DataScanServiceClient(),
            constants["CLIENTS"][
                "DATA_CATALOG_LINEAGE"
            ]: datacatalog_lineage_v1.LineageClient(),
            constants["CLIENTS"]["DATAPLEX_CATALOG"]: dataplex_v1.CatalogServiceClient()
        }
        ## Delete after debugging

    def regenerate_dataset_tables_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Regenerates metadata on the tables of a whole dataset.
        """
        self._client_options._use_human_comments=True
        self._client_options._regenerate = True
        return self.generate_dataset_tables_descriptions(dataset_fqn=dataset_fqn, strategy=strategy, documentation_csv_uri=documentation_csv_uri)

    def generate_dataset_tables_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Generates metadata on the tables of a whole dataset.

        Args:
            dataset_fqn: The fully qualified name of the dataset
            (e.g., 'project.dataset')

        Returns:
          None.

        Raises:
            NotFound: If the specified table does not exist.
        """

        logger.info(f"Generating metadata for dataset {dataset_fqn}.")
        #for table in list:
       #     self.generate_table_description(f"{dataset_fqn}.{table}")
        try:
            logger.info(f"Strategy received: {strategy}")
            logger.info(f"Available strategies: {constants['GENERATION_STRATEGY']}")
            
            # Validate strategy exists
            if strategy not in constants["GENERATION_STRATEGY"]:
                raise ValueError(f"Invalid strategy: {strategy}. Valid strategies are: {list(constants['GENERATION_STRATEGY'].keys())}")
            
            int_strategy = constants["GENERATION_STRATEGY"][strategy]
            logger.info(f"Strategy value: {int_strategy}")
            
            bq_client = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
            bq_client = bigquery.Client()
                        

            if int_strategy not in constants["GENERATION_STRATEGY"].values():
                raise ValueError(f"Invalid strategy: {strategy}.")
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                if documentation_csv_uri == None:
                    raise ValueError("A documentation URI is required for the DOCUMENTED strategy.")

            if self._client_options._regenerate:
                tables = self._list_tables_in_dataset_for_regeneration(dataset_fqn)
            else:
                tables = self._list_tables_in_dataset(dataset_fqn)
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                tables_from_uri = self._get_tables_from_uri(documentation_csv_uri)
                if not self._client_options._regenerate:
                    for table in tables_from_uri:
                        if table[0] not in tables:
                            raise ValueError(f"Table {table} not found in dataset {dataset_fqn}.")

                        self.generate_table_description(table[0], table[1])
                if self._client_options._regenerate:
                    tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                    for table in tables:
                        if self._check_if_table_should_be_regenerated(table):
                            if table not in tables_from_uri_first_elements:
                                raise ValueError(f"Table {table} not found in documentation")
                            self.generate_table_description(table)

            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED_THEN_REST"]:
                tables_from_uri = self._get_tables_from_uri(documentation_csv_uri)
                if not self._client_options._regenerate:
                    for table in tables_from_uri:
                        if table not in tables:
                            raise ValueError(f"Table {table} not found in dataset {dataset_fqn}.")
                        self.generate_table_description(table[0], table[1])
                tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                if self._client_options._regenerate:
                    tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                    for table in tables:
                        if self._check_if_table_should_be_regenerated(table):
                            if table not in tables_from_uri_first_elements:
                                raise ValueError(f"Table {table} not found in documentation")
                            self.generate_table_description(table)
                for table in tables:
                    if table not in tables_from_uri_first_elements:
                        self.generate_table_description(table)
            
            if int_strategy in [constants["GENERATION_STRATEGY"]["NAIVE"], constants["GENERATION_STRATEGY"]["RANDOM"], constants["GENERATION_STRATEGY"]["ALPHABETICAL"]]:
                tables_sorted = self._order_tables_to_strategy(tables, int_strategy)
                for table in tables_sorted:
                    self.generate_table_description(table)

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
    
    def generate_dataset_tables_columns_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Generates metadata on the tables of a whole dataset.

        Args:
            dataset_fqn: The fully qualified name of the dataset
            (e.g., 'project.dataset')

        Returns:
          None.

        Raises:
            NotFound: If the specified table does not exist.
        """
        
        logger.info(f"Generating metadata for dataset {dataset_fqn}.")
        #for table in list:
       #     self.generate_table_description(f"{dataset_fqn}.{table}")
        try:
            logger.info(f"Strategy received: {strategy}")
            logger.info(f"Available strategies: {constants['GENERATION_STRATEGY']}")
            
            # Validate strategy exists
            if strategy not in constants["GENERATION_STRATEGY"]:
                raise ValueError(f"Invalid strategy: {strategy}. Valid strategies are: {list(constants['GENERATION_STRATEGY'].keys())}")
            
            int_strategy = constants["GENERATION_STRATEGY"][strategy]
            logger.info(f"Strategy value: {int_strategy}")
            
            bq_client = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
            bq_client = bigquery.Client()
                        

            if int_strategy not in constants["GENERATION_STRATEGY"].values():
                raise ValueError(f"Invalid strategy: {strategy}.")
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                if documentation_csv_uri == None:
                    raise ValueError("A documentation URI is required for the DOCUMENTED strategy.")

            # If we are regenerating, we need to get the tables that need to be regenerated
            tables = self._list_tables_in_dataset(dataset_fqn)
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                tables_from_uri = self._get_tables_from_uri(documentation_csv_uri)
                for table in tables_from_uri:
                    if table[0] not in tables:
                        raise ValueError(f"Table {table[0]} not found in dataset {dataset_fqn}.")
                    if self._client_options._regenerate and self._check_if_table_should_be_regenerated(table[0]):
                        self.generate_table_description(table[0], table[1])
                    
                    if not self._client_options._regenerate:
                        self.generate_table_description(table[0], table[1])

                    #call column generation because checking for column to-be-regenerated is done per column
                    self.generate_columns_descriptions(table[0],table[1])

            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED_THEN_REST"]:
                tables_from_uri = self._get_tables_from_uri(documentation_csv_uri)
                for table in tables_from_uri:
                    if table not in tables:
                        raise ValueError(f"Table {table[0]} not found in dataset {dataset_fqn}.")
                    if self._client_options._regenerate and self._check_if_table_should_be_regenerated(table[0]):
                        self.generate_table_description(table[0], table[1])
                    
                    if not self._client_options._regenerate:
                        self.generate_table_description(table[0], table[1])

                tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                for table in tables:
                    if table not in tables_from_uri_first_elements:
                        if self._client_options._regenerate and self._check_if_table_should_be_regenerated(table[0]):
                            self.generate_table_description(table[0], table[1])
                
                        if not self._client_options._regenerate:
                            self.generate_table_description(table[0], table[1])
                        
                        self.generate_columns_descriptions(table[0],table[1])

            if int_strategy in [constants["GENERATION_STRATEGY"]["NAIVE"], constants["GENERATION_STRATEGY"]["RANDOM"], constants["GENERATION_STRATEGY"]["ALPHABETICAL"]]:
                tables_sorted = self._order_tables_to_strategy(tables, int_strategy)
                for table in tables_sorted:
                    self.generate_table_description(table)
                    self.generate_columns_descriptions(table)
               # self.generate_column_descriptions(table)

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e


    def generate_table_description(self, table_fqn, documentation_uri=None,human_comments=None):
        """Generates metadata on the tabes.

        Args:
            table_fqn: The fully qualified name of the table
            (e.g., 'project.dataset.table')

        Returns:
          "Table description generated successfully"

        Raises:
            NotFound: If the specified table does not exist.
        """
        logger.info(f"Generating metadata for table {table_fqn}.")
        
        
        self._table_exists(table_fqn)
        # Get base information
        logger.info(f"Getting schema for table {table_fqn}.")
        table_schema_str, _ = self._get_table_schema(table_fqn)
        logger.info(f"Getting sample for table {table_fqn}.")
        table_sample = self._get_table_sample(
            table_fqn, constants["DATA"]["NUM_ROWS_TO_SAMPLE"]
        )
        # Get additional information
        logger.info(f"Getting table quality for table {table_fqn}.")
        table_quality = self._get_table_quality(
            self._client_options._use_data_quality, table_fqn
        )
        logger.info(f"Getting table profile for table {table_fqn}.")
        table_profile = self._get_table_profile(
            self._client_options._use_profile, table_fqn
        )
        try:
            logger.info(f"Getting source tables for table {table_fqn}.")
            table_sources_info = self._get_table_sources_info(
                self._client_options._use_lineage_tables, table_fqn
            )
        except Exception as e:
            logger.error(f"Error getting table sources info for table {table_fqn}: {e}")
            table_sources_info = None
        try:
            logger.info(f"Getting jobs calculating for table {table_fqn}.")
            job_sources_info = self._get_job_sources(
                self._client_options._use_lineage_processes, table_fqn
            )
        except Exception as e:
            logger.error(f"Error getting job sources info for table {table_fqn}: {e}")
            job_sources_info = None
        prompt_manager = PromptManager(
            PromtType.PROMPT_TYPE_TABLE, self._client_options
        )

        if documentation_uri == "":
            documentation_uri = None


        # Get prompt
        table_description_prompt = prompt_manager.get_promtp()
        # Format prompt
        table_description_prompt_expanded = table_description_prompt.format(
            table_fqn=table_fqn,
            table_schema_str=table_schema_str,
            table_sample=table_sample,
            table_profile=table_profile,
            table_quality=table_quality,
            table_sources_info=table_sources_info,
            job_sources_info=job_sources_info,
            human_comments=human_comments
        )
        #logger.info(f"Prompt used is: {table_description_prompt_expanded}.")
        table_description = self._llm_inference(table_description_prompt_expanded,documentation_uri)
        if self._client_options._add_ai_warning==True:
            table_description = f"{constants['OUTPUT_CLAUSES']['AI_WARNING']}{table_description}"
        #logger.info(f"Generated description: {table_description}.")
        
        # Update table
        if not self._client_options._stage_for_review:
            self._update_table_bq_description(table_fqn, table_description)
            if self._client_options._persist_to_dataplex_catalog:
                self._update_table_dataplex_description(table_fqn, table_description)
                logger.info(f"Table description updated for table {table_fqn} in Dataplex catalog")
        else:
            if not self._check_if_exists_aspect_type(constants["ASPECT_TEMPLATE"]["name"]):
                logger.info(f"Aspect type {constants['ASPECT_TEMPLATE']['name']} not exists. Attempting to create it")
                self._create_aspect_type(constants["ASPECT_TEMPLATE"]["name"])
                logger.info(f"Aspect type {constants['ASPECT_TEMPLATE']['name']} created")
            self._update_table_draft_description(table_fqn, table_description,)
            logger.info(f"Table {table_fqn} will not be updated in BigQuery.")
            None
        return "Table description generated successfully"

    def generate_columns_descriptions(self, table_fqn,documentation_uri=None,human_comments=None):
        """Generates metadata on the columns.

        Args:
            table_fqn: The fully qualified name of the table
            (e.g., 'project.dataset.table')

        Returns:
          None.

        Raises:
            NotFound: If the specified table does not exist.
        """
        try:
            logger.info(f"Generating metadata for columns in table {table_fqn}.")
            self._table_exists(table_fqn)
            table_schema_str, table_schema = self._get_table_schema(table_fqn)
            table_sample = self._get_table_sample(
                table_fqn, constants["DATA"]["NUM_ROWS_TO_SAMPLE"]
            )

            # Get additional information
            table_quality = self._get_table_quality(
                self._client_options._use_data_quality, table_fqn
            )
            table_profile = self._get_table_profile(
                self._client_options._use_profile, table_fqn
            )
            try:
                table_sources_info = self._get_table_sources_info(
                    self._client_options._use_lineage_tables, table_fqn
                )
            except Exception as e:
                logger.error(f"Error getting table sources info for table {table_fqn}: {e}")
                table_sources_info = None
            try:
                job_sources_info = self._get_job_sources(
                    self._client_options._use_lineage_processes, table_fqn
                )
            except Exception as e:
                logger.error(f"Error getting job sources info for table {table_fqn}: {e}")
                job_sources_info = None

            if documentation_uri == "":
                documentation_uri = None

            prompt_manager = PromptManager(
                PromtType.PROMPT_TYPE_COLUMN, self._client_options
            )
            # Get prompt
            prompt_manager = PromptManager(
                PromtType.PROMPT_TYPE_COLUMN, self._client_options
            )
            column_description_prompt = prompt_manager.get_promtp()
            # We need to generate a new schema with the updated column
            # descriptions and then swap it
            updated_schema = []
            updated_columns = []

            # Iterate over the columns in the table schema
            for column in table_schema:
                # Extract column information from the table profile
                column_info = self._extract_column_info_from_table_profile(table_profile, column.name)

                if self._client_options._use_human_comments:
                    human_comments = self._get_column_comment(table_fqn,column.name)
                
                # Format the prompt with the column information
                column_description_prompt_expanded = column_description_prompt.format(
                    column_name=column.name,
                    table_fqn=table_fqn,
                    table_schema_str=table_schema_str,
                    table_sample=table_sample,
                    table_profile=column_info,
                    table_quality=table_quality,
                    table_sources_info=table_sources_info,
                    job_sources_info=job_sources_info,
                    human_comments=human_comments
                )


                if self._client_options._regenerate == True and self._check_if_column_should_be_regenerated(table_fqn,column.name) or self._client_options._regenerate == False:
                    #logger.info(f"Prompt used is: {column_description_prompt_expanded}.")
                    column_description = self._llm_inference(
                        column_description_prompt_expanded,
                        documentation_uri=documentation_uri,
                    )
                    if self._client_options._add_ai_warning==True:
                        column_description = f"{constants['OUTPUT_CLAUSES']['AI_WARNING']}{column_description}"

                    updated_schema.append(
                        self._get_updated_column(column, column_description)
                    )
                    if self._client_options._stage_for_review:
                        self._update_column_draft_description(table_fqn,column.name,column_description)
                    updated_columns.append(column)
                    logger.info(f"Generated column description: {column_description}.")
                    
                else:
                    updated_schema.append(column)
                    logger.info(f"Column {column.name} will not be updated.")
            if not self._client_options._stage_for_review:
                self._update_table_schema(table_fqn, updated_schema)
            
            if self._client_options._regenerate:
                for column in updated_columns:
                    logger.info(f"Updating table {table_fqn} column {column.name} as regenerated")
                    self._update_column_metadata_as_regenerated(table_fqn,column.name)

        except Exception as e:
            logger.error(f"Update of column description table {table_fqn} failed.")
            raise e(
                message=f"Generation of column description table {table_fqn} failed."
            )

    def _get_tables_from_uri(self, documentation_csv_uri):
        """Reads the CSV file from Google Cloud Storage and returns the tables.

        Args:
            documentation_csv_uri: The URI of the CSV file in Google Cloud Storage.

        Returns:
            A list of tables.

        Raises:
            Exception: If there is an error reading the CSV file.
        """
        try:
            # Create a client to interact with Google Cloud Storage
            storage_client = storage.Client()

            # Get the bucket and blob names from the URI
            bucket_name, blob_name = documentation_csv_uri.split("/", 3)[2:]

            # Get the bucket and blob objects
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)

            # Download the CSV file as a string
            csv_data = blob.download_as_text()

            # Split the CSV data into lines
            lines = csv_data.split("\n")

            # Remove any empty lines
            lines = [line for line in lines if line.strip()]

            # Extract the table names from the lines
            tables = [(line.split(",")[0], line.split(",")[1].strip()) for line in lines]
            #logger.info(f"Tables extracted from CSV: {tables}")
            for table in tables:
                logger.info(f"Table: {table[0]} doc: {table[1]}")
            return tables
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _order_tables_to_strategy(self, tables, strategy):
        
        if strategy == constants["GENERATION_STRATEGY"]["NAIVE"]:
            return tables
        elif strategy == constants["GENERATION_STRATEGY"]["RANDOM"]:
            tables_copy=tables.copy()
            random.shuffle(tables_copy)
            return tables_copy
        elif strategy == constants["GENERATION_STRATEGY"]["ALPHABETICAL"]:
            return sorted(tables)
        else:
            return tables
        
    def _list_tables_in_dataset(self,dataset_fqn):
        """Lists all tables in a given dataset.

        Args:
            project_id: The ID of the project.
            dataset_id: The ID of the dataset.

        Returns:
            A list of table names.
        """

        client = self._cloud_clients[
                    constants["CLIENTS"]["BIGQUERY"]
                ]
        client = bigquery.Client()

        project_id, dataset_id = self._split_dataset_fqn(dataset_fqn)

        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = client.list_tables(dataset_ref)

        table_names = [str(table.full_table_id).replace(":",".") for table in tables]
        return table_names

    def _list_tables_in_dataset_for_regeneration(self,dataset_fqn):
        """Lists all tables in a given dataset.
        """
        try:
            # Create Dataplex Catalog client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id = self._split_dataset_fqn(dataset_fqn)
            
            

            # Build the search request
            name = f"projects/{project_id}/locations/global"
            query = f"""system=BIGQUERY AND parent:@bigquery/projects/{project_id}/datasets/{dataset_id}"""
            #query = f"""system=BIGQUERY"""
            logger.info(f"Query: {query}")
            # Execute search request
            request = dataplex_v1.SearchEntriesRequest( 
                name=name,
                query=query
            )
            
            table_names = []
            try:
                # Get all pages of results
                search_results = client.search_entries(request=request)

                for result in search_results:
                    if result.dataplex_entry.fully_qualified_name.startswith("bigquery:"):
                        table_fqn = result.dataplex_entry.fully_qualified_name.replace("bigquery:", "")
                        table_names.append(table_fqn)
                        #logger.info(f"result: {result}")
                    
                return table_names        
                     
            except google.api_core.exceptions.PermissionDenied:
                logger.warning(f"Permission denied when searching for tables in dataset {dataset_fqn}")
                # Fall back to using BigQuery client
                return self._list_tables_in_dataset_bigquery(dataset_fqn)
            
        except Exception as e:
            logger.error(f"Error listing tables in dataset {dataset_fqn}: {e}")
            raise e
            
        return self._list_tables_in_dataset(dataset_fqn)

    def _extract_column_info_from_table_profile(self,profile, column_name):
        """
        Extract profile information for a specific column from the table profile JSON.
        
        Args:
            json_data (list): The JSON data containing table profile information
            column_name (str): Name of the column to extract information for
            
        Returns:
            dict: Dictionary containing column profile information or None if column not found
        """
        try:
            # Get the fields from the first profile
            
            if not profile or len(profile) == 0:
                logger.info(f"No profile found for column {column_name}.")
                return None
            
            fields = profile[0]['profile']['fields']
            
            # Find the matching column
            for field in fields:
                if field['name'] == column_name:
                    column_info = {
                        'name': field['name'],
                        'type': field['type'],
                        'mode': field['mode'],
                        'null_ratio': field['profile'].get('nullRatio', 0),
                        'distinct_ratio': field['profile'].get('distinctRatio', 0),
                    }
                    
                    # Add type-specific profile information
                    if 'integerProfile' in field['profile']:
                        column_info.update({
                            'average': field['profile']['integerProfile'].get('average'),
                            'std_dev': field['profile']['integerProfile'].get('standardDeviation'),
                            'min': field['profile']['integerProfile'].get('min'),
                            'max': field['profile']['integerProfile'].get('max'),
                            'quartiles': field['profile']['integerProfile'].get('quartiles')
                        })
                    elif 'stringProfile' in field['profile']:
                        column_info.update({
                            'min_length': field['profile']['stringProfile'].get('minLength'),
                            'max_length': field['profile']['stringProfile'].get('maxLength'),
                            'avg_length': field['profile']['stringProfile'].get('averageLength')
                        })
                    elif 'doubleProfile' in field['profile']:
                        column_info.update({
                            'average': field['profile']['doubleProfile'].get('average'),
                            'std_dev': field['profile']['doubleProfile'].get('standardDeviation'),
                            'min': field['profile']['doubleProfile'].get('min'),
                            'max': field['profile']['doubleProfile'].get('max'),
                            'quartiles': field['profile']['doubleProfile'].get('quartiles')
                        })
                    
                    # Add top N values if available
                    if 'topNValues' in field['profile']:
                        column_info['top_values'] = field['profile']['topNValues']
                    
                    return column_info
                    
            return None
            
        except Exception as e:
            print(f"Error extracting column info: {str(e)}")
            return None

    def _get_updated_column(self, column, column_description):
        try:
            if self._client_options._add_ai_warning==True and column.description is not None:
                try:
                    index = column.description.index(constants['OUTPUT_CLAUSES']['AI_WARNING'])
                    column_description = column.description[:index] + column_description
                except ValueError:
                    column_description = column.description + column_description
            
            return bigquery.SchemaField(
                name=column.name,
                field_type=column.field_type,
                mode=column.mode,
                default_value_expression=column.default_value_expression,
                description=column_description[
                        0 : constants["DATA"]["MAX_COLUMN_DESC_LENGTH"]
                    ],
                fields=column.fields,
                policy_tags=column.policy_tags,
                precision=column.precision,
                max_length=column.max_length,
            )
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _table_exists(self, table_fqn: str) -> None:
        """Checks if a specified BigQuery table exists.

        Args:
            table_fqn: The fully qualified name of the table
            (e.g., 'project.dataset.table')

        Raises:
            NotFound: If the specified table does not exist.
        """
        try:
            self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(table_fqn)
        except NotFound:
            logger.error(f"Table {table_fqn} is not found.")
            raise NotFound(message=f"Table {table_fqn} is not found.")

    def _get_table_schema(self, table_fqn):
        """Retrieves the schema of a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            tuple: A tuple containing:
                - list: Flattened schema fields as dicts with 'name' and 'type'
                - list: Original BigQuery SchemaField objects

        Raises:
            NotFound: If the specified table does not exist.
            Exception: If there is an error retrieving the schema.
        """
        try:
            table = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(
                table_fqn
            )
            schema_fields = table.schema
            flattened_schema = [
                {"name": field.name, "type": field.field_type}
                for field in schema_fields
            ]
            return flattened_schema, table.schema
        except NotFound:
            logger.error(f"Table {table_fqn} is not found.")
            raise NotFound(message=f"Table {table_fqn} is not found.")

    def _get_table_sample(self, table_fqn, num_rows_to_sample):
        """Retrieves a sample of rows from a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')
            num_rows_to_sample (int): Number of rows to sample from the table

        Returns:
            str: JSON string containing the sampled rows data

        Raises:
            bigquery.exceptions.BadRequest: If the query is invalid
            Exception: If there is an error retrieving the sample
        """
        try:
            bq_client = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
            query = f"SELECT * FROM `{table_fqn}` LIMIT {num_rows_to_sample}"
            return bq_client.query(query).to_dataframe().to_json()
        except bigquery.exceptions.BadRequest as e:
            print(f"BigQuery Bad Request: {e}")
            return "[]"
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _split_table_fqn(self, table_fqn):
        """Splits a fully qualified table name into its components.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            tuple: A tuple containing (project_id, dataset_id, table_id)

        Raises:
            Exception: If the table FQN cannot be parsed correctly
        """
        try:
            pattern = r"^([^.]+)[\.:]([^.]+)\.([^.]+)"
            logger.debug(f"Splitting table FQN: {table_fqn}.")
            match = re.search(pattern, table_fqn)
            logger.debug(f"I hope i Found 3 groups: {match.group(1)} {match.group(2)} {match.group(3)}")
            return match.group(1), match.group(2), match.group(3)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        
    def _split_dataset_fqn(self, dataset_fqn):
        """Splits a fully qualified dataset name into its components.

        Args:
            dataset_fqn (str): The fully qualified name of the dataset
                (e.g., 'project.dataset')

        Returns:
            tuple: A tuple containing (project_id, dataset_id)

        Raises:
            Exception: If the dataset FQN cannot be parsed correctly
        """
        try:
            pattern = r"^([^.]+)\.([^.]+)"
            match = re.search(pattern, dataset_fqn)
            return match.group(1), match.group(2)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _construct_bq_resource_string(self, table_fqn):
        """Constructs a BigQuery resource string for use in API calls.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            str: The constructed resource string in the format
                '//bigquery.googleapis.com/projects/{project}/datasets/{dataset}/tables/{table}'

        Raises:
            Exception: If there is an error constructing the resource string
        """
        try:
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            return f"//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_table_scan_reference(self, table_fqn):
        """Retrieves data scan references for a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            list: List of scan reference names associated with the table

        Raises:
            Exception: If there is an error retrieving scan references
        """
        try:
            scan_references = None
            scan_client = self._cloud_clients[
                constants["CLIENTS"]["DATAPLEX_DATA_SCAN"]
            ]
            logger.info(f"Getting table scan reference for table:{table_fqn}.")
            data_scans = scan_client.list_data_scans(
                parent=f"projects/{self._project_id}/locations/{self._dataplex_location}"
            )
            bq_resource_string = self._construct_bq_resource_string(table_fqn)
            scan_references = []
            for scan in data_scans:
                if scan.data.resource == bq_resource_string:
                    scan_references.append(scan.name)
            return scan_references
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_table_profile(self, use_enabled, table_fqn):
        """Retrieves the profile information for a BigQuery table.

        Args:
            use_enabled (bool): Whether profile retrieval is enabled
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            list: Table profile results, or empty list if disabled/not available

        Raises:
            Exception: If there is an error retrieving the table profile
        """
        try:
            table_profile = self._get_table_profile_quality(use_enabled, table_fqn)["data_profile"]
            if not table_profile:
                logger.info(f"No profile found for table in datascans{table_fqn}.")
                #self._client_options._use_profile = False
            return table_profile
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_table_quality(self, use_enabled, table_fqn):
        """Retrieves the data quality information for a BigQuery table.

        Args:
            use_enabled (bool): Whether quality check retrieval is enabled
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            list: Data quality results, or empty list if disabled/not available

        Raises:
            Exception: If there is an error retrieving quality information
        """
        try:
            table_quality = self._get_table_profile_quality(use_enabled, table_fqn)["data_quality"]
            # If the user is requesting to use data quality but there is
            # not data quality information to return, we disable the client
            # options flag so the prompt do not include this.
            if not table_quality:
                logger.info(f"No quality check found for table in datascans{table_fqn}.")
                #self._client_options._use_data_quality = False
            return table_quality
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_table_profile_quality(self, use_enabled, table_fqn):
        """Retrieves both profile and quality information for a BigQuery table.

        Args:
            use_enabled (bool): Whether profile/quality retrieval is enabled
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            dict: Dictionary containing:
                - data_profile (list): Profile results
                - data_quality (list): Quality results
                Both will be empty lists if disabled/not available

        Raises:
            Exception: If there is an error retrieving the information
        """
        try:
            if use_enabled:
                scan_client = self._cloud_clients[
                    constants["CLIENTS"]["DATAPLEX_DATA_SCAN"]
                ]
                data_profile_results = []
                data_quality_results = []
                table_scan_references = self._get_table_scan_reference(table_fqn)
                for table_scan_reference in table_scan_references:
                    if table_scan_reference:
                        for job in scan_client.list_data_scan_jobs(
                            ListDataScanJobsRequest(
                                parent=scan_client.get_data_scan(
                                    GetDataScanRequest(name=table_scan_reference)
                                ).name
                            )
                        ):
                            job_result = scan_client.get_data_scan_job(
                                request=GetDataScanJobRequest(
                                    name=job.name, view="FULL"
                                )
                            )
                            if job_result.state == DataScanJob.State.SUCCEEDED:
                                job_result_json = json.loads(
                                    dataplex_v1.types.datascans.DataScanJob.to_json(
                                        job_result
                                    )
                                )
                                if "dataQualityResult" in job_result_json:
                                    data_quality_results.append(
                                        job_result_json["dataQualityResult"]
                                    )
                                if "dataProfileResult" in job_result_json:
                                    data_profile_results.append(
                                        job_result_json["dataProfileResult"]
                                    )
                return {
                    "data_profile": data_profile_results,
                    "data_quality": data_quality_results,
                }
            else:
                return {
                    "data_profile": [],
                    "data_quality": [],
                }
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_table_sources_info(self, use_enabled, table_fqn):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        try:
            if use_enabled:
                table_sources_info = []
                table_sources = self._get_table_sources(table_fqn)
                for table_source in table_sources:
                    table_sources_info.append(
                        {
                            "source_table_name": table_source,
                            "source_table_schema": self._get_table_schema(table_source),
                            "source_table_description": self._get_table_description(
                                table_source
                            ),
                      #      "source_table_sample": self._get_table_sample(
                      #          table_source, constants["DATA"]["NUM_ROWS_TO_SAMPLE"]
                      #      ),
                        }
                    )
                if not table_sources_info:
                    self._client_options._use_lineage_tables = False
                return table_sources_info
            else:
                return []
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_table_sources(self, table_fqn):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        try:
            lineage_client = self._cloud_clients[
                constants["CLIENTS"]["DATA_CATALOG_LINEAGE"]
            ]
            target = datacatalog_lineage_v1.EntityReference()
            target.fully_qualified_name = f"bigquery:{table_fqn}"
            target_dataset=str(self._get_dataset_location(table_fqn)).lower()
            logger.info(f"_get_table_sources:Searching for lineage links for table {table_fqn}. in dataset {target_dataset}")
            request = datacatalog_lineage_v1.SearchLinksRequest(
                parent=f"projects/{self._project_id}/locations/{target_dataset}",
                target=target,
            )
            link_results = lineage_client.search_links(request=request)
            table_sources = []
            for link in link_results:
                if link.target == target:
                    table_sources.append(
                        link.source.fully_qualified_name.replace("bigquery:", "")
                    )
            return table_sources
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_dataset_location(self, table_fqn):
        try:
            bq_client = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
            project_id, dataset_id, _ = self._split_table_fqn(table_fqn)
            return str(bq_client.get_dataset(f"{project_id}.{dataset_id}").location).lower()
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_job_sources(self, use_enabled, table_fqn):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        try:
            if use_enabled:
                bq_process_sql = []
                lineage_client = self._cloud_clients[
                    constants["CLIENTS"]["DATA_CATALOG_LINEAGE"]
                ]
                target = datacatalog_lineage_v1.EntityReference()
                target.fully_qualified_name = f"bigquery:{table_fqn}"
                dataset_location = self._get_dataset_location(table_fqn)
                logger.info(f"Searching for lineage links for table {table_fqn}.")
                request = datacatalog_lineage_v1.SearchLinksRequest(
                    parent=f"projects/{self._project_id}/locations/{dataset_location}",
                    target=target,
                )
                try:
                    link_results = lineage_client.search_links(request=request)
                except Exception as e:
                    logger.error(f"Cannot find lineage links for table {table_fqn}:exception:{e}.")
                    return []
                    raise e
                
                if len(link_results.links) > 0:
                    links = [link.name for link in link_results]
                    lineage_processes_ids = [
                        process.process
                        for process in lineage_client.batch_search_link_processes(
                            request=datacatalog_lineage_v1.BatchSearchLinkProcessesRequest(
                                parent=f"projects/{self._project_id}/locations/{dataset_location}",
                                links=links,
                            )
                        )
                    ]
                    for process_id in lineage_processes_ids:
                        process_details = lineage_client.get_process(
                            request=datacatalog_lineage_v1.GetProcessRequest(
                                name=process_id,
                            )
                        )
                        if "bigquery_job_id" in process_details.attributes:
                            bq_process_sql.append(
                                self._bq_job_info(
                                    process_details.attributes["bigquery_job_id"],
                                    dataset_location,
                                )
                            )
                    if not bq_process_sql:
                        self._client_options._use_lineage_processes = False
                    return bq_process_sql
                else:
                    self._client_options._use_lineage_processes = False
                    return []
            else:
                return []
        except Exception as e:
            logger.error(f"Exception: {e}.")
            return []
            raise e

    def _bq_job_info(self, bq_job_id, dataset_location):
        """Retrieves information about a BigQuery job.

        Args:
            bq_job_id (str): The ID of the BigQuery job
            dataset_location (str): The location of the dataset

        Returns:
            str: The query associated with the job

        Raises:
            Exception: If there is an error retrieving the job information
        """
        try:
            return (
                self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
                .get_job(bq_job_id, location=dataset_location)
                .query
            )
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _llm_inference(self, prompt, documentation_uri=None):
        retries=3
        base_delay=1
        for attempt in range(retries+1):
            try:
                vertexai.init(project=self._project_id, location=self.llm_location)
                if self._client_options._use_ext_documents:
                    model = GenerativeModel(constants["LLM"]["LLM_VISION_TYPE"])
                else:
                    model = GenerativeModel(constants["LLM"]["LLM_TYPE"])

                generation_config = GenerationConfig(
                    temperature=constants["LLM"]["TEMPERATURE"],
                    top_p=constants["LLM"]["TOP_P"],
                    top_k=constants["LLM"]["TOP_K"],
                    candidate_count=constants["LLM"]["CANDIDATE_COUNT"],
                    max_output_tokens=constants["LLM"]["MAX_OUTPUT_TOKENS"],
                )
                safety_settings = {
                    generative_models.HarmCategory.HARM_CATEGORY_HATE_SPEECH: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    generative_models.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    generative_models.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                        generative_models.HarmCategory.HARM_CATEGORY_HARASSMENT: generative_models.HarmBlockThreshold.BLOCK_ONLY_HIGH,
                    }
                if documentation_uri != None:
                    doc = Part.from_uri(
                        documentation_uri, mime_type=constants["DATA"]["PDF_MIME_TYPE"]
                    )
                    responses = model.generate_content(
                        [doc, prompt],
                        generation_config=generation_config,
                        safety_settings=safety_settings,
                        stream=False,
                    )
                else:
                    responses = model.generate_content(
                        prompt,
                        generation_config=generation_config,
                        stream=False,
                    )
                return responses.text
            except Exception as e:
                if attempt == retries:
                    logger.error(f"Exception: {e}.")
                    raise e
                else:
                    # Exponential backoff - wait longer between each retry attempt
                    time.sleep(base_delay * (2 ** attempt))

    def _get_table_description(self, table_fqn):
        """Retrieves the current description of a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Returns:
            str: The current table description

        Raises:
            Exception: If there is an error retrieving the description
        """
        try:
            table = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(
                table_fqn
            )
            return table.description
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _update_table_bq_description(self, table_fqn, description):
        """Updates the table description in BigQuery."""
        try:
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            client = bigquery.Client(project=project_id)
            table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
            
            # Get existing description and format the new one
            existing_description = table.description or ""             
            combined_description = self._combine_description(existing_description, description, self._client_options._description_handling)
            
            table.description = combined_description
            client.update_table(table, ["description"])
            
            logger.info(f"Updated description for table {table_fqn}")
            return True
        except Exception as e:
            logger.error(f"Exception updating table description: {e}.")
            raise e

    def accept_table_draft_description(self, table_fqn):
        """Method to accept the table draft description

        Args:
            table_fqn: table FQN

        Raises:
            Exception
        """
        from typing import MutableSequence

        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        client = dataplex_v1.CatalogServiceClient()


        aspect_types = [f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}""",
                        f"""projects/dataplex-types/locations/global/aspectTypes/overview"""]
        
        # Create the aspect
        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        aspect=dataplex_v1.Aspect()
        request=dataplex_v1.GetEntryRequest(name=entry_name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
        
        entry = client.get_entry(request=request)
        for aspect in entry.aspects:
            print(f"aspect: {aspect}")
            aspect= entry.aspects[aspect]
            if aspect.aspect_type.endswith(f"""aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""):
                
                for i in aspect.data:
                    if i == "contents":
                        overview=aspect.data[i]

        self._update_table_dataplex_description(table_fqn, overview)
        self._update_table_bq_description(table_fqn, overview)

    def _get_column_comment(self,table_fqn, column_name,comment_number=None):
        """Return comment for coolumn. if comment_number is None return all comments, 
        if comment_number is an integer return the n-th comment

        Args:
            table_fqn: table FQN
            column_name: column name
            comment_number: comment number

        Raises:
            Add stringdocs
        """
        
        from typing import MutableSequence

        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        client = dataplex_v1.CatalogServiceClient()


        aspect_types = [f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]
        # Create the aspect
        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        aspect=dataplex_v1.Aspect()
        request=dataplex_v1.GetEntryRequest(name=entry_name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
        overview=None
        comments=[]
        try:
            entry = client.get_entry(request=request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        comments=[]
        for aspect in entry.aspects:
            logger.info(f"aspect: {aspect}")
            aspect= entry.aspects[aspect]
            logger.info(f"aspect.aspect_type: {aspect.aspect_type}")
            logger.info(f"aspect.path: {aspect.path}")
            if aspect.aspect_type.endswith(f"""aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path==f"Schema.{column_name}":
                for i in aspect.data:
                    if i == "human-comments":
                        if comment_number is None:
                            comments.extend(aspect.data[i])
                        else:
                            comments.append(aspect.data[i][comment_number])

        logger.info(f"comments: {comments}")  

        return comments
        
  
    
    def accept_column_draft_description(self, table_fqn, column_name):
        """Add Moves description from draft aspect to dataplex Overview and BQ

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        from typing import MutableSequence

        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        client = dataplex_v1.CatalogServiceClient()


        aspect_types = [f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]
        # Create the aspect
        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        aspect=dataplex_v1.Aspect()
        request=dataplex_v1.GetEntryRequest(name=entry_name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
        overview=None
        try:
            entry = client.get_entry(request=request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        
        for aspect in entry.aspects:
            logger.info(f"aspect: {aspect}")
            aspect= entry.aspects[aspect]
            logger.info(f"aspect.aspect_type: {aspect.aspect_type}")
            logger.info(f"aspect.path: {aspect.path}")
            if aspect.aspect_type.endswith(f"""aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path.endswith(f"""Schema.{column_name}"""):
                for i in aspect.data:
                    if i == "contents":
                        overview=aspect.data[i]

        #self._update_table_dataplex_description(table_fqn, overview)
        self._update_column_bq_description(table_fqn, column_name, overview)
    
    def _update_column_bq_description(self, table_fqn, column_name, description):
        """Updates the column description in BigQuery."""
        try:
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            client = bigquery.Client(project=project_id)
            table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
            
            schema = list(table.schema)
            for field in schema:
                if field.name == column_name:
                    # Get existing description and format the new one
                    existing_description = field.description or ""
                    combined_description = self._combine_description(existing_description, description, self._client_options._description_handling)
                    field.description = combined_description
                    break
            
            table.schema = schema
            client.update_table(table, ["schema"])
            
            logger.info(f"Updated description for column {column_name} in table {table_fqn}")
            return True
        except Exception as e:
            logger.error(f"Exception updating column description: {e}.")
            raise e

    def regenerate_table_description(self, table_fqn, documentation_uri=None):
        """Add Moves description from draft aspect to dataplex Overview and BQ

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        self._client_options._use_human_comments=True
        self._client_options._regenerate=True
        try:
            output=self.generate_table_description(self,table_fqn)
            self._update_table_metadata_as_regenerated(table_fqn)
            return output
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
    
    def regenerate_columns_descriptions(self, table_fqn,documentation_uri=None,human_comments=None):
        """ Regenerate columns descriptions
        Args:
            table_fqn: table FQN

        Raises:
            Exception
        """
        self._client_options._use_human_comments=True
        self._client_options._regenerate=True
        try:
            output= self.generate_columns_descriptions(table_fqn,documentation_uri,human_comments)
            
            return output
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
    
    def get_comments_to_table_draft_description(self, table_fqn):
        """[STUB] Get all comments for a table's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            list: List of comments associated with the draft description

        TODO: This is a stub that needs to be implemented. Implementation should:
        1. Query Dataplex catalog for the table's draft aspect
        2. Extract and return the comments array from the aspect data
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            
            # Set up aspect type and entry name
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Find the comments in the custom aspect
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    if "human-comments" in aspect.data:
                        return aspect.data["human-comments"]
            
            return []

        except Exception as e:
            logger.error(f"Error getting comments for table {table_fqn}: {e}")
            return []

    def get_negative_examples_to_table_draft_description(self, table_fqn):
        """[STUB] Get all negative examples for a table's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            list: List of negative examples associated with the draft description

        TODO: This is a stub that needs to be implemented. Implementation should:
        1. Query Dataplex catalog for the table's draft aspect
        2. Extract and return the negative examples array from the aspect data
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            
            # Set up aspect type and entry name
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Find the negative examples in the custom aspect
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    if "negative-examples" in aspect.data:
                        return aspect.data["negative-examples"]
            
            return []

        except Exception as e:
            logger.error(f"Error getting negative examples for table {table_fqn}: {e}")
            return []

    def add_comment_to_table_draft_description(self, table_fqn, comment):
        """[STUB] Add a comment to a table's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table
            comment (str): The comment text to add

        Returns:
            bool: True if successful, False otherwise

        TODO: This is a stub that needs to be implemented. Implementation should:
        1. Query Dataplex catalog for the table's draft aspect
        2. Add the new comment to the comments array
        3. Update the aspect with the new comments array
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            
            # Set up aspect type and entry name
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect with updated comments
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type

            # Find and update the comments in the custom aspect
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path == "":
                    new_aspect.data = entry.aspects[i].data
                    comments = new_aspect.data.get("human-comments", [])
                    comments.append({
                        "id": str(uuid.uuid4()),
                        "text": comment,
                        "type": "human",
                        "timestamp": datetime.datetime.now().isoformat()
                    })
                    new_aspect.data["human-comments"] = comments

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            # Update the entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )
            client.update_entry(request=request)
            return True

        except Exception as e:
            logger.error(f"Error adding comment to table {table_fqn}: {e}")
            return False

    def add_comment_to_column_draft_description(self, table_fqn, column_name, comment):
        """[STUB] Add a comment to a column's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column
            comment (str): The comment text to add

        Returns:
            bool: True if successful, False otherwise

        TODO: This is a stub that needs to be implemented. Implementation should:
        1. Query Dataplex catalog for the column's draft aspect
        2. Add the new comment to the comments array
        3. Update the aspect with the new comments array
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            
            # Set up aspect type and entry name
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect with updated comments
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type
            new_aspect.path = f"Schema.{column_name}"

            # Find and update the comments in the custom aspect
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                    new_aspect.data = entry.aspects[i].data
                    comments = new_aspect.data.get("human-comments", [])
                    comments.append({
                        "id": str(uuid.uuid4()),
                        "text": comment,
                        "type": "human",
                        "timestamp": datetime.datetime.now().isoformat()
                    })
                    new_aspect.data["human-comments"] = comments

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            # Update the entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )
            client.update_entry(request=request)
            return True

        except Exception as e:
            logger.error(f"Error adding comment to column {column_name} in table {table_fqn}: {e}")
            return False

    def _update_table_dataplex_description(self, table_fqn, description):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        # Create a client
        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        client = dataplex_v1.CatalogServiceClient()

        entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        aspect_type = f"""projects/dataplex-types/locations/global/aspectTypes/overview"""
        aspect_types = [aspect_type]
        old_overview=None
        aspect_content=None

        try:
            request=dataplex_v1.GetEntryRequest(name=entry_name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            current_entry = client.get_entry(request=request)
            for i in current_entry.aspects:
                if i.endswith(f"""global.overview""") and current_entry.aspects[i].path=="":
                        # Start of Selection
                        from google.protobuf.json_format import MessageToDict,ParseDict
                        logger.info(f"Reading existing aspect {i} of table {table_fqn}")
                        old_overview = dict(current_entry.aspects[i].data)
                        logger.info(f"""old_overview: {old_overview["content"]}""")

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        
        

        # Create the aspect
        aspect = dataplex_v1.Aspect()
        aspect.aspect_type = aspect_type
        aspect_content={}
        #aspect.aspect_type = f"{project_id}/global/{aspect_type_id}"
        if old_overview is not None:
            old_description = old_overview["content"]
            combined_description = self._combine_description(old_description, description, self._client_options._description_handling)
            aspect_content["content"] = combined_description
        else:
            aspect_content = {"content": description}


        logging.info(f"""aspect_content: {aspect_content}""")   
        # Convert aspect_content to a Struct
        data_struct = struct_pb2.Struct()
        data_struct.update(aspect_content)
        aspect.data = data_struct

        overview_path = f"dataplex-types.global.overview"


        print(f"project_id: {project_id}, dataset_id: {dataset_id}, table_id: {table_id}")
        entry = dataplex_v1.Entry()
        entry.name = entry_name
        entry.aspects[overview_path]= aspect



        # Initialize request argument(s)
        request = dataplex_v1.UpdateEntryRequest(
            entry=entry,
            update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
        )
        # Make the request
        try:
            response = client.update_entry(request=request)
            print( f"Aspect created: {response.name}")
            return True
        except Exception as e:
            print(f"Failed to create aspect: {e}")
            return False

    def _update_table_metadata_as_regenerated(self, table_fqn):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        #client = dataplex_v1.CatalogServiceClient()
        
        new_aspect = dataplex_v1.Aspect()
        aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
        aspect_name=f"""{self._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
        aspect_types = [aspect_type]

        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry = dataplex_v1.Entry()
        entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        #entry.aspects[f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""] = aspect
        # Check if the aspect already exists

        try:
            get_request=dataplex_v1.GetEntryRequest(name=entry.name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            entry = client.get_entry(request=get_request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

        data_struct = struct_pb2.Struct()
        for i in entry.aspects:
            if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path=="":
                logger.info(f"Updating aspect {i} with old_values")
                new_aspect.data=entry.aspects[i].data
                new_aspect.data.update({
                                "generation-date" : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "to-be-regenerated" : "false"
                                }
                                )
                logger.info(f"entry.aspects[aspect_name].data: {entry.aspects[i].data}")
                logger.info(f"new_aspect.data: {new_aspect.data}")

        new_entry=dataplex_v1.Entry()
        new_entry.name=entry.name
        new_entry.aspects[aspect_name]=new_aspect

        # Initialize request argument(s)  
        request = dataplex_v1.UpdateEntryRequest(
            entry=new_entry,
            update_mask=field_mask_pb2.FieldMask(paths=["aspects"]), 
            allow_missing=False,
            aspect_keys=[aspect_name]
        )
        # Make the request
        try:
            response = client.update_entry(request=request)
            print( f"Aspect created: {response.name}")
            return True
        except Exception as e:
            print(f"Failed to create aspect: {e}")
            return False

        return True    
    
    def _update_column_metadata_as_regenerated(self, table_fqn,column_name):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        #client = dataplex_v1.CatalogServiceClient()
        logger.info(f"Updating column {column_name} in table {table_fqn} as regenerated")
        try:            
            new_aspect = dataplex_v1.Aspect()
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name=f"""{self._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            aspect_types = [aspect_type]
            logger.info(f"aspect_type: {aspect_type}")
        except Exception as e:
            logger.error(f"Failed to create new aspect")
            logger.error(f"Exception: {e}.")
            raise e

        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry = dataplex_v1.Entry()
        entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        #entry.aspects[f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""] = aspect
        # Check if the aspect already exists

        try:
            get_request=dataplex_v1.GetEntryRequest(name=entry.name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            entry = client.get_entry(request=get_request)
            #logger.info(f"Found entry: {entry}")
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

        data_struct = struct_pb2.Struct()
        try:
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path==f"Schema.{column_name}":
                    logger.info(f"**********Updating new aspect {i} with old_values")
                    new_aspect.data=entry.aspects[i].data
                    
                    new_aspect.path=f"Schema.{column_name}"
                    new_aspect.data.update({
                                    "generation-date" : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                                    "to-be-regenerated" : "false"
                                    }
                                    )
                    logger.info(f"entry.aspects[aspect_name].data: {entry.aspects[i].data}")
                    logger.info(f"new_aspect.data: {new_aspect.data}")
        except Exception as e:
            logger.error(f"Failed to assign data to new aspect copy")
            logger.error(f"Exception: {e}.")
            raise e
        
        new_entry=dataplex_v1.Entry()
        new_entry.name=entry.name
        new_entry.aspects[aspect_name]=new_aspect

        # Initialize request argument(s)  
    
        request = dataplex_v1.UpdateEntryRequest(
            entry=new_entry,
            update_mask=field_mask_pb2.FieldMask(paths=["aspects"]), 
            allow_missing=False,
            aspect_keys=[aspect_name]
        )
        # Make the request
        try:
            response = client.update_entry(request=request)
            print( f"Aspect created: {response.name}")
            return True
        except Exception as e:
            print(f"Failed to create aspect: {e}")
            return False

        return True    


    def _update_table_draft_description(self, table_fqn, description):
        """Updates the draft description for a table in Dataplex.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')
            description (str): The new draft description for the table

        Raises:
            Exception: If there is an error updating the draft description
        """
        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        #client = dataplex_v1.CatalogServiceClient()

        # Load the TOML file for aspect content
        new_aspect_content = {
            "certified" : "false",
            "user-who-certified" : "John Doe",
            "contents" : description,
            "generation-date" : "2023-06-15T10:00:00Z",
            "to-be-regenerated" : "false",
            "human-comments" : [],
            "negative-examples" : [],
            "external-document-uri": "gs://example.com/document"
        }

        print(f"aspect_content: {new_aspect_content}")
        # Create the aspect
        new_aspect = dataplex_v1.Aspect()
        new_aspect.aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
        aspect_name=f"""{self._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
        aspect_types = [new_aspect.aspect_type]


        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry = dataplex_v1.Entry()
        entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        #entry.aspects[f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""] = aspect
        # Check if the aspect already exists
        try:
            get_request=dataplex_v1.GetEntryRequest(name=entry.name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            entry = client.get_entry(request=get_request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

        data_struct = struct_pb2.Struct()
        data_struct.update(new_aspect_content)
        new_aspect.data = data_struct
        for i in entry.aspects:
            if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path=="":
                logger.info(f"Updating aspect {i} with old_values")
                new_aspect.data=entry.aspects[i].data
                new_aspect.data.update({"contents": description,
                                "generation-date" : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "to-be-regenerated" : "false"
                                }
                                )
                logger.info(f"entry.aspects[aspect_name].data: {entry.aspects[i].data}")
                logger.info(f"new_aspect.data: {new_aspect.data}")
            #new_aspect.data=entry.aspects[i].data


        new_entry=dataplex_v1.Entry()
        new_entry.name=entry.name
        new_entry.aspects[aspect_name]=new_aspect

        # Initialize request argument(s)  
        request = dataplex_v1.UpdateEntryRequest(
            entry=new_entry,
            update_mask=field_mask_pb2.FieldMask(paths=["aspects"]), 
            allow_missing=False,
            aspect_keys=[aspect_name]
        )
        # Make the request
        try:
            response = client.update_entry(request=request)
            print( f"Aspect created: {response.name}")
            return True
        except Exception as e:
            print(f"Failed to create aspect: {e}")
            return False

        return True

    def _check_if_table_should_be_regenerated(self, table_fqn):
        """Updates the draft description for a column from a BigQuery table in Dataplex.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')
            column_name (str): The name of the column to update
            description (str): The new draft description for the column

        Raises:
            Exception: If there is an error updating the column description in Dataplex
        """

        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]        

        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn) 
    
        entry = dataplex_v1.Entry()
        entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        #entry.aspects[f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""] = aspect
        aspect_types=[f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]

        # Check if the aspect already exists
        try:
            get_request=dataplex_v1.GetEntryRequest(name=entry.name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            entry = client.get_entry(request=get_request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        
        for i in entry.aspects:
            #logger.info(f"""i: {i} path: "{entry.aspects[i].path}" """)
            if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path==f"" :                
                data_dict = entry.aspects[i].data
                if data_dict["to-be-regenerated"] == True:
                    return True
                else:
                    return False
        return False
    
    def _check_if_column_should_be_regenerated(self, table_fqn,column_name):
        """Updates the draft description for a column from a BigQuery table in Dataplex.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')
            column_name (str): The name of the column to update
            description (str): The new draft description for the column

        Raises:
            Exception: If there is an error updating the column description in Dataplex
        """

        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]        

        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn) 
    
        entry = dataplex_v1.Entry()
        entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        #entry.aspects[f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""] = aspect
        aspect_types=[f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]

        # Check if the aspect already exists
        try:
            get_request=dataplex_v1.GetEntryRequest(name=entry.name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            entry = client.get_entry(request=get_request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        
        for i in entry.aspects:
            #logger.info(f"""i: {i} path: "{entry.aspects[i].path}" """)
            if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path==f"Schema.{column_name}" :                
                data_dict = entry.aspects[i].data
                if data_dict["to-be-regenerated"] == True:
                    return True
                else:
                    return False
        return False

    def _update_column_draft_description(self, table_fqn, column_name, description):
        """Updates the draft description for a column from a BigQuery table in Dataplex.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')
            column_name (str): The name of the column to update
            description (str): The new draft description for the column

        Raises:
            Exception: If there is an error updating the column description in Dataplex
        """

                # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        #client = dataplex_v1.CatalogServiceClient()

        # Load the TOML file for aspect content
        new_aspect_content = {
            "certified" : "false",
            "user-who-certified" : "John Doe",
            "contents" : description,
            "generation-date" : "2023-06-15T10:00:00Z",
            "to-be-regenerated" : "false",
            "human-comments" : [],
            "negative-examples" : [],
            "external-document-uri": "gs://example.com/document"
        }

        print(f"aspect_content: {new_aspect_content}")
        # Create the aspect
        new_aspect = dataplex_v1.Aspect()
        new_aspect.aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
        aspect_name=f"""{self._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
        aspect_types = [new_aspect.aspect_type]


        project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

        entry = dataplex_v1.Entry()
        entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
        #entry.aspects[f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""] = aspect
        # Check if the aspect already exists
        try:
            get_request=dataplex_v1.GetEntryRequest(name=entry.name,view=dataplex_v1.EntryView.CUSTOM,aspect_types=aspect_types)
            entry = client.get_entry(request=get_request)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

        data_struct = struct_pb2.Struct()
        data_struct.update(new_aspect_content)
        new_aspect.data = data_struct
        for i in entry.aspects:
            logger.info(f"""i: {i} path: "{entry.aspects[i].path}" """)
            if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path==f"Schema.{column_name}" :
                logger.info(f"Updating aspect {i} with new values")
                new_aspect.data=entry.aspects[i].data
                new_aspect.data.update({"contents": description,
                                "generation-date" : datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "to-be-regenerated" : "false"
                                }
                                )

            #new_aspect.data=entry.aspects[i].data


        new_entry=dataplex_v1.Entry()
        new_entry.name=entry.name
        new_entry.aspects[aspect_name]=new_aspect

        # Initialize request argument(s)  
        request = dataplex_v1.UpdateEntryRequest(
            entry=new_entry,
            update_mask=field_mask_pb2.FieldMask(paths=["aspects"]), 
            allow_missing=False,
            aspect_keys=[aspect_name]
        )
        # Make the request
        try:
            response = client.update_entry(request=request)
            print( f"Aspect created: {response.name}")
            return True
        except Exception as e:
            print(f"Failed to create aspect: {e}")
            return False

        return True

    def _promote_table_description_from_draft(self, table_fqn):
        """Promotes the draft description to the actual table description.
        This method copies the description from the draft aspect in Dataplex to:
        1. The BigQuery table description
        2. The Dataplex overview aspect (if persist_to_dataplex_catalog is enabled)

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')

        Raises:
            Exception: If there is an error promoting the description
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

            # Set up aspect type and entry name
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Find the draft description in the custom aspect
            draft_description = None
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    draft_description = aspect.data["contents"]
                    break

            if draft_description is None:
                logger.error(f"No draft description found for table {table_fqn}")
                raise ValueError(f"No draft description found for table {table_fqn}")

            # Update BigQuery table description
            self._update_table_bq_description(table_fqn, draft_description)

            # Update Dataplex overview if enabled
            if self._client_options._persist_to_dataplex_catalog:
                self._update_table_dataplex_description(table_fqn, draft_description)
                logger.info(f"Updated Dataplex overview for table {table_fqn}")

            logger.info(f"Successfully promoted draft description for table {table_fqn}")
            return True

        except Exception as e:
            logger.error(f"Failed to promote draft description for table {table_fqn}: {e}")
            raise e
    
    def _promote_column_description_from_draft(self, table_fqn, column_name):
        """Promotes the draft description to the actual column description.
        This method copies the description from the draft aspect in Dataplex to
        the BigQuery column description.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')
            column_name (str): The name of the column to update

        Raises:
            Exception: If there is an error promoting the description
            ValueError: If no draft description is found for the column
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)

            # Set up aspect type and entry name
            aspect_type = f"""projects/{self._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Find the draft description in the custom aspect for the specific column
            draft_description = None
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and aspect.path == f"Schema.{column_name}":
                    draft_description = aspect.data["contents"]
                    break

            if draft_description is None:
                logger.error(f"No draft description found for column {column_name} in table {table_fqn}")
                raise ValueError(f"No draft description found for column {column_name} in table {table_fqn}")

            # Update BigQuery column description
            self._update_column_bq_description(table_fqn, column_name, draft_description)

            logger.info(f"Successfully promoted draft description for column {column_name} in table {table_fqn}")
            return True

        except Exception as e:
            logger.error(f"Failed to promote draft description for column {column_name} in table {table_fqn}: {e}")
            raise e
    
    def _add_comment_to_column_draft_description(self, table_fqn, description):
        """Add stringdocs

        Args:
            Add stringdocs

        Raises:
            Add stringdocs
        """
        None


    def _update_table_schema(self, table_fqn, schema):
        """Updates the schema of a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')
            schema (list): List of SchemaField objects representing the new schema

        Raises:
            Exception: If there is an error updating the schema
        """
        try:
            table = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(
                table_fqn
            )
            table.schema = schema
            _ = self._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].update_table(
                table, ["schema"]
            )
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e
        

    def _create_aspect_type(self,  aspect_type_id: str):
        """Creates a new aspect type in Dataplex catalog.

        Args:
            aspect_type_id (str): The ID to use for the new aspect type

        Raises:
            Exception: If there is an error creating the aspect type
        """
        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

        # Initialize request argument(s)
        aspect_type = dataplex_v1.AspectType()
        full_metadata_template = {
            "type_": constants["ASPECT_TEMPLATE"]["type_"],
            "name": constants["ASPECT_TEMPLATE"]["name"],
            "record_fields": constants["record_fields"]
        }
        import json
        print("Will deploy following template:")
        print(json.dumps(full_metadata_template))
        metadata_template = dataplex_v1.AspectType.MetadataTemplate(full_metadata_template)

        print("Will deploy following template:" + str(metadata_template))
        
        aspect_type.metadata_template = metadata_template
        aspect_type.display_name = constants["ASPECT_TEMPLATE"]["display_name"]

        request = dataplex_v1.CreateAspectTypeRequest(
        parent=f"projects/{self._project_id}/locations/global",
        aspect_type_id = aspect_type_id,
        aspect_type=aspect_type,
        )

        # Make the request
        try:
            operation = client.create_aspect_type(request=request)
        except Exception as e:
            logger.error(f"Failed to create aspect type: {e}")
            raise e

    def _check_if_exists_aspect_type(self,  aspect_type_id: str):
        """Checks if a specified aspect type exists in Dataplex catalog.

        Args:
            aspect_type_id (str): The ID of the aspect type to check

        Returns:
            bool: True if the aspect type exists, False otherwise

        Raises:
            Exception: If there is an error checking the aspect type existence
                beyond a NotFound error
        """
        # Create a client
        client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

        # Initialize request argument(s)
    
        request = dataplex_v1.GetAspectTypeRequest(
            name=f"projects/{self._project_id}/locations/global/aspectTypes/{aspect_type_id}"
        )
        
        # Make the request
        try:
            client.get_aspect_type(request=request)
            return True
        except google.api_core.exceptions.NotFound:
            return False

    def _combine_description(self, old_description, new_description, description_handling):
        if not new_description:
            return old_description

        if description_handling == constants["DESCRIPTION_HANDLING"]["APPEND"]:
            if old_description:
                try:
                    # Try to find the AI warning prefix in old description
                    index = old_description.index(constants['OUTPUT_CLAUSES']['AI_WARNING'])
                    # If found, replace everything after the prefix
                    return old_description[:index] + new_description
                except ValueError:
                    # If no prefix found, append normally
                    return old_description + new_description
            return new_description
        elif description_handling == constants["DESCRIPTION_HANDLING"]["PREPEND"]:
            return new_description + old_description
        elif description_handling == constants["DESCRIPTION_HANDLING"]["REPLACE"]:
            return new_description
        else:
            return old_description

    def _get_review_items_for_dataset(self, dataset_fqn: str, page_size: int = 100, page_token: str = None) -> dict:
        try:
            project_id, dataset_id = self._split_dataset_fqn(dataset_fqn)
            logger.info(f"Processing dataset {dataset_fqn} (project: {project_id}, dataset: {dataset_id})")
            
            # Initialize empty arrays and counters
            review_items = []
            result_count = 0
            next_page_token = None
            
            try:
                # Get Dataplex client
                client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

                # Build search request with proper filtering for the dataset
                name = f"projects/{project_id}/locations/global"
                # Filter for BigQuery entries in the specific dataset with our aspect type
                query = f"""cc"""
                logger.info(f"Built search request - name: {name}, query: {query}")
                
                request = dataplex_v1.SearchEntriesRequest(
                    name=name,
                    query=query,
                    page_size=page_size,
                    page_token=page_token
                )
        
                # Get search results
                response = client.search_entries(request=request)
                logger.info("Got search response")
                logger.info(f"Response: {response}")
                
                # Ensure we have a valid list of entries
                search_results = list(response.entries) if hasattr(response, 'entries') else []
                logger.info(f"Found {len(search_results)} entries in response")
                if not search_results:
                    logger.warning("No entries found in search results")
                    return {
                        "data": {
                            "items": [],
                            "nextPageToken": None,
                            "totalCount": 0
                        }
                    }
                
                # Get next page token if it exists
                next_page_token = getattr(response, 'next_page_token', None)
                logger.info(f"Next page token: {next_page_token}")
                
                # Process each search result
                for result in search_results:
                    if not hasattr(result, 'dataplex_entry'):
                        logger.warning("Result missing dataplex_entry attribute")
                        continue
                    
                    entry = result.dataplex_entry
                    if not entry.fully_qualified_name.startswith("bigquery:"):
                        logger.debug(f"Skipping non-bigquery entry: {entry.fully_qualified_name}")
                        continue
                        
                    table_fqn = entry.fully_qualified_name.replace("bigquery:", "")
                    logger.debug(f"Processing table: {table_fqn}")
                    
                    if not hasattr(entry, 'aspects'):
                        logger.warning(f"Entry {table_fqn} has no aspects")
                        continue
                    
                    # Get table description for current description field
                    table_description = self._get_table_description(table_fqn)
                    
                    # Extract basic information from aspects
                    for aspect_key, aspect in entry.aspects.items():
                        if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}"""):
                            try:
                                # For table-level metadata
                                if aspect.path == "":
                                    comments = []
                                    try:
                                        comments = self.get_comments_to_table_draft_description(table_fqn) or []
                                        negative_examples = self.get_negative_examples_to_table_draft_description(table_fqn) or []
                                        comments.extend(negative_examples)
                                    except Exception as e:
                                        logger.error(f"Error getting comments for {table_fqn}: {str(e)}")
                                        
                                    review_items.append({
                                        "id": f"{table_fqn}#table",
                                        "type": "table",
                                        "name": table_fqn,
                                        "currentDescription": table_description or "",
                                        "draftDescription": aspect.data.get("contents", ""),
                                        "isHtml": False,
                                        "status": "draft",
                                        "lastModified": aspect.data.get("generation-date", datetime.datetime.now().isoformat()),
                                        "comments": comments,
                                        "markedForRegeneration": aspect.data.get("to-be-regenerated", False)
                                    })
                                    result_count += 1
                                # For column-level metadata
                                elif aspect.path.startswith("Schema."):
                                    column_name = aspect.path.replace("Schema.", "")
                                    column_description = ""
                                    try:
                                        # Get current column description from schema
                                        _, schema = self._get_table_schema(table_fqn)
                                        for field in schema:
                                            if field.name == column_name:
                                                column_description = field.description or ""
                                                break
                                    except Exception as e:
                                        logger.error(f"Error getting column description for {table_fqn}.{column_name}: {str(e)}")
                                        
                                    review_items.append({
                                        "id": f"{table_fqn}#column#{column_name}",
                                        "type": "column",
                                        "name": f"{table_fqn}.{column_name}",
                                        "currentDescription": column_description,
                                        "draftDescription": aspect.data.get("contents", ""),
                                        "isHtml": False,
                                        "status": "draft",
                                        "lastModified": aspect.data.get("generation-date", datetime.datetime.now().isoformat()),
                                        "comments": [],
                                        "markedForRegeneration": aspect.data.get("to-be-regenerated", False)
                                    })
                                    result_count += 1
                            except Exception as e:
                                logger.error(f"Error processing aspect for {table_fqn}: {str(e)}")
                                continue
                
            except Exception as e:
                logger.error(f"Error during search_entries call: {str(e)}")
                # Don't raise here - we'll return what we have with an empty items array if needed
            
            logger.info(f"Completed processing. Found {len(review_items)} items for review in dataset {dataset_fqn}")
            
            # Return response wrapped in a data object
            return {
                "data": {
                    "items": review_items,
                    "nextPageToken": next_page_token,
                    "totalCount": result_count
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting review items for dataset {dataset_fqn}: {str(e)}")
            # Return a valid empty response rather than raising
            return {
                "data": {
                    "items": [],
                    "nextPageToken": None,
                    "totalCount": 0
                }
            }

    def _get_review_items_for_table(self, table_fqn: str) -> list:
        """Get all metadata items that need review for a specific table.

        Args:
            table_fqn (str): Table fully qualified name (project.dataset.table)

        Returns:
            list: List of metadata items that need review
        """
        try:
            review_items = []
            
            # Get table metadata - unpack the tuple correctly
            _, schema = self._get_table_schema(table_fqn)
            if not schema:
                return []

            # Check table description
            table_description = self._get_table_description(table_fqn)
            draft_description = self._get_table_draft_description(table_fqn)
            
            if draft_description:
                comments = self.get_comments_to_table_draft_description(table_fqn)
                negative_examples = self.get_negative_examples_to_table_draft_description(table_fqn)
                
                review_items.append({
                    "id": f"{table_fqn}#table",
                    "type": "table",
                    "name": table_fqn,
                    "currentDescription": table_description or "",
                    "draftDescription": draft_description,
                    "isHtml": False,
                    "status": "draft",
                    "lastModified": datetime.datetime.now().isoformat(),
                    "comments": comments + negative_examples,
                    "markedForRegeneration": self._check_if_table_should_be_regenerated(table_fqn)
                })

            # Check column descriptions - use schema instead of table.schema
            for field in schema:
                column_name = field.name
                column_description = field.description
                draft_description = self._get_column_draft_description(table_fqn, column_name)
                
                if draft_description:
                    comments = []  # TODO: Implement column comments
                    review_items.append({
                        "id": f"{table_fqn}#column#{column_name}",
                        "type": "column",
                        "name": f"{table_fqn}.{column_name}",
                        "currentDescription": column_description or "",
                        "draftDescription": draft_description,
                        "isHtml": False,
                        "status": "draft",
                        "lastModified": datetime.datetime.now().isoformat(),
                        "comments": comments,
                        "markedForRegeneration": self._check_if_column_should_be_regenerated(table_fqn, column_name)
                    })

            return review_items
        except Exception as e:
            logging.error(f"Error getting review items for table {table_fqn}: {str(e)}")
            raise

    def _get_table_draft_description(self, table_fqn: str) -> str:
        """Get the draft description for a table.

        Args:
            table_fqn (str): Table fully qualified name

        Returns:
            str: Draft description or None if not found
        """
        try:
            # TODO: Implement getting draft description from metadata store
            return None
        except Exception as e:
            logging.error(f"Error getting draft description for table {table_fqn}: {str(e)}")
            raise

    def _get_column_draft_description(self, table_fqn: str, column_name: str) -> str:
        """Get the draft description for a column.

        Args:
            table_fqn (str): Table fully qualified name
            column_name (str): Column name

        Returns:
            str: Draft description or None if not found
        """
        try:
            # TODO: Implement getting draft description from metadata store
            return None
        except Exception as e:
            logging.error(f"Error getting draft description for column {column_name} in table {table_fqn}: {str(e)}")
            raise

    def accept_review_item(self, item_id: str) -> dict:
        """Accept a review item.

        Args:
            item_id (str): Review item ID in format table_fqn#type[#column_name]

        Returns:
            dict: Status of the operation
        """
        try:
            parts = item_id.split("#")
            if len(parts) == 2:  # Table
                table_fqn = parts[0]
                return self.accept_table_draft_description(table_fqn)
            elif len(parts) == 3:  # Column
                table_fqn = parts[0]
                column_name = parts[2]
                return self.accept_column_draft_description(table_fqn, column_name)
            else:
                raise ValueError(f"Invalid item ID format: {item_id}")
        except Exception as e:
            logging.error(f"Error accepting review item {item_id}: {str(e)}")
            raise

    def reject_review_item(self, item_id: str) -> dict:
        """Reject a review item.

        Args:
            item_id (str): Review item ID in format table_fqn#type[#column_name]

        Returns:
            dict: Status of the operation
        """
        try:
            parts = item_id.split("#")
            if len(parts) == 2:  # Table
                table_fqn = parts[0]
                # TODO: Implement table rejection
                return {"status": "rejected", "id": item_id}
            elif len(parts) == 3:  # Column
                table_fqn = parts[0]
                column_name = parts[2]
                # TODO: Implement column rejection
                return {"status": "rejected", "id": item_id}
            else:
                raise ValueError(f"Invalid item ID format: {item_id}")
        except Exception as e:
            logging.error(f"Error rejecting review item {item_id}: {str(e)}")
            raise

    def edit_review_item(self, item_id: str, description: str) -> dict:
        """Edit a review item's description.

        Args:
            item_id (str): Review item ID in format table_fqn#type[#column_name]
            description (str): New description

        Returns:
            dict: Status of the operation
        """
        try:
            parts = item_id.split("#")
            if len(parts) == 2:  # Table
                table_fqn = parts[0]
                self._update_table_draft_description(table_fqn, description)
                return {"status": "updated", "id": item_id}
            elif len(parts) == 3:  # Column
                table_fqn = parts[0]
                column_name = parts[2]
                self._update_column_draft_description(table_fqn, column_name, description)
                return {"status": "updated", "id": item_id}
            else:
                raise ValueError(f"Invalid item ID format: {item_id}")
        except Exception as e:
            logging.error(f"Error editing review item {item_id}: {str(e)}")
            raise

    def add_review_comment(self, item_id: str, comment: str) -> dict:
        """Add a comment to a review item.

        Args:
            item_id (str): Review item ID in format table_fqn#type[#column_name]
            comment (str): Comment text

        Returns:
            dict: Status of the operation and comment details
        """
        try:
            parts = item_id.split("#")
            if len(parts) == 2:  # Table
                table_fqn = parts[0]
                self._add_comment_to_table_draft_description(table_fqn, comment)
                return {
                    "status": "added",
                    "id": item_id,
                    "comment": {
                        "id": str(uuid.uuid4()),
                        "text": comment,
                        "type": "human",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                }
            elif len(parts) == 3:  # Column
                table_fqn = parts[0]
                column_name = parts[2]
                self._add_comment_to_column_draft_description(table_fqn, comment)
                return {
                    "status": "added",
                    "id": item_id,
                    "comment": {
                        "id": str(uuid.uuid4()),
                        "text": comment,
                        "type": "human",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                }
            else:
                raise ValueError(f"Invalid item ID format: {item_id}")
        except Exception as e:
            logging.error(f"Error adding comment to review item {item_id}: {str(e)}")
            raise

    def mark_table_for_regeneration(self, table_fqn: str) -> bool:
        """Mark a table for regeneration by setting the to-be-regenerated flag in its aspect.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            
            # Set up aspect type and entry name
            aspect_type = f"""projects/{project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect with updated regeneration flag
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type

            # Find and update the aspect data
            aspect_found = False
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path == "":
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data["to-be-regenerated"] = True  # Changed to boolean
                    new_aspect.data["generation-date"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    aspect_found = True
                    break
            
            if not aspect_found:
                logger.warning(f"No existing aspect found for table {table_fqn}. Please generate metadata first.")
                return False

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            # Update the entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,  # Must be False for Dataplex-managed Entry Groups
                aspect_keys=[aspect_name]
            )
            client.update_entry(request=request)
            logger.info(f"Successfully marked table {table_fqn} for regeneration")
            return True

        except Exception as e:
            logger.error(f"Error marking table {table_fqn} for regeneration: {e}")
            return False

    def mark_column_for_regeneration(self, table_fqn: str, column_name: str) -> bool:
        """Mark a column for regeneration by setting the to-be-regenerated flag in its aspect.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create a client
            client = self._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._split_table_fqn(table_fqn)
            
            # Set up aspect type and entry name
            aspect_type = f"""projects/{project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect with updated regeneration flag
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type
            new_aspect.path = f"Schema.{column_name}"

            # Find and update the aspect data
            aspect_found = False
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data["to-be-regenerated"] = True  # Changed to boolean
                    new_aspect.data["generation-date"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    aspect_found = True
                    break

            if not aspect_found:
                logger.warning(f"No existing aspect found for column {column_name} in table {table_fqn}. Please generate metadata first.")
                return False

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            # Update the entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,  # Must be False for Dataplex-managed Entry Groups
                aspect_keys=[aspect_name]
            )
            client.update_entry(request=request)
            logger.info(f"Successfully marked column {column_name} in table {table_fqn} for regeneration")
            return True

        except Exception as e:
            logger.error(f"Error marking column {column_name} in table {table_fqn} for regeneration: {e}")
            return False