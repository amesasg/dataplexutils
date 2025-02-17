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
"""Dataplex Utils Metadata Wizard table operations
   2024 Google
"""
# Standard library imports
import logging
import toml
import pkgutil
import random

# Cloud imports
from google.cloud import storage
from google.cloud import dataplex_v1
import google.api_core.exceptions

# Local imports
from .prompt_manager import PromtType, PromptManager

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])

class TableOperations:
    """Table-specific operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def regenerate_dataset_tables_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Regenerates metadata on the tables of a whole dataset."""
        self._client._client_options._use_human_comments = True
        self._client._client_options._regenerate = True
        return self.generate_dataset_tables_descriptions(dataset_fqn=dataset_fqn, strategy=strategy, documentation_csv_uri=documentation_csv_uri)

    def generate_dataset_tables_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Generates metadata on the tables of a whole dataset.

        Args:
            dataset_fqn: The fully qualified name of the dataset
            (e.g., 'project.dataset')
            strategy: The strategy to use for generation
            documentation_csv_uri: Optional URI to documentation CSV

        Returns:
            None.

        Raises:
            NotFound: If the specified table does not exist.
        """
        logger.info(f"Generating metadata for dataset {dataset_fqn}.")
        try:
            logger.info(f"Strategy received: {strategy}")
            logger.info(f"Available strategies: {constants['GENERATION_STRATEGY']}")
            
            # Validate strategy exists
            if strategy not in constants["GENERATION_STRATEGY"]:
                raise ValueError(f"Invalid strategy: {strategy}. Valid strategies are: {list(constants['GENERATION_STRATEGY'].keys())}")
            
            int_strategy = constants["GENERATION_STRATEGY"][strategy]
            logger.info(f"Strategy value: {int_strategy}")

            if int_strategy not in constants["GENERATION_STRATEGY"].values():
                raise ValueError(f"Invalid strategy: {strategy}.")
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                if documentation_csv_uri is None:
                    raise ValueError("A documentation URI is required for the DOCUMENTED strategy.")

            if self._client._client_options._regenerate:
                tables = self._list_tables_in_dataset_for_regeneration(dataset_fqn)
            else:
                tables = self._list_tables_in_dataset(dataset_fqn)
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                tables_from_uri = self._get_tables_from_uri(documentation_csv_uri)
                if not self._client._client_options._regenerate:
                    for table in tables_from_uri:
                        if table[0] not in tables:
                            raise ValueError(f"Table {table} not found in dataset {dataset_fqn}.")
                        self.generate_table_description(table[0], table[1])
                if self._client._client_options._regenerate:
                    tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                    for table in tables:
                        if self._check_if_table_should_be_regenerated(table):
                            if table not in tables_from_uri_first_elements:
                                raise ValueError(f"Table {table} not found in documentation")
                            self.generate_table_description(table)

            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED_THEN_REST"]:
                tables_from_uri = self._get_tables_from_uri(documentation_csv_uri)
                if not self._client._client_options._regenerate:
                    for table in tables_from_uri:
                        if table not in tables:
                            raise ValueError(f"Table {table} not found in dataset {dataset_fqn}.")
                        self.generate_table_description(table[0], table[1])
                tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                if self._client._client_options._regenerate:
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

    def generate_table_description(self, table_fqn, documentation_uri=None, human_comments=None):
        """Generates metadata for a table.

        Args:
            table_fqn: The fully qualified name of the table
            documentation_uri: Optional URI to documentation
            human_comments: Optional human comments to consider

        Returns:
            str: Success message if description was generated

        Raises:
            NotFound: If the specified table does not exist.
        """
        logger.info(f"Generating metadata for table {table_fqn}.")
        
        self._client._bigquery_ops.table_exists(table_fqn)
        # Get base information
        logger.info(f"Getting schema for table {table_fqn}.")
        table_schema_str, _ = self._client._bigquery_ops.get_table_schema(table_fqn)
        logger.info(f"Getting sample for table {table_fqn}.")
        table_sample = self._client._bigquery_ops.get_table_sample(
            table_fqn, constants["DATA"]["NUM_ROWS_TO_SAMPLE"]
        )
        # Get additional information
        logger.info(f"Getting table quality for table {table_fqn}.")
        table_quality = self._get_table_quality(
            self._client._client_options._use_data_quality, table_fqn
        )
        logger.info(f"Getting table profile for table {table_fqn}.")
        table_profile = self._get_table_profile(
            self._client._client_options._use_profile, table_fqn
        )
        try:
            logger.info(f"Getting source tables for table {table_fqn}.")
            table_sources_info = self._get_table_sources_info(
                self._client._client_options._use_lineage_tables, table_fqn
            )
        except Exception as e:
            logger.error(f"Error getting table sources info for table {table_fqn}: {e}")
            table_sources_info = None
        try:
            logger.info(f"Getting jobs calculating for table {table_fqn}.")
            job_sources_info = self._get_job_sources(
                self._client._client_options._use_lineage_processes, table_fqn
            )
        except Exception as e:
            logger.error(f"Error getting job sources info for table {table_fqn}: {e}")
            job_sources_info = None

        if documentation_uri == "":
            documentation_uri = None

        # Get prompt
        prompt_manager = PromptManager(
            PromtType.PROMPT_TYPE_TABLE, self._client._client_options
        )
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

        table_description = self._client._utils.llm_inference(table_description_prompt_expanded, documentation_uri)
        if self._client._client_options._add_ai_warning:
            table_description = f"{constants['OUTPUT_CLAUSES']['AI_WARNING']}{table_description}"
        
        # Update table
        if not self._client._client_options._stage_for_review:
            self._client._bigquery_ops.update_table_description(table_fqn, table_description)
            if self._client._client_options._persist_to_dataplex_catalog:
                self._client._dataplex_ops.update_table_dataplex_description(table_fqn, table_description)
                logger.info(f"Table description updated for table {table_fqn} in Dataplex catalog")
        else:
            if not self._check_if_exists_aspect_type(constants["ASPECT_TEMPLATE"]["name"]):
                logger.info(f"Aspect type {constants['ASPECT_TEMPLATE']['name']} not exists. Attempting to create it")
                self._create_aspect_type(constants["ASPECT_TEMPLATE"]["name"])
                logger.info(f"Aspect type {constants['ASPECT_TEMPLATE']['name']} created")
            self._client._dataplex_ops.update_table_draft_description(table_fqn, table_description)
            logger.info(f"Table {table_fqn} will not be updated in BigQuery.")
            None
        return "Table description generated successfully"

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
            for table in tables:
                logger.info(f"Table: {table[0]} doc: {table[1]}")
            return tables
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _order_tables_to_strategy(self, tables, strategy):
        """Orders tables according to the specified strategy.

        Args:
            tables: List of table names
            strategy: Strategy to use for ordering

        Returns:
            Ordered list of table names
        """
        if strategy == constants["GENERATION_STRATEGY"]["NAIVE"]:
            return tables
        elif strategy == constants["GENERATION_STRATEGY"]["RANDOM"]:
            tables_copy = tables.copy()
            random.shuffle(tables_copy)
            return tables_copy
        elif strategy == constants["GENERATION_STRATEGY"]["ALPHABETICAL"]:
            return sorted(tables)
        else:
            return tables

    def _list_tables_in_dataset(self, dataset_fqn):
        """Lists all tables in a given dataset.

        Args:
            dataset_fqn: The fully qualified name of the dataset

        Returns:
            List of table names
        """
        client = self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
        project_id, dataset_id = self._client._utils.split_dataset_fqn(dataset_fqn)
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = client.list_tables(dataset_ref)
        return [str(table.full_table_id).replace(":", ".") for table in tables]

    def _list_tables_in_dataset_for_regeneration(self, dataset_fqn):
        """Lists all tables in a given dataset that need regeneration.

        Args:
            dataset_fqn: The fully qualified name of the dataset

        Returns:
            List of table names that need regeneration
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id = self._client._utils.split_dataset_fqn(dataset_fqn)
            name = f"projects/{project_id}/locations/global"
            query = f"""system=BIGQUERY AND parent:@bigquery/projects/{project_id}/datasets/{dataset_id}"""
            logger.info(f"Query: {query}")
            
            request = dataplex_v1.SearchEntriesRequest(
                name=name,
                query=query
            )
            
            table_names = []
            try:
                search_results = client.search_entries(request=request)
                for result in search_results:
                    if result.dataplex_entry.fully_qualified_name.startswith("bigquery:"):
                        table_fqn = result.dataplex_entry.fully_qualified_name.replace("bigquery:", "")
                        table_names.append(table_fqn)
                return table_names
            except google.api_core.exceptions.PermissionDenied:
                logger.warning(f"Permission denied when searching for tables in dataset {dataset_fqn}")
                return self._list_tables_in_dataset(dataset_fqn)
            
        except Exception as e:
            logger.error(f"Error listing tables in dataset {dataset_fqn}: {e}")
            raise e 