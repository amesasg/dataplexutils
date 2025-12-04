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
"""Dataplex Utils Metadata Wizard BigQuery operations
   2024 Google
"""
# Standard library imports
import logging
import toml
import pkgutil

# Cloud imports
from google.cloud.exceptions import NotFound
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest, Forbidden

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])

class BigQueryOperations:
    """BigQuery-specific operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def table_exists(self, table_fqn: str) -> None:
        """Checks if a specified BigQuery table exists.

        Args:
            table_fqn: The fully qualified name of the table
            (e.g., 'project.dataset.table')

        Raises:
            NotFound: If the specified table does not exist.
        """
        try:
            self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(table_fqn)
        except NotFound:
            logger.error(f"Table {table_fqn} is not found.")
            raise NotFound(message=f"Table {table_fqn} is not found.")

    def get_table_schema(self, table_fqn):
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
            table = self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(
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

    def get_table_sample(self, table_fqn, num_rows_to_sample):
        """Retrieves a sample of rows from a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')
            num_rows_to_sample (int): Number of rows to sample from the table

        Returns:
            str: JSON string containing the sampled rows data

        Raises:
            google.api_core.exceptions.BadRequest: If the query is invalid
            google.api_core.exceptions.Forbidden: If the user doesn't have permissions
            Exception: If there is an error retrieving the sample
        """
        try:
            bq_client = self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]]
            query = f"SELECT * FROM `{table_fqn}` LIMIT {num_rows_to_sample}"
            return bq_client.query(query).to_dataframe().to_json()
        except (BadRequest, Forbidden) as e:
            logger.warning(f"BigQuery error when sampling table {table_fqn}: {e}")
            return "[]"
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def get_table_description(self, table_fqn):
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
            table = self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(
                table_fqn
            )
            return table.description
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def update_table_description(self, table_fqn, description):
        """Updates the table description in BigQuery.

        Args:
            table_fqn (str): The fully qualified name of the table
            description (str): The new description to set

        Returns:
            bool: True if successful

        Raises:
            Exception: If there is an error updating the description
        """
        try:
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            client = bigquery.Client(project=project_id)
            table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
            
            # Get existing description and format the new one
            existing_description = table.description or ""             
            combined_description = self._client._utils.combine_description(
                existing_description, 
                description, 
                self._client._client_options._description_handling
            )
            
            table.description = combined_description
            client.update_table(table, ["description"])
            
            logger.info(f"Updated description for table {table_fqn}")
            return True
        except Exception as e:
            logger.error(f"Exception updating table description: {e}.")
            raise e

    def update_column_description(self, table_fqn, column_name, description):
        """Updates the column description in BigQuery.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column to update
            description (str): The new description to set

        Returns:
            bool: True if successful

        Raises:
            Exception: If there is an error updating the description
        """
        try:
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            client = bigquery.Client(project=project_id)
            table = client.get_table(f"{project_id}.{dataset_id}.{table_id}")
            
            schema = list(table.schema)
            for field in schema:
                if field.name == column_name:
                    # Get existing description and format the new one
                    existing_description = field.description or ""
                    combined_description = self._client._utils.combine_description(
                        existing_description, 
                        description, 
                        self._client._client_options._description_handling
                    )
                    field.description = combined_description
                    break
            
            table.schema = schema
            client.update_table(table, ["schema"])
            
            logger.info(f"Updated description for column {column_name} in table {table_fqn}")
            return True
        except Exception as e:
            logger.error(f"Exception updating column description: {e}.")
            raise e

    def update_table_schema(self, table_fqn, schema):
        """Updates the schema of a BigQuery table.

        Args:
            table_fqn (str): The fully qualified name of the table
                (e.g., 'project.dataset.table')
            schema (list): List of SchemaField objects representing the new schema

        Raises:
            Exception: If there is an error updating the schema
        """
        try:
            table = self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_table(
                table_fqn
            )
            table.schema = schema
            _ = self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].update_table(
                table, ["schema"]
            )
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e 