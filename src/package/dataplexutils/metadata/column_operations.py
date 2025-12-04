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
"""Dataplex Utils Metadata Wizard column operations
   2024 Google
"""
# Standard library imports
import logging
import toml
import pkgutil

# Cloud imports
from google.cloud import bigquery

# Local imports
from .prompt_manager import PromtType, PromptManager

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])

class ColumnOperations:
    """Column-specific operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def regenerate_dataset_tables_columns_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Regenerates metadata on the columns of tables in a whole dataset.

        Args:
            dataset_fqn: The fully qualified name of the dataset
            strategy: The strategy to use for generation
            documentation_csv_uri: Optional URI to documentation CSV
        """
        try:
            self._client._client_options._use_human_comments = True
            self._client._client_options._regenerate = True
            self.generate_dataset_tables_columns_descriptions(dataset_fqn, strategy, documentation_csv_uri)
            
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e




    def generate_dataset_tables_columns_descriptions(self, dataset_fqn, strategy="NAIVE", documentation_csv_uri=None):
        """Generates metadata on the columns of tables in a whole dataset.

        Args:
            dataset_fqn: The fully qualified name of the dataset
            (e.g., 'project.dataset')
            strategy: The strategy to use for generation
            documentation_csv_uri: Optional URI to documentation CSV

        Returns:
            None.

        Raises:
            NotFound: If the specified dataset does not exist.
        """
        logger.info(f"Generating column metadata for dataset {dataset_fqn}.")
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

            # Get tables in dataset
            if self._client._client_options._regenerate:
                tables = self._client._table_ops._list_tables_in_dataset_for_regeneration(dataset_fqn)
                logger.debug(f"Tables to regenerate columns: {tables}")
            else:
                tables = self._client._table_ops._list_tables_in_dataset(dataset_fqn)
                logger.debug(f"Tables to generate columns: {tables}")
            
            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED"]:
                tables_from_uri = self._client._table_ops._get_tables_from_uri(documentation_csv_uri)
                for table in tables_from_uri:
                    if table[0] not in tables:
                        raise ValueError(f"Table {table[0]} not found in dataset {dataset_fqn}.")
                    # Generate columns for this table
                    self.generate_columns_descriptions(table[0], table[1])

            if int_strategy == constants["GENERATION_STRATEGY"]["DOCUMENTED_THEN_REST"]:
                tables_from_uri = self._client._table_ops._get_tables_from_uri(documentation_csv_uri)
                for table in tables_from_uri:
                    if table[0] not in tables:
                        raise ValueError(f"Table {table[0]} not found in dataset {dataset_fqn}.")
                    # Generate columns for this table
                    self.generate_columns_descriptions(table[0], table[1])
                    self._client._table_ops.generate_table_description(table[0])

                tables_from_uri_first_elements = [table[0] for table in tables_from_uri]
                for table in tables:
                    if table not in tables_from_uri_first_elements:
                        self.generate_columns_descriptions(table)
                        self._client._table_ops.generate_table_description(table)
            
            if int_strategy in [constants["GENERATION_STRATEGY"]["NAIVE"], constants["GENERATION_STRATEGY"]["RANDOM"], constants["GENERATION_STRATEGY"]["ALPHABETICAL"]]:
                tables_sorted = self._client._table_ops._order_tables_to_strategy(tables, int_strategy)
                for table in tables_sorted:
                    self.generate_columns_descriptions(table)
                    self._client._table_ops.generate_table_description(table)

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def generate_columns_descriptions(self, table_fqn, documentation_uri=None, human_comments=None):
        """Generates metadata on the columns.

        Args:
            table_fqn: The fully qualified name of the table
            (e.g., 'project.dataset.table')
            documentation_uri: Optional URI to documentation
            human_comments: Optional human comments to consider

        Returns:
            None.

        Raises:
            NotFound: If the specified table does not exist.
        """
        try:
            logger.info(f"Generating metadata for columns in table {table_fqn}.")
            self._client._bigquery_ops.table_exists(table_fqn)
            table_schema_str, table_schema = self._client._bigquery_ops.get_table_schema(table_fqn)
            table_sample = self._client._bigquery_ops.get_table_sample(
                table_fqn, constants["DATA"]["NUM_ROWS_TO_SAMPLE"]
            )

            # Get additional information
            table_quality = self._client._dataplex_ops.get_table_quality(
                self._client._client_options._use_data_quality, table_fqn
            )
            table_profile = self._client._dataplex_ops.get_table_profile(
                self._client._client_options._use_profile, table_fqn
            )
            try:
                table_sources_info = self._client._dataplex_ops.get_table_sources_info(
                    self._client._client_options._use_lineage_tables, table_fqn
                )
            except Exception as e:
                logger.error(f"Error getting table sources info for table {table_fqn}: {e}")
                table_sources_info = None
            try:
                job_sources_info = self._client._dataplex_ops.get_job_sources(
                    self._client._client_options._use_lineage_processes, table_fqn
                )
            except Exception as e:
                logger.error(f"Error getting job sources info for table {table_fqn}: {e}")
                job_sources_info = None

            if documentation_uri == "":
                documentation_uri = None

            prompt_manager = PromptManager(
                PromtType.PROMPT_TYPE_COLUMN, self._client._client_options
            )
            # Get prompt
            column_description_prompt = prompt_manager.get_promtp()
            # We need to generate a new schema with the updated column
            # descriptions and then swap it
            updated_schema = []
            updated_columns = []

            # Iterate over the columns in the table schema
            for column in table_schema:
                # Extract column information from the table profile
                column_info = self._extract_column_info_from_table_profile(table_profile, column.name)

                if self._client._client_options._use_human_comments:
                    human_comments = self._client._dataplex_ops.get_column_comment(table_fqn, column.name)
                
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

                if self._client._client_options._regenerate == True and self._client._dataplex_ops.check_if_column_should_be_regenerated(table_fqn, column.name) or self._client._client_options._regenerate == False:
                    column_description = self._client._utils.llm_inference(
                        column_description_prompt_expanded,
                        documentation_uri=documentation_uri,
                    )
                    if self._client._client_options._add_ai_warning:
                        column_description = f"{constants['OUTPUT_CLAUSES']['AI_WARNING']}{column_description}"

                    updated_schema.append(
                        self._get_updated_column(column, column_description)
                    )
                    if self._client._client_options._stage_for_review:
                        self._client._dataplex_ops.update_column_draft_description(table_fqn, column.name, column_description)
                    updated_columns.append(column)
                    logger.info(f"Generated column description: {column_description}.")
                   # if self._client._client_options._regenerate:
                   #     self._client._dataplex_ops.mark_column_as_regenerated(table_fqn, column.name)
                   #     logger.info(f"Marked column {column.name} as regenerated in Dataplex catalog.")
                    
                else:
                    updated_schema.append(column)
                    logger.info(f"Column {column.name} will not be updated.")

            if not self._client._client_options._stage_for_review:
                self._client._bigquery_ops.update_table_schema(table_fqn, updated_schema)
            
            if self._client._client_options._regenerate:
                for column in updated_columns:                    
                    self._client._dataplex_ops.mark_column_as_regenerated(table_fqn, column.name)
                    logger.info(f"Marked table {table_fqn} column {column.name} as regenerated")

        except Exception as e:
            logger.error(f"Update of column description table {table_fqn} failed.")
            raise e

    def regenerate_columns_descriptions(self, table_fqn, documentation_uri=None, human_comments=None):
        """Regenerate columns descriptions.

        Args:
            table_fqn: The fully qualified name of the table
            documentation_uri: Optional URI to documentation
            human_comments: Optional human comments to consider

        Returns:
            None.

        Raises:
            Exception: If there is an error regenerating descriptions
        """
        self._client._client_options._use_human_comments = True
        self._client._client_options._regenerate = True
        try:
            output = self.generate_columns_descriptions(table_fqn, documentation_uri, human_comments)
            return output
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _extract_column_info_from_table_profile(self, profile, column_name):
        """Extract profile information for a specific column from the table profile.
        
        Args:
            profile (list): The table profile information
            column_name (str): Name of the column to extract information for
            
        Returns:
            dict: Dictionary containing column profile information or None if column not found
        """
        try:
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
            logger.error(f"Error extracting column info: {str(e)}")
            return None

    def _get_updated_column(self, column, column_description):
        """Creates an updated column schema field with new description.

        Args:
            column (SchemaField): The original column schema field
            column_description (str): The new description to set

        Returns:
            SchemaField: Updated schema field with new description
        """
        try:
            if self._client._client_options._add_ai_warning and column.description is not None:
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