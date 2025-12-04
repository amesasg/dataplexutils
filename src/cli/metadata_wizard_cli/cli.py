#!/usr/bin/env python
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
"""CLI interface to metadata wizard API
   2024 Google
"""

# Standard library imports
import argparse
import logging

# Third-party imports
import requests

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def _call_api(
    service,
    scope,
    use_lineage_tables,
    use_lineage_processes,
    use_profile,
    use_data_quality,
    use_ext_documents,
    persist_to_dataplex_catalog,
    stage_for_review,
    dataplex_project_id,
    llm_location,
    dataplex_location,
    documentation_uri,
    table_project_id,
    table_dataset_id,
    table_id,
    debug,
    documentation_csv_uri,
    strategy,
    top_values_in_description,
    description_handling="APPEND",
    description_prefix="",
):
    """Call the metadata wizard API with the provided parameters.

    Args:
        service (str): The API service endpoint
        scope (str): The scope of the operation (table, columns, dataset, dataset_columns)
        use_lineage_tables (bool): Whether to use lineage tables
        use_lineage_processes (bool): Whether to use lineage processes
        use_profile (bool): Whether to use profile information
        use_data_quality (bool): Whether to use data quality information
        use_ext_documents (bool): Whether to use external documents
        persist_to_dataplex_catalog (bool): Whether to persist to Dataplex catalog
        stage_for_review (bool): Whether to stage for review
        dataplex_project_id (str): The Dataplex project ID
        llm_location (str): The LLM location
        dataplex_location (str): The Dataplex location
        documentation_uri (str): The documentation URI
        table_project_id (str): The table project ID
        table_dataset_id (str): The table dataset ID
        table_id (str): The table ID
        debug (bool): Whether to use debug mode
        documentation_csv_uri (str): The documentation CSV URI
        strategy (str): The generation strategy
        top_values_in_description (bool): Whether to include top values in description
        description_handling (str): How to handle description updates
        description_prefix (str): Prefix for generated descriptions
    """
    API_URL = f"https://{service}"
    API_URL_DEBUG = "http://localhost:8000"
    METADATA_TABLE_SCOPE_ROUTE = "/generate_table_description"
    METADATA_COLUMNS_SCOPE_ROUTE = "/generate_columns_descriptions"
    METADATA_DATASET_SCOPE_ROUTE = "/generate_dataset_tables_descriptions"
    METADATA_DATASET_COLUMNS_SCOPE_ROUTE = "/generate_dataset_tables_columns_descriptions"

    if debug:
        API_URL = API_URL_DEBUG

    if scope == "table":
        url = API_URL + METADATA_TABLE_SCOPE_ROUTE
    elif scope == "columns":
        url = API_URL + METADATA_COLUMNS_SCOPE_ROUTE
    elif scope == "dataset":
        url = API_URL + METADATA_DATASET_SCOPE_ROUTE
    elif scope == "dataset_columns":
        url = API_URL + METADATA_DATASET_COLUMNS_SCOPE_ROUTE
    else:
        raise ValueError(f"Invalid scope: {scope}")

    params = {
        "client_options_settings": {
            "use_lineage_tables": use_lineage_tables,
            "use_lineage_processes": use_lineage_processes,
            "use_profile": use_profile,
            "use_data_quality": use_data_quality,
            "use_ext_documents": use_ext_documents,
            "persist_to_dataplex_catalog": persist_to_dataplex_catalog,
            "stage_for_review": stage_for_review,
            "top_values_in_description": top_values_in_description,
            "description_handling": description_handling,
            "description_prefix": description_prefix
        },
        "client_settings": {
            "project_id": dataplex_project_id,
            "llm_location": llm_location,
            "dataplex_location": dataplex_location
        },
        "table_settings": {
            "project_id": table_project_id,
            "dataset_id": table_dataset_id,
            "table_id": table_id,
            "documentation_uri": documentation_uri
        },
        "dataset_settings": {
            "project_id": table_project_id,
            "dataset_id": table_dataset_id,
            "documentation_csv_uri": documentation_csv_uri,
            "strategy": strategy
        },
    }

    try:
        logger.debug("Sending request with params: %s", params)
        response = requests.post(url, json=params)
        response.raise_for_status()
        result = response.json()
        logger.debug("Received response: %s", result)
        print(result)
    except requests.exceptions.RequestException as e:
        logger.error("Error calling API: %s", e)
        print(f"Error calling API: {e}")
    except requests.exceptions.JSONDecodeError as e:
        logger.error("Error decoding JSON response: %s", e)
        print(f"Error decoding JSON response: {e}")


def _get_input_arguments():
    """Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed command line arguments
    """
    parser = argparse.ArgumentParser(description="Call Metadata Wizard API.")
    
    # Required arguments
    parser.add_argument("--service", dest="service", required=True, type=str,
                       help="The API service endpoint")
    parser.add_argument("--scope", dest="scope", required=True, type=str,
                       help="The scope of the operation (table, columns, dataset, dataset_columns)")
    parser.add_argument("--dataplex_project_id", dest="dataplex_project_id", required=True, type=str,
                       help="The Dataplex project ID")
    parser.add_argument("--llm_location", dest="llm_location", required=True, type=str,
                       help="The LLM location")
    parser.add_argument("--dataplex_location", dest="dataplex_location", required=True, type=str,
                       help="The Dataplex location")
    parser.add_argument("--table_project_id", dest="table_project_id", required=True, type=str,
                       help="The table project ID")
    parser.add_argument("--table_dataset_id", dest="table_dataset_id", required=True, type=str,
                       help="The table dataset ID")
    parser.add_argument("--table_id", dest="table_id", required=True, type=str,
                       help="The table ID")

    # Optional arguments with defaults
    parser.add_argument("--use_lineage_tables", dest="use_lineage_tables", required=False, default=False, type=bool,
                       help="Whether to use lineage tables")
    parser.add_argument("--use_lineage_processes", dest="use_lineage_processes", required=False, default=False, type=bool,
                       help="Whether to use lineage processes")
    parser.add_argument("--use_profile", dest="use_profile", required=False, default=False, type=bool,
                       help="Whether to use profile information")
    parser.add_argument("--use_data_quality", dest="use_data_quality", required=False, default=False, type=bool,
                       help="Whether to use data quality information")
    parser.add_argument("--use_ext_documents", dest="use_ext_documents", required=False, default=False, type=bool,
                       help="Whether to use external documents")
    parser.add_argument("--documentation_uri", dest="documentation_uri", required=False, default="", type=str,
                       help="The documentation URI")
    parser.add_argument("--debug", dest="debug", required=False, type=bool, default=False,
                       help="Whether to use debug mode")
    parser.add_argument("--strategy", dest="strategy", required=False, type=str, default="NAIVE",
                       help="The generation strategy")
    parser.add_argument("--documentation_csv_uri", dest="documentation_csv_uri", required=False, type=str, default="",
                       help="The documentation CSV URI")
    parser.add_argument("--persist_to_dataplex_catalog", dest="persist_to_dataplex_catalog", required=False, type=bool, default=False,
                       help="Whether to persist to Dataplex catalog")
    parser.add_argument("--stage_for_review", dest="stage_for_review", required=False, type=bool, default=False,
                       help="Whether to stage for review")
    parser.add_argument("--top_values_in_description", dest="top_values_in_description", required=False, type=bool, default=True,
                       help="Whether to include top values in description")
    parser.add_argument("--description_handling", dest="description_handling", required=False, type=str, default="APPEND",
                       help="How to handle description updates")
    parser.add_argument("--description_prefix", dest="description_prefix", required=False, type=str, default="",
                       help="Prefix for generated descriptions")

    return parser.parse_args()


def main():
    """Main entry point for the CLI."""
    args = _get_input_arguments()
    _call_api(
        service=args.service,
        scope=args.scope,
        use_lineage_tables=args.use_lineage_tables,
        use_lineage_processes=args.use_lineage_processes,
        use_profile=args.use_profile,
        use_data_quality=args.use_data_quality,
        use_ext_documents=args.use_ext_documents,
        persist_to_dataplex_catalog=args.persist_to_dataplex_catalog,
        stage_for_review=args.stage_for_review,
        dataplex_project_id=args.dataplex_project_id,
        llm_location=args.llm_location,
        dataplex_location=args.dataplex_location,
        documentation_uri=args.documentation_uri,
        table_project_id=args.table_project_id,
        table_dataset_id=args.table_dataset_id,
        table_id=args.table_id,
        debug=args.debug,
        documentation_csv_uri=args.documentation_csv_uri,
        strategy=args.strategy,
        top_values_in_description=args.top_values_in_description,
        description_handling=args.description_handling,
        description_prefix=args.description_prefix,
    )


if __name__ == "__main__":
    main()
