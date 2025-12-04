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
"""Dataplex Utils Metadata Wizard review operations
   2024 Google
"""
# Standard library imports
import logging
import toml
import pkgutil
import datetime
import uuid
import traceback

# Cloud imports
from google.cloud import dataplex_v1
from google.protobuf import field_mask_pb2, struct_pb2
import google.api_core.exceptions
from google.protobuf.json_format import MessageToDict

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])
logger.setLevel(logging.DEBUG)

class ReviewOperations:
    """Review-specific operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def build_search_query_for_review(self, dataset_fqn: str, search_query: str = None) -> str:
        """Build an effective query that always includes the dataset filter.
        
        This method ensures that the dataset_fqn is always included in the query,
        even when no search_query is provided. If a search_query is provided,
        it's combined with the dataset filter.
        
        Args:
            dataset_fqn (str): The fully qualified name of the dataset (project_id.dataset_id)
            search_query (str, optional): Additional search criteria to filter results
            
        Returns:
            str: The effective query string that includes the dataset filter
        """
        # Start with the base system filter
        base_query = f"""system=BIGQUERY and aspect:global.{constants['ASPECT_TEMPLATE']['name']}.is-accepted=false"""
        
        # Always include the dataset filter
        dataset_filter = f"parent:{dataset_fqn}"
        
        # Combine filters
        if search_query is not None:
            # If search_query already contains the dataset filter, don't duplicate it
            if f"parent:{dataset_fqn}" in search_query:
                return f"{base_query} AND {search_query}"
            else:
                return f"{base_query} AND {dataset_filter} AND {search_query}"
        else:
            return f"{base_query} AND {dataset_filter}"
        
    def build_search_query_for_regeneration(self, dataset_fqn: str, search_query: str = None) -> str:
        """Build an effective query that always includes the dataset filter.
        
        This method ensures that the dataset_fqn is always included in the query,
        even when no search_query is provided. If a search_query is provided,
        it's combined with the dataset filter.
        
        Args:
            dataset_fqn (str): The fully qualified name of the dataset (project_id.dataset_id)
            search_query (str, optional): Additional search criteria to filter results
            
        Returns:
            str: The effective query string that includes the dataset filter
        """
        # Start with the base system filter
        base_query = f"""system=BIGQUERY and aspect:global.{constants['ASPECT_TEMPLATE']['name']}.to-be-regenerated=true"""
        
        # Always include the dataset filter
        dataset_filter = f"parent:{dataset_fqn}"
        
        # Combine filters
        if search_query is not None:
            # If search_query already contains the dataset filter, don't duplicate it
            if f"parent:{dataset_fqn}" in search_query:
                return f"{base_query} AND {search_query}"
            else:
                return f"{base_query} AND {dataset_filter} AND {search_query}"
        else:
            return f"{base_query} AND {dataset_filter}"

    def get_review_items_for_dataset(self, dataset_fqn: str, search_query: str=None, page_size: int = 100, page_token: str = None) -> dict:
        """Get review items for a dataset based on search criteria.

        Args:
            search_query (str): Optional search query to filter tables
            page_size (int): Number of items per page
            page_token (str): Token for pagination

        Returns:
            dict: Dictionary containing review items and pagination info
        """
        try:
            logger.info(f"Processing search query: {search_query}")
            
            review_items = []
            result_count = 0
            
            try:
                client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
                name = f"projects/{self._client._project_id}/locations/global"

                logger.info(f"Building search query for dataset: {dataset_fqn} and search query: {search_query}")
                query = self.build_search_query_for_review(dataset_fqn, search_query)
                logger.info(f"Built search request - name: {name}, query: {query}")
                
                request = dataplex_v1.SearchEntriesRequest(
                    name=name,
                    query=query,
                    page_size=page_size,
                    page_token=page_token
                )
        
                response = client.search_entries(request=request)
                logger.info("Got search response")
                
                for result in response:
                    if not hasattr(result, 'dataplex_entry'):
                        logger.info("Result has no dataplex_entry, skipping")
                        continue
                        
                    entry = result.dataplex_entry
                    if not entry.fully_qualified_name.startswith("bigquery:"):
                        logger.info(f"Entry {entry.fully_qualified_name} is not a BigQuery table, skipping")
                        continue
                        
                    table_fqn = entry.fully_qualified_name.replace("bigquery:", "")
                    current_description = ""
                    
                    if hasattr(entry, 'entry_source') and hasattr(entry.entry_source, 'description'):
                        current_description = entry.entry_source.description
                        logger.info(f"Found description for {table_fqn}: {current_description}")
                    
                    review_item = {
                        "id": f"{table_fqn}#table",
                        "type": "table",
                        "name": table_fqn,
                        "currentDescription": current_description,
                        "draftDescription": "",  # Empty for list view
                        "isHtml": False,
                        "status": "current",
                        "lastModified": entry.update_time.isoformat() if hasattr(entry, 'update_time') else datetime.datetime.now().isoformat(),
                        "comments": [],  # Empty for list view
                        "markedForRegeneration": False  # Default for list view
                    }
                    review_items.append(review_item)
                    result_count += 1
                    logger.info(f"Added review item for table {table_fqn}")
                
                    # Check for column-level metadata tags
                    for aspect_key, aspect in entry.aspects.items():
                        if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path.startswith("Schema."):
                            # Extract column name from path
                            column_name = aspect.path.replace("Schema.", "")
                            logger.info(f"Found column metadata for {column_name}")
                            
                            # Get column current description from BigQuery
                            flat_schema, schema = self._client._bigquery_ops.get_table_schema(table_fqn)
                            column = next((f for f in schema if f.name == column_name), None)
                            current_description = column.description if column else ""
                            
                            column_review_item = {
                                "id": f"{table_fqn}#column#{column_name}",
                                "type": "column",
                                "name": f"{table_fqn}.{column_name}",
                                "currentDescription": current_description,
                                "draftDescription": "",  # Empty for list view
                                "isHtml": False,
                                "status": "current",
                                "lastModified": entry.update_time.isoformat() if hasattr(entry, 'update_time') else datetime.datetime.now().isoformat(),
                                "comments": [],  # Empty for list view
                                "markedForRegeneration": False  # Default for list view
                            }
                            review_items.append(column_review_item)
                            result_count += 1
                            logger.info(f"Added review item for column {column_name}")
                
                response_data = {
                    "items": review_items,
                    "nextPageToken": response.next_page_token if hasattr(response, 'next_page_token') else None,
                    "totalCount": response.total_size if hasattr(response, 'total_size') else result_count
                }
                
                return {"data": response_data}
                
            except Exception as e:
                logger.error(f"Error during search_entries call: {str(e)}")
                raise
            
        except Exception as e:
            logger.error(f"Error getting review items for search query '{search_query}': {str(e)}")
            raise

    def get_review_item_details(self, table_fqn: str, column_name: str = None) -> dict:
        """Get detailed information about a specific review item.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str, optional): The name of the column

        Returns:
            dict: Detailed information about the review item
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)
            
            # Extract tags from entry
            tags = {}
            if hasattr(entry, 'labels'):
                tags.update(entry.labels)
            if hasattr(entry, 'tags'):
                tags.update(entry.tags)
            
            # Get table schema first as we'll need it for both table and column details
            flat_schema, schema = self._client._bigquery_ops.get_table_schema(table_fqn)
            if not schema:
                raise ValueError(f"Table {table_fqn} not found")

            # If column_name is specified, return only that column's details
            if column_name:
                column = next((f for f in schema if f.name == column_name), None)
                if not column:
                    raise ValueError(f"Column {column_name} not found in table {table_fqn}")
                
                return self._get_column_details(entry, table_fqn, column, tags)
            
            # Handle table details
            current_description = self._client._bigquery_ops.get_table_description(table_fqn)
            draft_description = None
            metadata = {
                'certified': False,
                'user_who_certified': '',
                'generation_date': datetime.datetime.now().isoformat(),
                'to_be_regenerated': False,
                'external_document_uri': ''
            }

            # Get table-level aspect data
            for aspect_key, aspect in entry.aspects.items():
                if (aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") 
                    and aspect.path == "" 
                    and hasattr(aspect, 'data')
                    and aspect.data):
                    
                    aspect_data = aspect.data
                    if hasattr(aspect_data, 'get') or isinstance(aspect_data, dict):
                        if aspect_data.get("contents"):
                            draft_description = aspect_data["contents"]
                        metadata.update({
                            'certified': aspect_data.get('certified', False),
                            'user_who_certified': aspect_data.get('user-who-certified', ''),
                            'generation_date': aspect_data.get('generation-date', datetime.datetime.now().isoformat()),
                            'to_be_regenerated': aspect_data.get('to-be-regenerated', False),
                            'external_document_uri': aspect_data.get('external-document-uri', '')
                        })
                        if aspect_data.get('tags'):
                            tags.update(aspect_data['tags'])

            # Get comments and negative examples
            comments = self.get_comments_to_table_draft_description(table_fqn) or []
            negative_examples = self.get_negative_examples_to_table_draft_description(table_fqn) or []
            
            # Combine comments and negative examples
            all_comments = [c for c in comments if isinstance(c, str)]
            all_comments.extend([e for e in negative_examples if isinstance(e, str)])

            # Process all columns and include those with metadata
            columns_with_metadata = []
            
            for column in schema:
                # Check if column has any aspects
                has_aspects = any(
                    aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column.name}""")
                    for aspect_key in entry.aspects.keys()
                )
                
                if has_aspects:
                    column_details = self._get_column_details(entry, table_fqn, column, dict(tags))
                    if column_details:
                        columns_with_metadata.append(column_details)

            return {
                "type": "table",
                "id": f"{table_fqn}#table",
                "name": table_fqn,
                "currentDescription": current_description or "",
                "draftDescription": draft_description or "",
                "isHtml": False,
                "status": "draft" if draft_description else "current",
                "lastModified": metadata.get('generation_date', datetime.datetime.now().isoformat()),
                "comments": all_comments,
                "markedForRegeneration": metadata.get('to_be_regenerated', False),
                "metadata": metadata,
                "tags": tags,
                "columns": columns_with_metadata
            }

        except Exception as e:
            logger.error(f"Error getting review item details for table {table_fqn} column {column_name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    def _get_column_details(self, entry, table_fqn: str, column, parent_tags: dict) -> dict:
        """Get column details from entry and column information.
        
        Args:
            entry: The Dataplex entry containing aspects
            table_fqn (str): The fully qualified name of the table
            column: The column schema object
            parent_tags (dict): Tags inherited from the parent table
            
        Returns:
            dict: Column details including metadata and descriptions
        """
        try:
            logger.debug(f"Getting column details for {column.name}")
            current_description = column.description or ""
            draft_description = None
            metadata = {
                'certified': False,
                'user_who_certified': '',
                'generation_date': datetime.datetime.now().isoformat(),
                'to_be_regenerated': False,
                'external_document_uri': ''
            }
            column_tags = dict(parent_tags)
            comments = []

            # Find the draft description in the custom aspect for the specific column
            for aspect_key, aspect in entry.aspects.items():
                if (aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column.name}""") 
                    and aspect.path == f"Schema.{column.name}"
                    and hasattr(aspect, 'data')):
                    
                    aspect_data = aspect.data
                    
                    # Handle both dict and MapComposite types
                    if hasattr(aspect_data, 'get') or isinstance(aspect_data, dict):
                        # Extract draft description
                        if aspect_data.get("contents"):
                            draft_description = aspect_data["contents"]
                            
                        # Extract metadata
                        metadata.update({
                            'certified': aspect_data.get('certified', False),
                            'user_who_certified': aspect_data.get('user-who-certified', ''),
                            'generation_date': aspect_data.get('generation-date', datetime.datetime.now().isoformat()),
                            'to_be_regenerated': aspect_data.get('to-be-regenerated', False),
                            'external_document_uri': aspect_data.get('external-document-uri', '')
                        })
                        
                        # Extract comments
                        if aspect_data.get('human-comments'):
                            comments.extend([
                                comment for comment in aspect_data['human-comments']
                                if isinstance(comment, str)
                            ])
                        
                        # Extract tags
                        if aspect_data.get('tags'):
                            column_tags.update(aspect_data['tags'])

            result = {
                "type": "column",
                "id": f"{table_fqn}#column#{column.name}",
                "name": f"{table_fqn}.{column.name}",
                "currentDescription": current_description,
                "draftDescription": draft_description or "",
                "isHtml": False,
                "status": "draft" if draft_description else "current",
                "lastModified": metadata.get('generation_date', datetime.datetime.now().isoformat()),
                "comments": comments,
                "markedForRegeneration": metadata.get('to_be_regenerated', False),
                "metadata": metadata,
                "tags": column_tags
            }
            return result
            
        except Exception as e:
            logger.error(f"Error getting column details for {column.name}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def get_comments_to_table_draft_description(self, table_fqn):
        """Get all comments for a table's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            list: List of comments associated with the draft description
        """
        try:
            logger.info(f"=== START: get_comments_to_table_draft_description for {table_fqn} ===")
            
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    if "human-comments" in aspect.data:
                        raw_comments = aspect.data["human-comments"]
                        validated_comments = []
                        for comment in raw_comments:
                            if isinstance(comment, str):
                                validated_comments.append(comment)
                        return validated_comments

            return []

        except Exception as e:
            logger.error(f"Error getting comments for table {table_fqn}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []

    def get_negative_examples_to_table_draft_description(self, table_fqn):
        """Get all negative examples for a table's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            list: List of negative examples associated with the draft description
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    if "negative-examples" in aspect.data:
                        return aspect.data["negative-examples"]
            
            return []

        except Exception as e:
            logger.error(f"Error getting negative examples for table {table_fqn}: {str(e)}")
            return []

    def add_comment_to_table_draft_description(self, table_fqn, comment):
        """Add a comment to a table's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table
            comment (str): The comment to add

        Returns:
            bool: True if successful
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type
            
            try:
                request = dataplex_v1.GetEntryRequest(
                    name=entry_name,
                    view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                    aspect_types=[aspect_type]
                )
                entry = client.get_entry(request=request)
                
                aspect_found = False
                for i in entry.aspects:
                    if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path == "":
                        aspect_found = True
                        new_aspect.data = entry.aspects[i].data
                        existing_comments = list(new_aspect.data.get("human-comments", []))
                        existing_comments.append(comment)
                        new_aspect.data["human-comments"] = existing_comments
                        break
                
                if not aspect_found:
                    aspect_data = {
                        "human-comments": [comment]
                    }
                    data_struct = struct_pb2.Struct()
                    data_struct.update(aspect_data)
                    new_aspect.data = data_struct
                
            except google.api_core.exceptions.NotFound:
                aspect_data = {
                    "human-comments": [comment]
                }
                data_struct = struct_pb2.Struct()
                data_struct.update(aspect_data)
                new_aspect.data = data_struct
            
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect
            
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )
            
            response = client.update_entry(request=request)
            return True
            
        except Exception as e:
            logger.error(f"Error updating comments: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def add_comment_to_column_draft_description(self, table_fqn, column_name, comment):
        """Add a comment to a column's draft description.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column
            comment (str): The comment to add

        Returns:
            bool: True if successful
        """
        try:
            logger.info(f"=== START: add_comment_to_column_draft_description for {table_fqn}.{column_name} ===")
            
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type
            new_aspect.path = f"Schema.{column_name}"

            try:
                request = dataplex_v1.GetEntryRequest(
                    name=entry_name,
                    view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                    aspect_types=[aspect_type]
                )
                entry = client.get_entry(request=request)
                
                found_aspect = False
                for i in entry.aspects:
                    if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                        found_aspect = True
                        new_aspect.data = entry.aspects[i].data
                        existing_comments = list(new_aspect.data.get("human-comments", []))
                        existing_comments.append(comment)
                        new_aspect.data["human-comments"] = existing_comments
                        break
                
                if not found_aspect:
                    aspect_data = {
                        "human-comments": [comment]
                    }
                    data_struct = struct_pb2.Struct()
                    data_struct.update(aspect_data)
                    new_aspect.data = data_struct

            except google.api_core.exceptions.NotFound:
                aspect_data = {
                    "human-comments": [comment]
                }
                data_struct = struct_pb2.Struct()
                data_struct.update(aspect_data)
                new_aspect.data = data_struct

            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )
            
            response = client.update_entry(request=request)
            logger.info("Successfully updated entry")
            return True

        except Exception as e:
            logger.error(f"Error adding comment to column {column_name} in table {table_fqn}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return False

    def _get_table_draft_description(self, table_fqn: str) -> str:
        """Get the draft description for a table.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            str: Draft description or None if not found
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    return aspect.data["contents"]

            return None
        except Exception as e:
            logger.error(f"Error getting draft description for table {table_fqn}: {str(e)}")
            return None

    def _get_column_draft_description(self, table_fqn: str, column_name: str) -> str:
        """Get the draft description for a column.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column

        Returns:
            str: Draft description or None if not found
        """
        try:
            logger.debug(f"Getting draft description for column {column_name}")
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]
            entry_name = f"projects/{project_id}/locations/{self._client._dataplex_ops._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,  # IMPORTANT: Must remain CUSTOM - do not change to ALL or FULL as it breaks aspect filtering
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)
            
            logger.debug(f"Available aspects: {list(entry.aspects.keys())}")
            
            # Check for column aspects with different patterns
            aspect_patterns = [
                f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""",
                f"""global.{constants["ASPECT_TEMPLATE"]["name"]}.Schema.{column_name}""",
                f"""Schema.{column_name}"""
            ]
            
            for pattern in aspect_patterns:
                for aspect_key, aspect in entry.aspects.items():
                    if pattern in aspect_key and hasattr(aspect, 'data'):
                        logger.debug(f"Found aspect for column {column_name}: {aspect_key}")
                        logger.debug(f"Aspect data: {aspect.data}")
                        if aspect.data and isinstance(aspect.data, dict) and "contents" in aspect.data:
                            return aspect.data["contents"]

            logger.debug(f"No draft description found for column {column_name}")
            return None
        except Exception as e:
            logger.error(f"Error getting draft description for column {column_name} in table {table_fqn}: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def reject_review_item(self, item_id: str) -> dict:
        """Reject a review item by ID.
        
        Args:
            item_id (str): The ID of the review item to reject
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Rejecting review item with ID: {item_id}")
            
            # Parse the item_id to determine if it's a table or column
            parts = item_id.split(":")
            if len(parts) < 2:
                raise ValueError(f"Invalid item ID format: {item_id}")
                
            item_type = parts[0]
            
            if item_type == "table":
                # Format: table:project.dataset.table
                table_fqn = ":".join(parts[1:])
                # Mark the table for regeneration
                self._client._dataplex_ops.mark_table_for_regeneration(table_fqn)
                return {"success": True, "message": f"Table {table_fqn} marked for regeneration"}
            elif item_type == "column":
                # Format: column:project.dataset.table:column_name
                table_fqn = parts[1]
                column_name = parts[2]
                # Mark the column for regeneration
                self._client._dataplex_ops.mark_column_for_regeneration(table_fqn, column_name)
                return {"success": True, "message": f"Column {column_name} in table {table_fqn} marked for regeneration"}
            else:
                raise ValueError(f"Unknown item type: {item_type}")
                
        except Exception as e:
            logger.error(f"Error rejecting review item: {str(e)}")
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}
            
    def edit_review_item(self, item_id: str, description: str) -> dict:
        """Edit a review item's description.
        
        Args:
            item_id (str): The ID of the review item to edit
            description (str): The new description
            
        Returns:
            dict: Result of the operation
        """
        try:
            logger.info(f"Editing review item with ID: {item_id}")
            
            # Parse the item_id to determine if it's a table or column
            parts = item_id.split(":")
            if len(parts) < 2:
                raise ValueError(f"Invalid item ID format: {item_id}")
                
            item_type = parts[0]
            
            if item_type == "table":
                # Format: table:project.dataset.table
                table_fqn = ":".join(parts[1:])
                # Update the table draft description
                client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
                
                # Get the entry
                entry = self._client._dataplex_ops._get_entry(table_fqn)
                if not entry:
                    raise ValueError(f"Entry not found for table: {table_fqn}")
                
                # Update the draft description in the entry's aspects
                aspects = entry.aspects
                if not aspects:
                    aspects = {}
                
                # Create or update the draft description aspect
                draft_aspect_name = constants["DATAPLEX"]["DRAFT_DESCRIPTION_ASPECT_TYPE"]
                if draft_aspect_name not in aspects:
                    aspects[draft_aspect_name] = struct_pb2.Struct()
                
                aspects[draft_aspect_name].update({"description": description})
                
                # Update the entry
                update_mask = field_mask_pb2.FieldMask(paths=["aspects"])
                request = dataplex_v1.UpdateEntryRequest(
                    entry=entry,
                    update_mask=update_mask
                )
                
                updated_entry = client.update_entry(request=request)
                return {"success": True, "message": f"Table {table_fqn} description updated"}
                
            elif item_type == "column":
                # Format: column:project.dataset.table:column_name
                table_fqn = parts[1]
                column_name = parts[2]
                
                # Update the column draft description
                client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
                
                # Get the entry
                entry = self._client._dataplex_ops._get_entry(table_fqn)
                if not entry:
                    raise ValueError(f"Entry not found for table: {table_fqn}")
                
                # Update the draft description in the entry's aspects
                aspects = entry.aspects
                if not aspects:
                    aspects = {}
                
                # Create or update the draft column descriptions aspect
                draft_aspect_name = constants["DATAPLEX"]["DRAFT_COLUMN_DESCRIPTIONS_ASPECT_TYPE"]
                if draft_aspect_name not in aspects:
                    aspects[draft_aspect_name] = struct_pb2.Struct()
                
                # Get existing column descriptions
                column_descriptions = {}
                if aspects[draft_aspect_name].fields:
                    column_descriptions = MessageToDict(aspects[draft_aspect_name])
                
                # Update the specific column description
                column_descriptions[column_name] = description
                
                # Update the aspect
                aspects[draft_aspect_name].update(column_descriptions)
                
                # Update the entry
                update_mask = field_mask_pb2.FieldMask(paths=["aspects"])
                request = dataplex_v1.UpdateEntryRequest(
                    entry=entry,
                    update_mask=update_mask
                )
                
                updated_entry = client.update_entry(request=request)
                return {"success": True, "message": f"Column {column_name} in table {table_fqn} description updated"}
            else:
                raise ValueError(f"Unknown item type: {item_type}")
                
        except Exception as e:
            logger.error(f"Error editing review item: {str(e)}")
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)} 