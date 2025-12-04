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

class DataplexOperations:
    """Dataplex-specific operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def _check_if_exists_aspect_type(self, aspect_type_id: str):
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
        client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

        # Initialize request argument(s)
        request = dataplex_v1.GetAspectTypeRequest(
            name=f"projects/{self._client._project_id}/locations/global/aspectTypes/{aspect_type_id}"
        )
        
        # Make the request
        try:
            client.get_aspect_type(request=request)
            return True
        except google.api_core.exceptions.NotFound:
            return False

    def _create_aspect_type(self, aspect_type_id: str):
        """Creates a new aspect type in Dataplex catalog.

        Args:
            aspect_type_id (str): The ID to use for the new aspect type

        Raises:
            Exception: If there is an error creating the aspect type
        """
        # Create a client
        client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

        # Initialize request argument(s)
        aspect_type = dataplex_v1.AspectType()
        full_metadata_template = {
            "type_": constants["ASPECT_TEMPLATE"]["type_"],
            "name": constants["ASPECT_TEMPLATE"]["name"],
            "record_fields": constants["record_fields"]
        }
        metadata_template = dataplex_v1.AspectType.MetadataTemplate(full_metadata_template)

        logger.info("Will deploy following template:" + str(metadata_template))
        
        aspect_type.metadata_template = metadata_template
        aspect_type.display_name = constants["ASPECT_TEMPLATE"]["display_name"]

        request = dataplex_v1.CreateAspectTypeRequest(
            parent=f"projects/{self._client._project_id}/locations/global",
            aspect_type_id=aspect_type_id,
            aspect_type=aspect_type,
        )

        # Make the request
        try:
            operation = client.create_aspect_type(request=request)
        except Exception as e:
            logger.error(f"Failed to create aspect type: {e}")
            raise e

    def _attach_aspect_from_json(self,aspect_contract,table_fqn):
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            # Create entry name
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            #for aspect_contract in json_fiels['contract_terms']:
                 # Create entry name
            new_aspect_content = {}
            for field in aspect_contract['fields']:
                    name = field.get("name")
                    value = field.get("value")
                    if name and value:
                        new_aspect_content[name] = value
            aspect_name = f"""{self._client._project_id}.global.{aspect_contract["aspect_name"]}"""
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{aspect_contract["aspect_name"]}"""
                #initalise the aspect values            
            data_struct = struct_pb2.Struct()
            data_struct.update(new_aspect_content)
            new_aspect.data = data_struct
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name 
            new_entry.aspects[aspect_name] = new_aspect


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
                    logger.info(f"Aspect created: {response.name}")
                    return True
            except Exception as e:
                    logger.error(f"Failed to create aspect: {e}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Failed to create aspect type: {e}")
            raise e

    def _create_aspect_type_from_json(self,json_fiels):
        """Creates a new aspect type in Dataplex catalog.

        Args:
            aspect_type_id (str): The ID to use for the new aspect type

        Raises:
            Exception: If there is an error creating the aspect type
        """
        # Create a client
        client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
        aspect_fields = []

        # Initialize request argument(s)
        i=1
        for field in json_fiels['fields']:
            print(field)
            aspect_fields.append(
                            dataplex_v1.AspectType.MetadataTemplate
                            (
                               type_ = field["type"],
                               name = field["name"],
                               index = i,
                            #    annotations=dataplex_v1.AspectType.MetadataTemplate.Annotations(
                            #    description="description of the field"),
                            #     constraints=dataplex_v1.AspectType.MetadataTemplate.Constraints(# Specifies if field will be required in Aspect Type.
                            #      required=True),
                            )
            )
            i =i+1

        template_name = json_fiels["aspect_name"]

        aspect_type = dataplex_v1.AspectType(
            description="description of the aspect type",
            metadata_template=dataplex_v1.AspectType.MetadataTemplate(
            name=template_name,
            type_="record",
                # Aspect Type fields, that themselves are Metadata Templates.
            record_fields=aspect_fields),
        )
        parent=f"projects/{self._client._project_id}/locations/global",
        print(parent)
        print(template_name)
        try:
            create_operation = client.create_aspect_type(
                    parent=f"projects/{self._client._project_id}/locations/global",
                    aspect_type=aspect_type, 
                aspect_type_id=template_name
            )
        except Exception as e:
            logger.error(f"Failed to create aspect type: {e}")
            raise e

    def update_table_dataplex_description(self, table_fqn, description):
        """Updates the table description in Dataplex.

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
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/dataplex-types/locations/global/aspectTypes/overview"""
            aspect_types = [aspect_type]
            old_overview = None
            aspect_content = None

            try:
                request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
                current_entry = client.get_entry(request=request)
                for i in current_entry.aspects:
                    if i.endswith(f"""global.overview""") and current_entry.aspects[i].path == "":
                        logger.info(f"Reading existing aspect {i} of table {table_fqn}")
                        old_overview = dict(current_entry.aspects[i].data)
                        logger.info(f"""old_overview: {old_overview["content"][1:50]}...""")
            except Exception as e:
                logger.error(f"Exception: {e}.")
                raise e

            # Create the aspect
            aspect = dataplex_v1.Aspect()
            aspect.aspect_type = aspect_type
            aspect_content = {}

            if old_overview is not None:
                old_description = old_overview["content"]
                logger.debug(f"""old_description: {old_description[0:50]}...""")
                logger.debug(f"""new description: {description[0:50]}...""")
                logger.debug(f"""description_handling: {self._client._client_options._description_handling}""")
                combined_description = self._client._utils.combine_description(
                    old_description, 
                    description, 
                    self._client._client_options._description_handling
                )
                logger.debug(f"""FINAL combined_description: {combined_description}""")
                aspect_content["content"] = combined_description
            else:
                aspect_content = {"content": description}

            logger.info(f"""aspect_content: {aspect_content}...""")
            # Convert aspect_content to a Struct
            data_struct = struct_pb2.Struct()
            data_struct.update(aspect_content)
            aspect.data = data_struct

            overview_path = f"dataplex-types.global.overview"

            logger.info(f"project_id: {project_id}, dataset_id: {dataset_id}, table_id: {table_id}")
            entry = dataplex_v1.Entry()
            entry.name = entry_name
            entry.aspects[overview_path] = aspect

            # Initialize request argument(s)
            request = dataplex_v1.UpdateEntryRequest(
                entry=entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
            )

            # Make the request
            try:
                response = client.update_entry(request=request)
                logger.info(f"Aspect created: {response.name}")
                return True
            except Exception as e:
                logger.error(f"Failed to create aspect: {e}")
                return False

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def update_table_draft_description(self, table_fqn, description, metadata=None):
        """Updates the draft description for a table in Dataplex.

        Args:
            table_fqn (str): The fully qualified name of the table
            description (str): The new draft description
            metadata (dict, optional): Additional metadata to include

        Returns:
            bool: True if successful

        Raises:
            Exception: If there is an error updating the draft description
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            # Create new aspect content
            new_aspect_content = {
                #"certified": "false",
                #"user-who-certified": "",
                "contents": description,
                "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to-be-regenerated": "false",
                #"human-comments": [],
                #"negative-examples": []
                "is-accepted": "false"
            }

            # If additional metadata was provided, update the aspect content
            if metadata:
                new_aspect_content.update(metadata)

            logger.info(f"aspect_content: {new_aspect_content}")
            
            # Create the aspect
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [new_aspect.aspect_type]

            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry = dataplex_v1.Entry()
            entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Check if the aspect already exists
            try:
                get_request = dataplex_v1.GetEntryRequest(name=entry.name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
                entry = client.get_entry(request=get_request)
            except Exception as e:
                logger.error(f"Exception: {e}.")
                raise e

            data_struct = struct_pb2.Struct()
            data_struct.update(new_aspect_content)
            new_aspect.data = data_struct
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path == "":
                    logger.info(f"Updating aspect {i} with old_values")
                    new_aspect.data = entry.aspects[i].data
                    update_data = {
                        "contents": description,
                        "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to-be-regenerated": "false",
                        "is-accepted": "false"
                    }
                    if metadata:
                        update_data.update(metadata)
                    new_aspect.data.update(update_data)
                    logger.info(f"entry.aspects[aspect_name].data: {entry.aspects[i].data}")
                    logger.info(f"new_aspect.data: {new_aspect.data}")

            new_entry = dataplex_v1.Entry()
            new_entry.name = entry.name
            new_entry.aspects[aspect_name] = new_aspect

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
                logger.info(f"Aspect created: {response.name}")
                return True
            except Exception as e:
                logger.error(f"Failed to create aspect: {e}")
                return False

            return True

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def accept_table_draft_description(self, table_fqn):
        """Accepts the draft description for a table, promoting it to the actual table description.
        
        Args:
            table_fqn (str): The fully qualified name of the table
            
        Returns:
            bool: True if successful
            
        Raises:
            Exception: If there is an error accepting the draft description
        """
        try:
            # Create a client
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            
            # Get project and dataset IDs
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            # Set up aspect types and entry name
            aspect_types = [
                f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            ]
            
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            
            # Get the entry with the draft aspect
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            
            entry = client.get_entry(request=request)
            overview = None
            
            # Find the draft description in the custom aspect
            for aspect_key, aspect in entry.aspects.items():
                logger.info(f"Processing aspect: {aspect_key}")
                if aspect.aspect_type.endswith(f"""aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    if "contents" in aspect.data:
                        overview = aspect.data["contents"]
                        logger.info(f"Found draft description: {overview[:50]}...")
                        break
            

            if overview:
                # Update both BigQuery and Dataplex descriptions
                logger.info(f"Updating Dataplex description: {overview}")
                success_dataplex = self.update_table_dataplex_description(table_fqn, overview)
                logger.info(f"Updating BigQuery description: {overview}")
                success_bigquery = self._client._bigquery_ops.update_table_description(table_fqn, overview)
                
                if success_dataplex and success_bigquery:
                    logger.info(f"Successfully updated description for table {table_fqn}")
                    return True
                else:
                    logger.warning(f"Partial update for table {table_fqn}: Dataplex={success_dataplex}, BigQuery={success_bigquery}")
                    return False
            else:
                logger.warning(f"No draft description found for table {table_fqn}")
                return False
                
        except Exception as e:
            logger.error(f"Exception in accept_table_draft_description: {e}")
            return False

    def update_column_draft_description(self, table_fqn, column_name, description):
        """Updates the draft description for a column in Dataplex.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column
            description (str): The new draft description

        Returns:
            bool: True if successful

        Raises:
            Exception: If there is an error updating the draft description
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            # Create new aspect content
            new_aspect_content = {
                #"certified": "false",
                #"user-who-certified": "",
                "contents": description,
                "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to-be-regenerated": "false",
                #"human-comments": [],
                #"negative-examples": [],
                #"external-document-uri": "",
                "is-accepted": "false"
            }

            logger.info(f"aspect_content: {new_aspect_content}")

            # Create the aspect
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            aspect_types = [new_aspect.aspect_type]

            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry = dataplex_v1.Entry()
            entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            # Check if the aspect already exists
            try:
                get_request = dataplex_v1.GetEntryRequest(name=entry.name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
                entry = client.get_entry(request=get_request)
            except Exception as e:
                logger.error(f"Exception: {e}.")
                raise e

            data_struct = struct_pb2.Struct()
            data_struct.update(new_aspect_content)
            new_aspect.data = data_struct

            for i in entry.aspects:
                logger.info(f"""i: {i} path: "{entry.aspects[i].path}" """)
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                    logger.info(f"Updating aspect {i} with new values")
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data.update({
                        "contents": description,
                        "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to-be-regenerated": "false",
                        "is-accepted": "false"
                    })

            new_entry = dataplex_v1.Entry()
            new_entry.name = entry.name
            new_entry.aspects[aspect_name] = new_aspect

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
                logger.info(f"Aspect created: {response.name}")
                return True
            except Exception as e:
                logger.error(f"Failed to create aspect: {e}")
                return False

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def check_if_table_should_be_regenerated(self, table_fqn):
        """Checks if a table should be regenerated.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            bool: True if the table should be regenerated
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry = dataplex_v1.Entry()
            entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_types = [f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]

            try:
                get_request = dataplex_v1.GetEntryRequest(name=entry.name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
                entry = client.get_entry(request=get_request)
            except Exception as e:
                logger.error(f"Exception: {e}.")
                raise e

            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path == "":
                    data_dict = entry.aspects[i].data
                    return data_dict["to-be-regenerated"] == True

            return False

        except Exception as e:
            logger.error(f"Exception: {e}.")
            return False

    def check_if_column_should_be_regenerated(self, table_fqn, column_name):
        """Checks if a column should be regenerated.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column

        Returns:
            bool: True if the column should be regenerated
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry = dataplex_v1.Entry()
            entry.name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_types = [f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]

            try:
                get_request = dataplex_v1.GetEntryRequest(name=entry.name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
                entry = client.get_entry(request=get_request)
            except Exception as e:
                logger.error(f"Exception: {e}.")
                raise e

            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                    data_dict = entry.aspects[i].data
                    return data_dict["to-be-regenerated"] == True

            return False

        except Exception as e:
            logger.error(f"Exception: {e}.")
            return False

    def get_column_comment(self, table_fqn, column_name, comment_number=None):
        """Gets comments for a column.

        Args:
            table_fqn (str): The fully qualified name of the table
            column_name (str): The name of the column
            comment_number (int, optional): Specific comment number to retrieve

        Returns:
            list: List of comments or specific comment if comment_number provided
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]

            request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
            entry = client.get_entry(request=request)

            comments = []
            for aspect in entry.aspects:
                if aspect.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[aspect].path == f"Schema.{column_name}":
                    if "human-comments" in entry.aspects[aspect].data:
                        if comment_number is None:
                            comments.extend(entry.aspects[aspect].data["human-comments"])
                        else:
                            comments.append(entry.aspects[aspect].data["human-comments"][comment_number])

            return comments

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def get_table_comment(self, table_fqn, comment_number=None):
        """Gets comments for a table.

        Args:
            table_fqn (str): The fully qualified name of the table
            comment_number (int, optional): Specific comment number to retrieve

        Returns:
            list: List of comments or specific comment if comment_number provided
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]

            request = dataplex_v1.GetEntryRequest(name=entry_name, view=dataplex_v1.EntryView.CUSTOM, aspect_types=aspect_types)
            entry = client.get_entry(request=request)

            comments = []
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == "":
                    if "human-comments" in aspect.data:
                        if comment_number is None:
                            comments.extend(aspect.data["human-comments"])
                        else:
                            comments.append(aspect.data["human-comments"][comment_number])

            return comments

        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def _get_dataset_location(self, table_fqn):
        """Gets the location of a dataset.

        Args:
            table_fqn (str): The fully qualified name of the table

        Returns:
            str: The dataset location
        """
        try:
            project_id, dataset_id, _ = self._client._utils.split_table_fqn(table_fqn)
            return str(self._client._cloud_clients[constants["CLIENTS"]["BIGQUERY"]].get_dataset(
                f"{project_id}.{dataset_id}"
            ).location).lower()
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def accept_column_draft_description(self, table_fqn, column_name):
        """Move description from draft aspect to dataplex Overview and BQ for a specific column.

        Args:
            table_fqn (str): The fully qualified name of the table (project.dataset.table)
            column_name (str): The name of the column to update

        Returns:
            bool: True if successful, False otherwise

        Raises:
            Exception: If there's an error accessing or updating the entry
        """
        try:
            # Create a client
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]

            aspect_types = [f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            overview = None
            
            entry = client.get_entry(request=request)
            
            for aspect_key, aspect in entry.aspects.items():
                logger.info(f"Processing aspect: {aspect_key}")
                logger.info(f"Aspect path: {aspect.path}")
                
                if aspect.aspect_type.endswith(f"""aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}""") and aspect.path == f"Schema.{column_name}":
                    if "contents" in aspect.data:
                        overview = aspect.data["contents"]
                        logger.info(f"Found draft description for column {column_name}: {overview[:50]}...")
                        break

            if overview:
                success = self._client._bigquery_ops.update_column_description(table_fqn, column_name, overview)
                if success:
                    logger.info(f"Successfully updated description for column {column_name} in table {table_fqn}")
                    return True
                else:
                    logger.warning(f"Failed to update description for column {column_name} in table {table_fqn}")
                    return False
            else:
                logger.warning(f"No draft description found for column {column_name} in table {table_fqn}")
                return False
                
        except Exception as e:
            logger.error(f"Exception in accept_column_draft_description: {e}")
            return False

    def get_table_quality(self, use_data_quality, table_fqn):
        """Gets the quality information for a table from Dataplex.

        Args:
            use_data_quality (bool): Whether to use data quality information
            table_fqn (str): The fully qualified name of the table

        Returns:
            dict: Table quality information or None if not available/enabled
        """
        if not use_data_quality:
            return None
            
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = "projects/dataplex-types/locations/global/aspectTypes/data_quality"
            aspect_types = [aspect_type]

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith("global.data_quality") and aspect.path == "":
                    return dict(aspect.data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting table quality for {table_fqn}: {e}")
            return None

    def get_table_profile(self, use_profile, table_fqn):
        """Gets the profile information for a table from Dataplex.

        Args:
            use_profile (bool): Whether to use profile information
            table_fqn (str): The fully qualified name of the table

        Returns:
            dict: Table profile information or None if not available/enabled
        """
        if not use_profile:
            return None
            
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = "projects/dataplex-types/locations/global/aspectTypes/data_profile"
            aspect_types = [aspect_type]

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith("global.data_profile") and aspect.path == "":
                    return dict(aspect.data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting table profile for {table_fqn}: {e}")
            return None

    def get_table_sources_info(self, use_lineage_tables, table_fqn):
        """Gets source table information from Dataplex.

        Args:
            use_lineage_tables (bool): Whether to use lineage table information
            table_fqn (str): The fully qualified name of the table

        Returns:
            dict: Source table information or None if not available/enabled
        """
        if not use_lineage_tables:
            return None
            
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = "projects/dataplex-types/locations/global/aspectTypes/lineage"
            aspect_types = [aspect_type]

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith("global.lineage") and aspect.path == "":
                    return dict(aspect.data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting table sources info for {table_fqn}: {e}")
            return None

    def get_job_sources(self, use_lineage_processes, table_fqn):
        """Gets job source information from Dataplex.

        Args:
            use_lineage_processes (bool): Whether to use lineage process information
            table_fqn (str): The fully qualified name of the table

        Returns:
            dict: Job source information or None if not available/enabled
        """
        if not use_lineage_processes:
            return None
            
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)
            
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = "projects/dataplex-types/locations/global/aspectTypes/process_lineage"
            aspect_types = [aspect_type]

            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            
            entry = client.get_entry(request=request)
            for aspect_key, aspect in entry.aspects.items():
                if aspect_key.endswith("global.process_lineage") and aspect.path == "":
                    return dict(aspect.data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting job sources for {table_fqn}: {e}")
            return None 

    def mark_table_for_regeneration(self, table_fqn: str) -> bool:
        """Marks a table for regeneration by setting the to-be-regenerated flag in its metadata.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')

        Returns:
            bool: True if the table was successfully marked for regeneration, False otherwise.

        Raises:
            Exception: If there is an error updating the table metadata in Dataplex
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            # Create entry name
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]

            # Get existing entry with aspects
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type

            # Update or create aspect data
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path=="":
                    logger.info(f"Updating existing aspect {i}")
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data.update({
                        "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to-be-regenerated": True
                    })
                    break
            else:
                # No existing aspect found, create new one
                aspect_data = {
                    "certified": "false",
                    "user-who-certified": "",
                    "contents": "",
                    "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to-be-regenerated": True,
                    "human-comments": [],
                    "negative-examples": [],
                    "external-document-uri": ""
                }
                data_struct = struct_pb2.Struct()
                data_struct.update(aspect_data)
                new_aspect.data = data_struct

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            # Update entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )

            # Make the request
            response = client.update_entry(request=request)
            logger.info(f"Successfully marked table {table_fqn} for regeneration")
            return True

        except Exception as e:
            logger.error(f"Failed to mark table {table_fqn} for regeneration: {str(e)}")
            return False

    def mark_table_as_regenerated(self, table_fqn: str) -> bool:
        """Marks a table as regenerated by setting the to-be-regenerated flag to false in its metadata.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')

        Returns:
            bool: True if the table was successfully marked as regenerated, False otherwise.

        Raises:
            Exception: If there is an error updating the table metadata in Dataplex
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            # Create entry name
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_types = [aspect_type]

            # Get existing entry with aspects
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type

            # Update or create aspect data
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}""") and entry.aspects[i].path=="":
                    logger.info(f"Updating existing aspect {i}")
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data.update({
                        "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to-be-regenerated": False
                    })
                    break
            else:
                # No existing aspect found, create new one
                aspect_data = {
                    "certified": "false",
                    "user-who-certified": "",
                    "contents": "",
                    "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to-be-regenerated": False,
                    "human-comments": [],
                    "negative-examples": [],
                    "external-document-uri": ""
                }
                data_struct = struct_pb2.Struct()
                data_struct.update(aspect_data)
                new_aspect.data = data_struct

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry_name
            new_entry.aspects[aspect_name] = new_aspect

            # Update entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )

            # Make the request
            response = client.update_entry(request=request)
            logger.info(f"Successfully marked table {table_fqn} as regenerated")
            return True

        except Exception as e:
            logger.error(f"Failed to mark table {table_fqn} as regenerated: {str(e)}")
            return False

    def mark_column_for_regeneration(self, table_fqn: str, column_name: str) -> bool:
        """Marks a column for regeneration by setting the to-be-regenerated flag in its metadata.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')
            column_name (str): The name of the column to mark for regeneration

        Returns:
            bool: True if the column was successfully marked for regeneration, False otherwise.

        Raises:
            Exception: If there is an error updating the column metadata in Dataplex
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            # Create entry name
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            aspect_types = [aspect_type]

            # Get existing entry
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type
            new_aspect.path = f"Schema.{column_name}"

            # Update or create aspect data
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                    logger.info(f"Updating existing aspect {i}")
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data.update({
                        "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to-be-regenerated": True
                    })
                    break
            else:
                # No existing aspect found, create new one
                aspect_data = {
                    "certified": "false",
                    "user-who-certified": "",
                    "contents": "",
                    "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to-be-regenerated": True,
                    "human-comments": [],
                    "negative-examples": [],
                    "external-document-uri": ""
                }
                data_struct = struct_pb2.Struct()
                data_struct.update(aspect_data)
                new_aspect.data = data_struct

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry.name
            new_entry.aspects[aspect_name] = new_aspect

            # Update entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )

            # Make the request
            response = client.update_entry(request=request)
            logger.info(f"Successfully marked column {column_name} in table {table_fqn} for regeneration")
            return True

        except Exception as e:
            logger.error(f"Failed to mark column {column_name} in table {table_fqn} for regeneration: {str(e)}")
            return False

    def mark_column_as_regenerated(self, table_fqn: str, column_name: str) -> bool:
        """Marks a column as regenerated by setting the to-be-regenerated flag to false in its metadata.

        Args:
            table_fqn (str): The fully qualified name of the table (e.g., 'project.dataset.table')
            column_name (str): The name of the column to mark as regenerated

        Returns:
            bool: True if the column was successfully marked as regenerated, False otherwise.

        Raises:
            Exception: If there is an error updating the column metadata in Dataplex
        """
        try:
            client = self._client._cloud_clients[constants["CLIENTS"]["DATAPLEX_CATALOG"]]
            project_id, dataset_id, table_id = self._client._utils.split_table_fqn(table_fqn)

            # Create entry name
            entry_name = f"projects/{project_id}/locations/{self._get_dataset_location(table_fqn)}/entryGroups/@bigquery/entries/bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
            aspect_type = f"""projects/{self._client._project_id}/locations/global/aspectTypes/{constants["ASPECT_TEMPLATE"]["name"]}"""
            aspect_name = f"""{self._client._project_id}.global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}"""
            aspect_types = [aspect_type]

            # Get existing entry
            request = dataplex_v1.GetEntryRequest(
                name=entry_name,
                view=dataplex_v1.EntryView.CUSTOM,
                aspect_types=aspect_types
            )
            entry = client.get_entry(request=request)

            # Create new aspect
            new_aspect = dataplex_v1.Aspect()
            new_aspect.aspect_type = aspect_type
            new_aspect.path = f"Schema.{column_name}"

            # Update or create aspect data
            for i in entry.aspects:
                if i.endswith(f"""global.{constants["ASPECT_TEMPLATE"]["name"]}@Schema.{column_name}""") and entry.aspects[i].path == f"Schema.{column_name}":
                    logger.info(f"Updating existing aspect {i}")
                    new_aspect.data = entry.aspects[i].data
                    new_aspect.data.update({
                        "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "to-be-regenerated": False
                    })
                    break
            else:
                # No existing aspect found, create new one
                aspect_data = {
                    "certified": "false",
                    "user-who-certified": "",
                    "contents": "",
                    "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "to-be-regenerated": False,
                    "human-comments": [],
                    "negative-examples": [],
                    "external-document-uri": ""
                }
                data_struct = struct_pb2.Struct()
                data_struct.update(aspect_data)
                new_aspect.data = data_struct

            # Create new entry with updated aspect
            new_entry = dataplex_v1.Entry()
            new_entry.name = entry.name
            new_entry.aspects[aspect_name] = new_aspect

            # Update entry
            request = dataplex_v1.UpdateEntryRequest(
                entry=new_entry,
                update_mask=field_mask_pb2.FieldMask(paths=["aspects"]),
                allow_missing=False,
                aspect_keys=[aspect_name]
            )

            # Make the request
            response = client.update_entry(request=request)
            logger.info(f"Successfully marked column {column_name} in table {table_fqn} as regenerated")
            return True

        except Exception as e:
            logger.error(f"Failed to mark column {column_name} in table {table_fqn} as regenerated: {str(e)}")
            return False 