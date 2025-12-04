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

from fastapi import FastAPI, Body, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from dataplexutils.metadata.client import Client
from dataplexutils.metadata.client_options import ClientOptions
from dataplexutils.metadata.version import __version__
from pydantic import BaseModel
import logging
import datetime
import traceback
from pydantic import ValidationError
import json
import uuid
from google.cloud import dataplex_v1
import toml
import pkgutil

# Load constants
constants = toml.loads(pkgutil.get_data("dataplexutils.metadata", "constants.toml").decode())

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

class ClientOptionsSettings(BaseModel):
    use_lineage_tables: bool
    use_lineage_processes: bool
    use_profile: bool
    use_data_quality: bool
    use_ext_documents: bool
    persist_to_dataplex_catalog: bool
    stage_for_review: bool
    top_values_in_description: bool
    description_handling: str
    description_prefix: str


class ClientSettings(BaseModel):
    project_id: str
    llm_location: str
    dataplex_location: str


class TableSettings(BaseModel):
    project_id: str
    dataset_id: str
    table_id: str
    documentation_uri: str | None = None

class DatasetSettings(BaseModel):
    project_id: str
    dataset_id: str | None = None
    documentation_csv_uri: str
    strategy: str

class ColumnSettings(BaseModel):
    column_name: str

class RegenerationCounts(BaseModel):
    tables: int
    columns: int

class RegenerationRequest(BaseModel):
    objects: list[str]

class MarkForRegenerationRequest(BaseModel):
    table_fqn: str
    column_name: str | None = None

class UpdateDraftDescriptionRequest(BaseModel):
    client_settings: ClientSettings
    table_settings: TableSettings
    description: str
    is_html: bool

class AddCommentRequest(BaseModel):
    client_settings: ClientSettings
    table_settings: TableSettings
    comment: str
    column_name: str | None = None

class AddNegativeExampleRequest(BaseModel):
    client_settings: ClientSettings
    table_settings: TableSettings
    example: str

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for debugging
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/version")
def read_version():
    return {"version": __version__}


@app.post("/generate_table_description")
def generate_table_description(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
 
    """
        Generates a table description in Dataplex using the provided settings.

        Args:
            client_options_settings: Configuration for the Dataplex client options.
            client_settings: Project and location details.
            table_settings: Table identifier information.

        Returns:
            The result of the table description generation process, or an error
            message if something goes wrong.

    """
    try:
        print("Client options class definition: ",ClientOptions.__dict__)
        client_options = ClientOptions(
            use_lineage_tables=client_options_settings.use_lineage_tables,
            use_lineage_processes=client_options_settings.use_lineage_processes,
            use_profile=client_options_settings.use_profile,
            use_data_quality=client_options_settings.use_data_quality,
            use_ext_documents=client_options_settings.use_ext_documents,
            persist_to_dataplex_catalog=client_options_settings.persist_to_dataplex_catalog,
            stage_for_review=client_options_settings.stage_for_review,
            top_values_in_description=client_options_settings.top_values_in_description,
            description_handling=client_options_settings.description_handling,
            description_prefix=client_options_settings.description_prefix
        )
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
            client_options=client_options
        )
        table_fqn = f"{table_settings.project_id}.{table_settings.dataset_id}.{table_settings.table_id}"
        logger.info(f"Received arguments: {client_options_settings}, {client_settings}, {table_settings}")
        logger.info(f"Generating for table: {table_fqn}")
        client.generate_table_description(table_fqn,table_settings.documentation_uri)
        return {
            "message": "Table description generated successfully"
           
        }
    except Exception as e:
        logger.exception("An error occurred while generating table descriptions") 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

@app.post("/generate_columns_descriptions")
def generate_columns_descriptions(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
    try:
        client_options = ClientOptions(
            use_lineage_tables=client_options_settings.use_lineage_tables,
            use_lineage_processes=client_options_settings.use_lineage_processes,
            use_profile=client_options_settings.use_profile,
            use_data_quality=client_options_settings.use_data_quality,
            use_ext_documents=client_options_settings.use_ext_documents,
            persist_to_dataplex_catalog=client_options_settings.persist_to_dataplex_catalog,
            stage_for_review=client_options_settings.stage_for_review,
            top_values_in_description=client_options_settings.top_values_in_description,
            description_handling=client_options_settings.description_handling,
            description_prefix=client_options_settings.description_prefix
        )
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
            client_options=client_options
        )

        table_fqn = f"{table_settings.project_id}.{table_settings.dataset_id}.{table_settings.table_id}"
        client.generate_columns_descriptions(table_fqn, table_settings.documentation_uri)
        return {"message": "Column descriptions generated successfully"}
    except Exception as e:
        logger.exception("An error occurred while generating column descriptions") 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

@app.post("/generate_dataset_tables_descriptions")
def generate_dataset_tables_descriptions(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
    """
        Generates a table description in Dataplex using the provided settings.

        Args:
            client_options_settings: Configuration for the Dataplex client options.
            client_settings: Project and location details.
            dataset_settings: Dataset identifier information.
        
        Returns:
            The result of the multiple table description generation process, or an error
            message if something goes wrong.
    
    """
    try:
        logger.debug("Generating dataset tables request")
        client_options = ClientOptions(
            use_lineage_tables=client_options_settings.use_lineage_tables,
            use_lineage_processes=client_options_settings.use_lineage_processes,
            use_profile=client_options_settings.use_profile,
            use_data_quality=client_options_settings.use_data_quality,
            use_ext_documents=client_options_settings.use_ext_documents,
            persist_to_dataplex_catalog=client_options_settings.persist_to_dataplex_catalog,
            stage_for_review=client_options_settings.stage_for_review,
            top_values_in_description=client_options_settings.top_values_in_description,
            description_handling=client_options_settings.description_handling,
            description_prefix=client_options_settings.description_prefix
        )
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,        
            client_options=client_options,
            
        )

        dataset_fqn = f"{dataset_settings.project_id}.{dataset_settings.dataset_id}"
        logger.info(f"Received arguments: {client_options_settings}, {client_settings}, {dataset_settings}")
        logger.info(f"Generating for dataset: {dataset_fqn}")
        client.generate_dataset_tables_descriptions(dataset_fqn,dataset_settings.strategy,dataset_settings.documentation_csv_uri)
        return {"message": "Dataset table descriptions generated successfully"}
    except Exception as e:
        logger.exception("An error occurred while generating dataset descriptions") 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

@app.post("/generate_dataset_tables_columns_descriptions")
def generate_dataset_tables_columns_descriptions(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
    """
        Generates a table description in Dataplex using the provided settings.

        Args:
            client_options_settings: Configuration for the Dataplex client options.
            client_settings: Project and location details.
            dataset_settings: Dataset identifier information.
        
        Returns:
            The result of the multiple table description generation process, or an error
            message if something goes wrong.
    
    """
    try:
        logger.debug("Generating dataset tables request")
        client_options = ClientOptions(
            use_lineage_tables=client_options_settings.use_lineage_tables,
            use_lineage_processes=client_options_settings.use_lineage_processes,
            use_profile=client_options_settings.use_profile,
            use_data_quality=client_options_settings.use_data_quality,
            use_ext_documents=client_options_settings.use_ext_documents,
            persist_to_dataplex_catalog=client_options_settings.persist_to_dataplex_catalog,
            stage_for_review=client_options_settings.stage_for_review,
            top_values_in_description=client_options_settings.top_values_in_description,
            description_handling=client_options_settings.description_handling,
            description_prefix=client_options_settings.description_prefix
        )
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,        
            client_options=client_options,            
        )

        dataset_fqn = f"{dataset_settings.project_id}.{dataset_settings.dataset_id}"
        logger.info(f"Received arguments: {client_options_settings}, {client_settings}, {dataset_settings}")
        logger.info(f"Generating for dataset: {dataset_fqn}")
        client.generate_dataset_tables_columns_descriptions(dataset_fqn,dataset_settings.strategy,dataset_settings.documentation_csv_uri)
        return {"message": "Dataset table columns descriptions generated successfully"}
    except Exception as e:
        logger.exception("An error occurred while generating dataset descriptions") 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

@app.post("/accept_table_draft_description")
def accept_table_draft_description(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
    """
    Accepts the draft description for a table, promoting it to the actual table description.

    Args:
        client_options_settings: Configuration for the Dataplex client options.
        client_settings: Project and location details.
        table_settings: Table identifier information.

    Returns:
        A message indicating success or failure.
    """
    try:
        logger.info("=== START: accept_table_draft_description ===")
        client_options = ClientOptions(
            use_lineage_tables=client_options_settings.use_lineage_tables,
            use_lineage_processes=client_options_settings.use_lineage_processes,
            use_profile=client_options_settings.use_profile,
            use_data_quality=client_options_settings.use_data_quality,
            use_ext_documents=client_options_settings.use_ext_documents,
            persist_to_dataplex_catalog=client_options_settings.persist_to_dataplex_catalog,
            stage_for_review=client_options_settings.stage_for_review,
            description_handling=client_options_settings.description_handling,
            description_prefix=client_options_settings.description_prefix
        )
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
            client_options=client_options
        )

        table_fqn = f"{table_settings.project_id}.{table_settings.dataset_id}.{table_settings.table_id}"
        logger.info(f"Accepting draft description for table: {table_fqn}")
        
        # Get existing comments and negative examples
        existing_comments = client.get_comments_to_table_draft_description(table_fqn) or []
        existing_negative_examples = client.get_negative_examples_to_table_draft_description(table_fqn) or []
        
        # First, update the aspect metadata to mark it as accepted
        # Only use fields that are defined in the aspect template
        aspect_content = {
            "certified": "true",
            "user-who-certified": "system",  # You might want to pass the actual user from the frontend
            "contents": client._review_ops.get_review_item_details(table_fqn)["draftDescription"],
            "generation-date": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to-be-regenerated": "false",
            "human-comments": existing_comments,  # Preserve existing comments
            "negative-examples": existing_negative_examples,  # Preserve existing negative examples
            "external-document-uri": table_settings.documentation_uri if hasattr(table_settings, 'documentation_uri') else "",
            "is-accepted": True,
            "when-accepted": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        # Update the aspect with the new metadata
        success = client._dataplex_ops.update_table_draft_description(
            table_fqn=table_fqn,
            description=aspect_content["contents"],
            metadata=aspect_content
        )
        
        if not success:
            raise Exception("Failed to update aspect metadata")
        
        # Then promote the draft description to the actual description
        client.accept_table_draft_description(table_fqn)
        
        logger.info("Draft description accepted and metadata updated successfully")
        return {"message": "Table draft description accepted successfully"}
    except Exception as e:
        logger.error("=== ERROR in accept_table_draft_description ===")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
            detail=str(e)
        )
    finally:
        logger.info("=== END: accept_table_draft_description ===")

@app.post("/accept_column_draft_description")
def accept_column_draft_description(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
    column_settings: ColumnSettings = Body()
):
    """
    Accepts the draft description for a column, promoting it to the actual column description.

    Args:
        client_options_settings: Configuration for the Dataplex client options.
        client_settings: Project and location details.
        table_settings: Table identifier information.
        column_settings: Column identifier information.

    Returns:
        A message indicating success or failure.
    """
    try:
        client_options = ClientOptions(
            use_lineage_tables=client_options_settings.use_lineage_tables,
            use_lineage_processes=client_options_settings.use_lineage_processes,
            use_profile=client_options_settings.use_profile,
            use_data_quality=client_options_settings.use_data_quality,
            use_ext_documents=client_options_settings.use_ext_documents,
            persist_to_dataplex_catalog=client_options_settings.persist_to_dataplex_catalog,
            stage_for_review=client_options_settings.stage_for_review,
            description_handling=client_options_settings.description_handling,
            description_prefix=client_options_settings.description_prefix
        )
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
            client_options=client_options
        )

        table_fqn = f"{table_settings.project_id}.{table_settings.dataset_id}.{table_settings.table_id}"
        logger.info(f"Accepting draft description for column {column_settings.column_name} in table: {table_fqn}")
        client.accept_column_draft_description(table_fqn, column_settings.column_name)
        return {"message": f"Column {column_settings.column_name} draft description accepted successfully"}
    except Exception as e:
        logger.exception("An error occurred while accepting column draft description") 
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    logger.error(f"HTTP error occurred: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )

@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    logger.debug(f"Headers: {request.headers}")
    body = await request.body()
    logger.debug(f"Body: {body.decode()}")
    response = await call_next(request)
    return response

# Regeneration Management APIs
@app.post("/get_regeneration_counts")
def get_regeneration_counts(
    client_settings: ClientSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
    search_query: str = Body(None),
):
    try:
        # Validate required parameters
        if not client_settings.project_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="project_id is required"
            )
        if not client_settings.llm_location:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="llm_location is required"
            )
        if not client_settings.dataplex_location:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="dataplex_location is required"
            )
        if not dataset_settings.project_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="dataset_project_id is required"
            )
            
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        dataset_fqn = f"{dataset_settings.project_id}.{dataset_settings.dataset_id}" 
        logger.info(f"Getting regeneration counts for dataset: {dataset_fqn}")
        
        # Use _list_tables_in_dataset_for_regeneration to get tables marked for regeneration
        tables = client._table_ops._list_tables_in_dataset_for_regeneration(dataset_fqn)
        tables_count = len(tables)
        
        logger.info(f"Found {tables_count} tables marked for regeneration")
        
        return RegenerationCounts(
            tables=tables_count,
            columns=0  # TODO: Implement column counting
        )
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error in get_regeneration_counts: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Add a GET endpoint for backward compatibility
@app.get("/get_regeneration_counts")
def get_regeneration_counts_get(
    project_id: str,
    llm_location: str,
    dataplex_location: str,
    dataset_project_id: str,
    dataset_id: str,
    search_query: str = None,
):
    logger.info(f"GET request received for get_regeneration_counts, converting to POST format")
    
    # Create the objects expected by the POST endpoint
    client_settings = ClientSettings(
        project_id=project_id,
        llm_location=llm_location,
        dataplex_location=dataplex_location
    )
    
    dataset_settings = DatasetSettings(
        project_id=dataset_project_id,
        dataset_id=dataset_id,
        documentation_csv_uri="",  # Required by the model but not used for this endpoint
        strategy=""  # Required by the model but not used for this endpoint
    )
    
    # Call the POST endpoint handler
    return get_regeneration_counts(
        client_settings=client_settings,
        dataset_settings=dataset_settings,
        search_query=search_query
    )

@app.post("/regenerate_selected")
def regenerate_selected(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
    regeneration_request: RegenerationRequest = Body(),
):
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
            client_options=ClientOptions(**client_options_settings.dict())
        )
        
        dataset_fqn = f"{dataset_settings.project_id}.{dataset_settings.dataset_id}"
        search_query = regeneration_request.objects[0] if regeneration_request.objects else None
        
        logger.info(f"Processing regeneration for dataset: {dataset_fqn} with filter: {search_query}")
        
        # Use the utility method to build the effective query
        # This ensures the dataset_fqn is always included in the query
        effective_query = client._review_ops.build_search_query_for_regeneration(dataset_fqn, search_query)
        logger.info(f"Final query for review items: {effective_query}")
        
        # Get all items matching the pattern
        matching_items = client._review_ops.get_review_items_for_dataset(dataset_fqn, effective_query)
        items = matching_items.get("data", {}).get("items", [])
        
        logger.info(f"Found {len(items)} items matching filter")
        
        results = []
        for item in items:
            item_name = item.get("name", "")
            logger.info(f"Regenerating item: {item_name}")
            
            # TODO: Implement actual regeneration logic here
            # This is a placeholder - you'll need to implement the actual regeneration
            # based on your application's requirements
            
            results.append({"object": item_name, "status": "regenerated"})
        
        return {"regenerated_objects": results}
    except Exception as e:
        logger.error(f"Error in regenerate_selected: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/regenerate_all")
def regenerate_all(
    client_options_settings: ClientOptionsSettings = Body(),
    client_settings: ClientSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
            client_options=ClientOptions(**client_options_settings.dict())
        )
        
        dataset_fqn = f"{dataset_settings.project_id}.{dataset_settings.dataset_id}"
        logger.info(f"Regenerating all marked items in dataset: {dataset_fqn}")
        
        # Set regeneration flag to True
        client._client_options._regenerate = True
        
        # Call generate_dataset_tables_columns_descriptions with regeneration flag
        result = client.regenerate_dataset_tables_columns_descriptions(
            dataset_fqn=dataset_fqn,
            strategy=dataset_settings.strategy,
            documentation_csv_uri=dataset_settings.documentation_csv_uri
        )
        
        # Reset regeneration flag
        client._client_options._regenerate = False
        
        return {"message": "All marked items (tables and columns) regenerated successfully"}
    except Exception as e:
        logger.error(f"Error in regenerate_all: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

# Review Management Models
class Comment(BaseModel):
    id: str
    text: str
    type: str
    timestamp: str

class MetadataItem(BaseModel):
    id: str
    type: str
    name: str
    currentDescription: str
    draftDescription: str
    isHtml: bool
    status: str
    lastModified: str
    comments: list[Comment]
    markedForRegeneration: bool = False

# Review Management APIs
@app.post("/metadata/review")
def get_review_items(
    client_settings: ClientSettings = Body(),
    dataset_settings: DatasetSettings = Body(),
):
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        # Only validate project_id
        if not dataset_settings.project_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="project_id must be provided"
            )
        
        try:
            # Get all review items for the project, optionally filtered by dataset
        
            
            logger.info(f"Getting review items for project {dataset_settings.project_id}")
                
            result = client._review_ops.get_review_items_for_dataset(dataset_fqn=dataset_settings.dataset_id)
            logger.info(f"Raw result from review_ops: {result}")
            
            # Ensure we always return a properly structured response
            if not isinstance(result, dict):
                result = {"items": [], "nextPageToken": None, "totalCount": 0}
            
            # If result has a "data" wrapper, unwrap it
            if isinstance(result, dict) and "data" in result:
                result = result["data"]
            
            # Ensure all required fields are present
            response_data = {
                "items": result.get("items", []),
                "nextPageToken": result.get("nextPageToken", None),
                "totalCount": result.get("totalCount", len(result.get("items", [])))
            }
            
            logger.info(f"Structured response data: {response_data}")
            return response_data
            
        except Exception as e:
            logger.error(f"Error getting review items: {str(e)}")
            return {
                "items": [],
                "nextPageToken": None,
                "totalCount": 0
            }
            
    except Exception as e:
        logger.error(f"Error in get_review_items: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/metadata/review/{id}/reject")
def reject_review_item(
    id: str,
    client_settings: ClientSettings = Body(),
):
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        result = client.reject_review_item(id)
        return {"status": "rejected", "id": id, **result}
    except Exception as e:
        logger.error(f"Error in reject_review_item: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/metadata/review/{id}/edit")
def edit_review_item(
    id: str,
    client_settings: ClientSettings = Body(),
    description: str = Body(..., embed=True),
):
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        result = client.edit_review_item(id, description)
        return {"status": "updated", "id": id, **result}
    except Exception as e:
        logger.error(f"Error in edit_review_item: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/metadata/review/{id}/comment")
def add_review_comment(
    id: str,
    client_settings: ClientSettings = Body(),
    comment: str = Body(..., embed=True),
):
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        # TODO: Implement comment logic
        return {
            "status": "added",
            "id": id,
            "comment": {
                "id": "new_comment_id",
                "text": comment,
                "type": "human",
                "timestamp": datetime.datetime.now().isoformat()
            }
        }
    except Exception as e:
        logger.error(f"Error in add_review_comment: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/mark_for_regeneration")
def mark_for_regeneration(
    client_settings: ClientSettings = Body(),
    request: MarkForRegenerationRequest = Body(),
):
    """Mark a table or column for regeneration.

    If column_name is provided, marks the specific column for regeneration.
    If only table_fqn is provided, marks the entire table for regeneration.
    """
    try:
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        if request.column_name:
            success = client.mark_column_for_regeneration(request.table_fqn, request.column_name)
            if success:
                return {"message": f"Column {request.column_name} in table {request.table_fqn} marked for regeneration"}
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to mark column {request.column_name} for regeneration"
                )
        else:
            success = client.mark_table_for_regeneration(request.table_fqn)
            if success:
                return {"message": f"Table {request.table_fqn} marked for regeneration"}
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to mark table {request.table_fqn} for regeneration"
                )
    except Exception as e:
        logger.error(f"Error in mark_for_regeneration: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/metadata/review/details")
def get_review_item_details(
    client_settings: ClientSettings = Body(),
    table_settings: TableSettings = Body(),
    column_name: str = Body(None),
):
    """Get detailed information about a review item.
    
    Args:
        client_settings: Project and location details
        table_settings: Table identifier information
        column_name: Optional column name. If provided, returns column details
    
    Returns:
        Detailed information about the review item
    """
    try:
        logger.info(f"Getting details for table: {table_settings.project_id}.{table_settings.dataset_id}.{table_settings.table_id}")
        if column_name:
            logger.info(f"Column: {column_name}")
        
        client = Client(
            project_id=client_settings.project_id,
            llm_location=client_settings.llm_location,
            dataplex_location=client_settings.dataplex_location,
        )
        
        table_fqn = f"{table_settings.project_id}.{table_settings.dataset_id}.{table_settings.table_id}"
        
        if column_name:
            # Get column details
            details = client.get_review_item_details(table_fqn, column_name)
        else:
            # Get table details
            details = client.get_review_item_details(table_fqn)
            
        if not details:
            raise ValueError(f"No details found for {'column ' + column_name if column_name else 'table'} {table_fqn}")
        
        # Return the details directly without wrapping in data field
        return details

    except Exception as e:
        logger.error(f"Error getting review item details for table {table_fqn} column {column_name}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@app.post("/update_table_draft_description")
async def update_table_draft_description(request: Request, update_request: UpdateDraftDescriptionRequest):
    """Update the draft description for a table.
    
    Args:
        request: The raw request object for logging
        update_request: Request containing client settings, table settings, and the new description
    
    Returns:
        A dictionary with the status of the update operation
    """
    try:
        # Log the raw request body
        body = await request.body()
        logger.info("=== START: update_table_draft_description ===")
        logger.info(f"Raw request body: {body.decode()}")
        logger.info(f"Parsed request: {update_request.dict()}")
        
        client = Client(
            project_id=update_request.client_settings.project_id,
            llm_location=update_request.client_settings.llm_location,
            dataplex_location=update_request.client_settings.dataplex_location
        )
        
        # Construct the table FQN
        table_fqn = f"{update_request.table_settings.project_id}.{update_request.table_settings.dataset_id}.{update_request.table_settings.table_id}"
        logger.info(f"Constructed table FQN: {table_fqn}")
        
        # Update the draft description
        logger.info(f"Updating draft description. Length: {len(update_request.description)}")
        logger.info(f"Is HTML: {update_request.is_html}")
        
        success = client._dataplex_ops.update_table_draft_description(
            table_fqn=table_fqn,
            description=update_request.description
        )
        
        if success:
            logger.info("Draft description updated successfully")
            return {
                "status": "success",
                "message": "Draft description updated successfully"
            }
        else:
            logger.error("Failed to update draft description (returned False)")
            raise HTTPException(
                status_code=500,
                detail="Failed to update draft description"
            )
        
    except ValidationError as e:
        # Log validation errors in detail
        logger.error("=== Validation Error ===")
        logger.error(f"Error details: {e.errors()}")
        logger.error(f"Error JSON: {e.json()}")
        raise HTTPException(
            status_code=422,
            detail=f"Validation error: {str(e)}"
        )
    except Exception as e:
        logger.error("=== Error in update_table_draft_description ===")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}")
        logger.error(f"Traceback:\n{traceback.format_exc()}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update draft description: {str(e)}"
        )
    finally:
        logger.info("=== END: update_table_draft_description ===")

@app.post("/metadata/review/add_comment")
def add_comment(request: AddCommentRequest):
    """Add a comment to a table or column's draft description.
    
    Args:
        request: Contains client settings, table settings, and the comment to add
    
    Returns:
        The newly added comment text
    """
    try:
        logger.info("=== START: add_comment ===")
        client = Client(
            project_id=request.client_settings.project_id,
            llm_location=request.client_settings.llm_location,
            dataplex_location=request.client_settings.dataplex_location,
        )
        
        table_fqn = f"{request.table_settings.project_id}.{request.table_settings.dataset_id}.{request.table_settings.table_id}"
        logger.info(f"Adding comment to table: {table_fqn}")
        
        if request.column_name:
            success = client.add_comment_to_column_draft_description(table_fqn, request.column_name, request.comment)
        else:
            success = client.add_comment_to_table_draft_description(table_fqn, request.comment)
            
        if not success:
            logger.error("Failed to add comment")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to add comment"
            )
            
        logger.info("Comment added successfully")
        return {"comment": request.comment}
        
    except Exception as e:
        logger.error(f"Error adding comment: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
    finally:
        logger.info("=== END: add_comment ===")

@app.post("/metadata/review/add_negative_example")
def add_negative_example(request: AddNegativeExampleRequest):
    """Add a negative example to a table's draft description.
    
    Args:
        request: Contains client settings, table settings, and the negative example to add
    
    Returns:
        The newly added negative example text
    """
    try:
        client = Client(
            project_id=request.client_settings.project_id,
            llm_location=request.client_settings.llm_location,
            dataplex_location=request.client_settings.dataplex_location,
        )
        
        table_fqn = f"{request.table_settings.project_id}.{request.table_settings.dataset_id}.{request.table_settings.table_id}"
        
        # Get existing aspect
        existing_comments = client.get_comments_to_table_draft_description(table_fqn) or []
        existing_negative_examples = client.get_negative_examples_to_table_draft_description(table_fqn) or []
        
        # Add to existing examples
        existing_negative_examples.append(request.example)
        
        # Update aspect with new metadata
        aspect_content = {
            "negative-examples": existing_negative_examples,
            "human-comments": existing_comments
        }
        
        success = client._dataplex_ops.update_table_draft_description(
            table_fqn=table_fqn,
            description=client._review_ops.get_review_item_details(table_fqn)["draftDescription"],
            metadata=aspect_content
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to add negative example"
            )
            
        return {"example": request.example}
        
    except Exception as e:
        logger.error(f"Error adding negative example: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
