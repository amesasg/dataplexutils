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
"""Dataplex Utils Metadata Wizard utility functions
   2024 Google
"""
# Standard library imports
import re
import logging
import toml
import pkgutil
import time

# Cloud imports
import vertexai
from vertexai.generative_models import GenerationConfig, GenerativeModel, Part
import vertexai.preview.generative_models as generative_models

# Load constants
constants = toml.loads(pkgutil.get_data(__name__, "constants.toml").decode())
# Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(constants["LOGGING"]["WIZARD_LOGGER"])

class MetadataUtils:
    """Utility functions for metadata operations."""

    def __init__(self, client):
        """Initialize with reference to main client."""
        self._client = client

    def split_table_fqn(self, table_fqn):
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
            logger.debug(f"Found 3 groups: {match.group(1)} {match.group(2)} {match.group(3)}")
            return match.group(1), match.group(2), match.group(3)
        except Exception as e:
            logger.error(f"Exception: {e}.")
            raise e

    def split_dataset_fqn(self, dataset_fqn):
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

    def combine_description(self, old_description, new_description, description_handling):
        """Combines old and new descriptions based on handling strategy.

        Args:
            old_description (str): The existing description
            new_description (str): The new description to add
            description_handling (str): How to combine the descriptions

        Returns:
            str: The combined description
        """
        if not new_description:
            logger.debug(f"No new description provided, returning old description: {old_description[:50]}...")
            return old_description

        # Convert description_handling to lowercase for case-insensitive comparison
        description_handling_lower = description_handling.lower() if description_handling else ""
        
        logger.debug(f"Combining descriptions: old_description: {old_description[:50]}...")
        logger.debug(f"new_description: {new_description[:50]}...")
        logger.debug(f"description_handling: {description_handling}")
        logger.debug(f"description_handling_lower: {description_handling_lower}")
        logger.debug(f"constants[\"DESCRIPTION_HANDLING\"][\"APPEND\"]: {constants['DESCRIPTION_HANDLING']['APPEND']}")
        logger.debug(f"constants[\"DESCRIPTION_HANDLING\"][\"PREPEND\"]: {constants['DESCRIPTION_HANDLING']['PREPEND']}")
        logger.debug(f"constants[\"DESCRIPTION_HANDLING\"][\"REPLACE\"]: {constants['DESCRIPTION_HANDLING']['REPLACE']}")

        if description_handling_lower == constants["DESCRIPTION_HANDLING"]["APPEND"]:
            logger.debug(f"Using APPEND strategy")
            if old_description:
                try:
                    # Try to find the AI warning prefix in old description
                    index = old_description.index(constants['OUTPUT_CLAUSES']['AI_WARNING'])
                    # If found, replace everything after the prefix
                    result = old_description[:index] + new_description
                    logger.debug(f"Found AI warning prefix, replacing content after prefix: {result[:50]}...")
                    return result
                except ValueError:
                    # If no prefix found, append normally
                    result = old_description + new_description
                    logger.debug(f"No AI warning prefix found, appending normally: {result[:50]}...")
                    return result
            logger.debug(f"No old description, returning new description: {new_description[:50]}...")
            return new_description
        elif description_handling_lower == constants["DESCRIPTION_HANDLING"]["PREPEND"]:
            logger.debug(f"Using PREPEND strategy")
            result = new_description + old_description
            logger.debug(f"Prepending new description: {result[:50]}...")
            return result
        elif description_handling_lower == constants["DESCRIPTION_HANDLING"]["REPLACE"]:
            logger.debug(f"Using REPLACE strategy")
            logger.debug(f"Replacing old description with new description: {new_description[:50]}...")
            return new_description
        else:
            logger.debug(f"No valid description handling provided, returning old description: {old_description[:50]}...")
            return old_description

    def llm_inference(self, prompt, documentation_uri=None):
        """Performs LLM inference using Vertex AI.

        Args:
            prompt (str): The prompt to send to the LLM
            documentation_uri (str, optional): URI of documentation to include

        Returns:
            str: The generated text response
        """
        retries = 3
        base_delay = 1
        for attempt in range(retries + 1):
            try:
                vertexai.init(project=self._client._project_id, location=self._client.llm_location)
                if self._client._client_options._use_ext_documents:
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
                if documentation_uri is not None:
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


    def llm_inference_validate_field(self, prompt,context=None):
        """Performs LLM inference using Vertex AI.

        Args:
            prompt (str): The prompt to send to the LLM
            documentation_uri (str, optional): URI of documentation to include

        Returns:
            str: The generated text response
        """
        retries = 3
        base_delay = 1
        for attempt in range(retries + 1):
            try:
                vertexai.init(project=self._client._project_id, location=self._client.llm_location)
                if self._client._client_options._use_ext_documents:
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
                
                # if context is not None:
                #     doc = Part.from_uri(
                #         documentation_uri, mime_type=constants["DATA"]["PDF_MIME_TYPE"]
                #     )
                #     responses = model.generate_content(
                #         [doc, prompt],
                #         generation_config=generation_config,
                #         safety_settings=safety_settings,
                #         stream=False,
                #     )
                # else:
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