import requests
import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# API Configuration
API_BASE_URL = "http://localhost:8000"  # Change this to match your API URL
PROJECT_ID = "jsk-dataplex-demo-380508"
DATASET_ID = "metadata_generation"
TABLE_ID = "cc"

def test_review_details():
    """Test the metadata review details endpoint to verify comment retrieval."""
    
    # Construct the request URL
    url = f"{API_BASE_URL}/metadata/review/details"
    
    # Prepare the request payload
    payload = {
        "client_settings": {
            "project_id": PROJECT_ID,
            "llm_location": "",
            "dataplex_location": "us-central1"
        },
        "table_settings": {
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID,
            "documentation_uri": ""
        }
    }
    
    try:
        logger.info("\n=== Testing Review Details Endpoint ===")
        logger.info("Making request to review details endpoint...")
        logger.info(f"Request URL: {url}")
        logger.info(f"Request payload: {json.dumps(payload, indent=2)}")
        
        # Make the request
        response = requests.post(url, json=payload)
        response.raise_for_status()
        
        # Parse the response
        data = response.json()
        logger.info(f"Response status code: {response.status_code}")
        logger.info(f"Raw response data: {json.dumps(data, indent=2)}")
        
        # Check if we got data back
        if not data.get('data'):
            logger.error("No data returned in response")
            return {
                'success': False,
                'error': 'No data returned in response'
            }
            
        # Extract comments from the response
        item_data = data['data']
        comments = item_data.get('comments', [])
        
        # Validate comments structure
        validation_errors = []
        
        # Log comments information
        logger.info(f"\nNumber of comments found: {len(comments)}")
        
        if len(comments) == 0:
            logger.warning("No comments found in the response")
            validation_errors.append("No comments found")
        
        for i, comment in enumerate(comments, 1):
            logger.info(f"\nComment {i}:")
            
            # Check required fields
            required_fields = ['id', 'text', 'type', 'timestamp']
            missing_fields = [field for field in required_fields if not comment.get(field)]
            
            if missing_fields:
                error = f"Comment {i} is missing required fields: {', '.join(missing_fields)}"
                validation_errors.append(error)
                logger.error(error)
            
            # Log comment details
            logger.info(f"ID: {comment.get('id')}")
            logger.info(f"Text: {comment.get('text')}")
            logger.info(f"Type: {comment.get('type')}")
            logger.info(f"Timestamp: {comment.get('timestamp')}")
            
            # Validate comment type
            if comment.get('type') not in ['human', 'negative']:
                error = f"Comment {i} has invalid type: {comment.get('type')}"
                validation_errors.append(error)
                logger.error(error)
            
            # Validate timestamp format
            try:
                if comment.get('timestamp'):
                    datetime.fromisoformat(comment['timestamp'].replace('Z', '+00:00'))
            except ValueError:
                error = f"Comment {i} has invalid timestamp format: {comment.get('timestamp')}"
                validation_errors.append(error)
                logger.error(error)
        
        # Check other important fields in the response
        if not item_data.get('type'):
            validation_errors.append("Response missing 'type' field")
        if not item_data.get('name'):
            validation_errors.append("Response missing 'name' field")
        if 'currentDescription' not in item_data:
            validation_errors.append("Response missing 'currentDescription' field")
        if 'draftDescription' not in item_data:
            validation_errors.append("Response missing 'draftDescription' field")
        
        success = len(validation_errors) == 0
        
        if success:
            logger.info("\n✓ All validation checks passed")
        else:
            logger.error("\n✗ Validation failed with the following errors:")
            for error in validation_errors:
                logger.error(f"  - {error}")
        
        return {
            'success': success,
            'errors': validation_errors if not success else None,
            'comments_count': len(comments),
            'response_data': item_data
        }
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Request failed: {str(e)}"
        logger.error(error_msg)
        if hasattr(e, 'response') and hasattr(e.response, 'text'):
            logger.error(f"Error response: {e.response.text}")
        return {
            'success': False,
            'error': error_msg
        }
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(error_msg)
        return {
            'success': False,
            'error': error_msg
        }

def print_test_summary(result):
    """Print a formatted summary of the test results."""
    logger.info("\n=== Test Summary ===")
    logger.info(f"Test Status: {'✓ Passed' if result['success'] else '✗ Failed'}")
    
    if result.get('comments_count') is not None:
        logger.info(f"Comments Found: {result['comments_count']}")
    
    if not result['success']:
        if 'error' in result:
            logger.info(f"Error: {result['error']}")
        if 'errors' in result and result['errors']:
            logger.info("\nValidation Errors:")
            for error in result['errors']:
                logger.info(f"  - {error}")
    
    if result.get('response_data'):
        logger.info("\nResponse Data Summary:")
        data = result['response_data']
        logger.info(f"  Type: {data.get('type')}")
        logger.info(f"  Name: {data.get('name')}")
        logger.info(f"  Status: {data.get('status')}")
        logger.info(f"  Has Current Description: {'Yes' if data.get('currentDescription') else 'No'}")
        logger.info(f"  Has Draft Description: {'Yes' if data.get('draftDescription') else 'No'}")

if __name__ == "__main__":
    logger.info("=== Starting API Tests ===")
    logger.info(f"Testing against API at: {API_BASE_URL}")
    logger.info(f"Testing table: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")
    
    # Test review details endpoint
    result = test_review_details()
    print_test_summary(result) 