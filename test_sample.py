from src.package.dataplexutils.metadata import Client
from google.api_core.exceptions import BadRequest, Forbidden

# Initialize the client
client = Client('test-project', 'us-central1', 'us')

# Test with a malformed query that should trigger an exception
try:
    # This is an invalid table name that should trigger an exception
    sample = client._bigquery_ops.get_table_sample('invalid-table-name', 10)
    print('Sample retrieved successfully:', sample)
except (BadRequest, Forbidden) as e:
    print('Expected exception caught correctly:', type(e).__name__, e)
except Exception as e:
    print('Unexpected error:', type(e).__name__, e)

# Test with a valid but nonexistent table
try:
    # This is a valid table name but it doesn't exist
    sample = client._bigquery_ops.get_table_sample('nonexistent.dataset.table', 10)
    print('Sample retrieved successfully:', sample)
except (BadRequest, Forbidden) as e:
    print('Expected exception caught correctly:', type(e).__name__, e)
except Exception as e:
    print('Unexpected error:', type(e).__name__, e) 