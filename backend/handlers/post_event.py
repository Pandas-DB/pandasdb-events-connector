import json
import boto3
import pandas as pd
import io
from requests.utils import unquote
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext
from aws_lambda_powertools.utilities.data_classes import APIGatewayProxyEvent
from typing import Dict, Any, List, Union

from .utils.validators import validate_request
from .utils.utils import create_response

logger = Logger()
tracer = Tracer()


def convert_to_dataframe(data: Union[List, Dict]) -> pd.DataFrame:
    """
    Convert various JSON structures to a pandas DataFrame.

    Handles:
    - List of dictionaries [{...}, {...}]
    - Single dictionary {"col1": [...], "col2": [...]}
    - List of values [1, 2, 3]
    - Single dictionary with nested data
    """
    try:
        if isinstance(data, list):
            if len(data) == 0:
                raise ValueError("Empty data list provided")

            # If it's a list of dictionaries
            if isinstance(data[0], dict):
                return pd.DataFrame(data)

            # If it's a list of values
            return pd.DataFrame({"value": data})

        elif isinstance(data, dict):
            # If it's a dictionary of lists (columnar format)
            if all(isinstance(v, list) for v in data.values()):
                return pd.DataFrame(data)

            # If it's a single record
            return pd.DataFrame([data])

        else:
            raise ValueError(f"Unsupported data type: {type(data)}")

    except Exception as e:
        raise ValueError(f"Failed to convert data to DataFrame: {str(e)}")


@logger.inject_lambda_context
@tracer.capture_lambda_handler
def handler(event: APIGatewayProxyEvent, context: LambdaContext) -> Dict[str, Any]:
    s3 = boto3.client('s3')

    try:
        user_id = validate_request(event)
        bucket = f"events-{user_id}"

        try:
            body = json.loads(event['body'])
        except (TypeError, json.JSONDecodeError) as e:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Invalid JSON in request body',
                    'details': str(e)
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }

        path_params = event.get('pathParameters', {})
        key = unquote(path_params.get('event_name', ''))

        if not key:
            return create_response(400, {'error': 'Key parameter is required'})

        if not key.endswith('.parquet'):
            key = f"{key.rstrip('/')}/data.parquet"

        if 'data' not in body:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required field: data'
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }

        try:
            # Convert the data to a pandas DataFrame with validation
            try:
                df = convert_to_dataframe(body['data'])
            except ValueError as e:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'Invalid data format',
                        'details': str(e)
                    }),
                    'headers': {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*'
                    }
                }

            # Create a buffer to store the Parquet data
            parquet_buffer = io.BytesIO()

            # Write the DataFrame to the buffer in Parquet format
            df.to_parquet(parquet_buffer, engine='pyarrow', index=False)

            # Reset buffer position to the beginning
            parquet_buffer.seek(0)

            # Upload to S3
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/x-parquet'
            )

            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Data successfully stored as Parquet',
                    'location': f"s3://{bucket}/{key}",
                    'rows': len(df),
                    'columns': list(df.columns)
                }),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                }
            }
        except Exception as e:
            logger.exception('Error storing data in S3')
            raise

    except Exception as e:
        logger.exception('Unexpected error in event producer')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Internal server error',
                'details': str(e)
            }),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
