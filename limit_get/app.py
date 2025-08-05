import json
import boto3
import os
from decimal import Decimal

# --- Helper to handle DynamoDB's Decimal type ---
# DynamoDB stores numbers as Decimal objects, which are not directly JSON serializable.
# This helper function converts them to standard floats or ints.
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Check if it's an integer or a float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        # Let the base class default method raise the TypeError
        return super(DecimalEncoder, self).default(obj)

# --- Configuration ---
# Use an environment variable for the table name for better configuration management.
TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'CreditProfileTable')

# --- AWS Client Initialization ---
# Initialize the DynamoDB resource outside the handler for performance reuse.
dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(TABLE_NAME)


#TODO: Change access control of response to domain

def lambda_handler(event, context):
    """
    This function is triggered by an API Gateway GET request.
    It retrieves a user's complete credit profile from the DynamoDB table
    and returns it.

    API Gateway Path: /score/{userId}
    """
    print(f"Received event: {event}")

    try:
        # 1. Extract the userId from the path parameters of the API Gateway request
        user_id = event.get('pathParameters', {}).get('userId')

        if not user_id:
            print("ERROR: userId not found in path parameters.")
            return {
                'statusCode': 400,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*' # Or your specific domain
                },
                'body': json.dumps({'error': 'userId is missing from the path.'})
            }

        print(f"Attempting to retrieve score for userId: {user_id}")

        # 2. Fetch the item from the DynamoDB table using the userId as the key
        response = table.get_item(
            Key={'userId': user_id} # Assuming 'userId' is your partition key
        )


        if 'Item' in response:
            item = response['Item']
            
            if 'score' in item:
                response_data = {
                    'userId': item.get('userId'),
                    'score': item.get('score')
                }
                print(f"User found with score. Returning: {response_data}")
                return {
                    'statusCode': 200,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps(response_data, cls=DecimalEncoder)
                }
            else:
                
                response_data = {
                    'userId': item.get('userId'),
                    'status': 'Scoring in progress'
                }
                print(f"User found but score is pending. Returning: {response_data}")
                return {
                    'statusCode': 202,
                    'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
                    'body': json.dumps(response_data)
                }
        else:
            # If the item is not found, return a 404 error
            print(f"User with userId: {user_id} not found.")
            return {
                'statusCode': 404,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*' # Or your specific domain
                },
                'body': json.dumps({'error': 'User not found.'})
            }

    except Exception as e:
        # Handle any unexpected errors during execution
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*' # Or your specific domain
            },
            'body': json.dumps({'error': 'An internal server error occurred.'})
        }
