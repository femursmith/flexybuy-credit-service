import json
import boto3
import os
from decimal import Decimal


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


TABLE_NAME = os.environ.get('CreditLimitTable')


dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(TABLE_NAME)




def lambda_handler(event, context):
    
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
            
            if 'creditLimit' in item:
                response_data = {
                    'userId': item.get('userId'),
                    'limit': item.get('creditLimit')
                }
                print(f"User found with score. Returning: {response_data}")
                return {
                    'statusCode': 200,
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
                    'body': json.dumps(response_data)
                }
        else:
            # If the item is not found, return a 404 error
            print(f"User with userId: {user_id} not found.")
            return {
                'statusCode': 404,
                'body': json.dumps({'error': 'User not found.'})
            }

    except Exception as e:
        # Handle any unexpected errors during execution
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'An internal server error occurred.'})
        }
