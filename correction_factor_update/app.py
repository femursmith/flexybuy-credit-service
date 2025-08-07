import os
import json
import boto3

dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('CREDIT_PROFILE_TABLE')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    user_id = event.get('pathParameters', {}).get('userId')
    user_exists = False
    try:
        resp = dynamodb.get_item(
            TableName=table_name,
            Key={'userId': {'S': user_id}},
            ProjectionExpression='userId'
        )
        user_exists = 'Item' in resp

    except Exception:
        user_exists = False

    if not user_exists:
        return {
            "statusCode": 404,
            "body": json.dumps({"message": "User not found."})
        }
    if not user_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing userId in path."})
        }



    try:
        body = json.loads(event.get('body', '{}'))
        correction_factor = float(body.get('correction_factor', None))
    except (ValueError, TypeError):
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid correction_factor value."})
        }

    if correction_factor is None or not (0 < correction_factor < 1):
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "correction_factor must be between 0 and 1 (exclusive)."})
        }

    
    try:
        table.update_item(
            Key={'userId': user_id},
            UpdateExpression="SET correction_factor = :cf",
            ExpressionAttributeValues={':correctionFactor': correction_factor}
        )
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Correction factor updated successfully."})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to update correction factor.", "error": str(e)})
        }