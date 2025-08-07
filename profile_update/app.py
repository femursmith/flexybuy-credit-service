import json
import boto3
import os
from datetime import datetime

dynamodb = boto3.client('dynamodb')
TABLE_NAME = os.environ.get('CREDIT_PROFILE_TABLE')

def lambda_handler(event, context):
    routeKey = event.get('routeKey', '')
    method = event.get('httpMethod', '')
    body = json.loads(event.get('body', '{}'))
    
    user_id = body.get('userId')
    if not user_id:
        return _response(400, 'userId is required')
    
    timestamp = datetime.utcnow().isoformat() + 'Z'

    try:
        if routeKey == 'POST /profile':
            core_profile = body.get('coreProfile')
            if not core_profile:
                return _response(400, 'coreProfile is required')

            update_expression = 'SET coreProfile = :coreProfile, profileLastUpdatedAt = :updatedAt'
            expression_values = {
                ':coreProfile': {'M': _format_map(core_profile)},
                ':updatedAt': {'S': timestamp}
            }

        elif routeKey == 'POST /kyc_answers':
            kyc_answers = body.get('kycAnswers')
            if not kyc_answers:
                return _response(400, 'kycAnswers is required')

            update_expression = 'SET kycAnswers = :kycAnswers, profileLastUpdatedAt = :updatedAt'
            expression_values = {
                ':kycAnswers': {'M': _format_map(kyc_answers)},
                ':updatedAt': {'S': timestamp}
            }

        elif routeKey == 'POST /fin_activity':
            fin_metrics = body.get('finActivityMetrics')
            if not fin_metrics:
                return _response(400, 'finActivityMetrics is required')

            update_expression = 'SET finActivityMetrics = :metrics, profileLastUpdatedAt = :updatedAt'
            expression_values = {
                ':metrics': {'M': _format_map(fin_metrics)},
                ':updatedAt': {'S': timestamp}
            }

        else:
            return _response(404, 'Unsupported path or method')

        # Perform update
        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={'userId': {'S': user_id}},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )

        return _response(200, 'Update successful')
    
    except Exception as e:
        return _response(500, f'Error: {str(e)}')


def _format_map(data):
    formatted = {}
    for key, value in data.items():
        if isinstance(value, bool):
            formatted[key] = {'BOOL': value}
        elif isinstance(value, int) or isinstance(value, float):
            formatted[key] = {'N': str(value)}
        elif value is None:
            formatted[key] = {'NULL': True}
        else:
            formatted[key] = {'S': str(value)}
    return formatted


def _response(status_code, message):
    return {
        'statusCode': status_code,
        'headers': { 'Content-Type': 'application/json' },
        'body': json.dumps({'message': message})
    }
