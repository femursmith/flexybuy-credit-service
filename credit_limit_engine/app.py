import json
import os
from datetime import datetime
from decimal import Decimal
import boto3

# --- Fuzzy Logic Dependencies ---
# These must be included in your Lambda deployment package (e.g., via a Layer or .zip file)
import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl

# --- Configuration ---
CREDIT_PROFILE_TABLE = os.environ.get('CREDIT_PROFILE_TABLE', 'CreditProfileTable')
CREDIT_LIMIT_TABLE = os.environ.get('CREDIT_LIMIT_TABLE', 'CreditLimitTable')
CONFIDENCE_SCORE = float(os.environ.get('CONFIDENCE_SCORE', '0.8')) # Admin-configurable parameter
MODEL_VERSION = "v1.0.0"
MINIMUM_CREDIT_LIMIT = 50
MAXIMUM_CREDIT_LIMIT = 1000

# --- AWS Client Initialization ---
dynamodb_resource = boto3.resource('dynamodb')
credit_limit_table = dynamodb_resource.Table(CREDIT_LIMIT_TABLE)


# --- Fuzzy Logic System Definition (as provided) ---

# 1. Define fuzzy variables
DTI = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'DTI')
Volatility = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'Volatility')
MinBalance = ctrl.Antecedent(np.arange(0, 1.01, 0.01), 'MinBalance')
DebtHonesty = ctrl.Antecedent(np.arange(1, 5.1, 0.1), 'DebtHonesty')
Character = ctrl.Antecedent(np.arange(1, 5.1, 0.1), 'Character')
RiskScore = ctrl.Consequent(np.arange(0, 1.01, 0.01), 'RiskScore')

# 2. Membership functions
DTI['low'] = fuzz.trimf(DTI.universe, [0.0, 0.0, 0.3])
DTI['med'] = fuzz.trimf(DTI.universe, [0.2, 0.5, 0.8])
DTI['high'] = fuzz.trimf(DTI.universe, [0.6, 1.0, 1.0])
Volatility['stable'] = fuzz.trimf(Volatility.universe, [0.0, 0.0, 0.4])
Volatility['moderate'] = fuzz.trimf(Volatility.universe, [0.3, 0.5, 0.7])
Volatility['volatile'] = fuzz.trimf(Volatility.universe, [0.6, 1.0, 1.0])
MinBalance['low'] = fuzz.trimf(MinBalance.universe, [0.0, 0.0, 0.3])
MinBalance['med'] = fuzz.trimf(MinBalance.universe, [0.2, 0.5, 0.8])
MinBalance['high'] = fuzz.trimf(MinBalance.universe, [0.6, 1.0, 1.0])
DebtHonesty['poor'] = fuzz.trimf(DebtHonesty.universe, [1.0, 1.0, 3.0])
DebtHonesty['fair'] = fuzz.trimf(DebtHonesty.universe, [2.0, 3.0, 4.0])
DebtHonesty['good'] = fuzz.trimf(DebtHonesty.universe, [3.0, 5.0, 5.0])
Character['weak'] = fuzz.trimf(Character.universe, [1.0, 1.0, 3.0])
Character['average'] = fuzz.trimf(Character.universe, [2.0, 3.0, 4.0])
Character['strong'] = fuzz.trimf(Character.universe, [3.0, 5.0, 5.0])
RiskScore['low'] = fuzz.trimf(RiskScore.universe, [0.0, 0.0, 0.4])
RiskScore['medium'] = fuzz.trimf(RiskScore.universe, [0.3, 0.5, 0.7])
RiskScore['high'] = fuzz.trimf(RiskScore.universe, [0.6, 1.0, 1.0])

# 3. Fuzzy Rules
rules = [
    ctrl.Rule(DTI['high'] | Volatility['volatile'], RiskScore['high']),
    ctrl.Rule(MinBalance['low'] & (DTI['med'] | Volatility['moderate']), RiskScore['medium']),
    ctrl.Rule(DebtHonesty['good'] & Character['strong'] & DTI['low'], RiskScore['low']),
    ctrl.Rule(DebtHonesty['poor'] | Character['weak'], RiskScore['high']),
    ctrl.Rule(DebtHonesty['fair'] & Character['average'] & Volatility['stable'], RiskScore['medium']),
]

# 4. Control System and Simulation
evaluation_ctrl = ctrl.ControlSystem(rules)
evaluator = ctrl.ControlSystemSimulation(evaluation_ctrl)

def assess_risk(dti, volatility, min_balance, debt_honesty, character):
    """
    Compute risk score given normalized applicant metrics.
    Returns a float between 0 (low risk) and 1 (high risk).
    """
    evaluator.input['DTI'] = dti
    evaluator.input['Volatility'] = volatility
    evaluator.input['MinBalance'] = min_balance
    evaluator.input['DebtHonesty'] = debt_honesty
    evaluator.input['Character'] = character
    evaluator.compute()
    return evaluator.output['RiskScore']

# --- Helper Functions ---
def deserialize_dynamodb_item(item):
    """Converts a DynamoDB item (from a stream) into a regular Python dictionary."""
    if not item:
        return {}
    
    deserializer = boto3.dynamodb.types.TypeDeserializer()
    return {k: deserializer.deserialize(v) for k, v in item.items()}

# --- KYC Scoring Logic ---

def calculate_kyc_scores(kyc_answers):
    """Calculates Character and Capacity scores based on the marking scheme."""
    if not kyc_answers:
        return {'character_score': 7.5, 'capacity_score': 10}

    scores = {'character_score': 0, 'capacity_score': 0}
    
    # Character Score (Max 15)
    residence_map = {"More than 10 years": 5, "8 - 10 years": 4, "4 - 8 years": 3, "2 - 4 years": 2, "Less than 2 years": 1}
    borrow_history_map = {"Yes, but I paid it off": 5, "No, but I borrowed before": 4, "No": 3, "Yes, and I still owe money": 1}
    repayment_ability_map = {"Yes, without delays or challenges": 5, "It's difficult but I manage to pay": 2, "Sometimes I wasn't able to pay back": 0, "Not applicable": 3}
    
    scores['character_score'] += residence_map.get(kyc_answers.get('residenceDuration'), 1)
    scores['character_score'] += borrow_history_map.get(kyc_answers.get('borrowingHistory'), 3)
    scores['character_score'] += repayment_ability_map.get(kyc_answers.get('repaymentAbility'), 3)

    # Capacity Score (Max 15)
    income_map = {"Above 1800 GHS": 5, "1401 GHS - 1800 GHS": 4, "1001 GHS - 1400 GHS": 3, "701 GHS - 1000 GHS": 2, "351 GHS - 700 GHS": 1, "Below 350 GHS": 0}
    job_duration_map = {"More than 10 years": 5, "8 - 10 years": 4, "4 - 8 years": 3, "2 - 4 years": 2, "Less than 2 years": 1}
    borrow_source_map = {"Banks": 5, "Other Financial apps (digital)": 5, "Mobile Money providers (MTN, Telecel, AT)": 4, "Money lenders (physical / shop)": 2, "Friends or family": 2, "No applicable": 3}

    scores['capacity_score'] += income_map.get(kyc_answers.get('monthlyIncomeRange'), 0)
    scores['capacity_score'] += job_duration_map.get(kyc_answers.get('jobDuration'), 1)
    scores['capacity_score'] += borrow_source_map.get(kyc_answers.get('borrowingSource'), 3)

    return scores

# --- Main Credit Limit Engine ---

def calculate_initial_limit(profile):
    """
    Main engine to calculate and save the initial credit limit for a user.
    Accepts a deserialized user profile dictionary as input.
    """
    user_id = profile.get('userId')
    if not user_id:
        raise ValueError("userId not found in the provided profile.")
        
    print(f"Starting initial credit limit calculation for userId: {user_id}")

    # 1. Gather Data from the profile object
    kyc_answers = profile.get('kycAnswers', {})
    statement_metrics = profile.get('statementMetrics', {})
    per_statement_list = statement_metrics.get('perStatement', [])

    if not per_statement_list:
        raise ValueError("No statement analysis found in profile. Cannot calculate limit.")
        
    latest_statement = sorted(per_statement_list, key=lambda x: x['analysisDate'], reverse=True)[0]
        
    # 2. Normalize Data for Fuzzy Logic
    kyc_scores = calculate_kyc_scores(kyc_answers)
    debt_honesty = 1 + (kyc_scores['capacity_score'] / 15) * 4
    character = 1 + (kyc_scores['character_score'] / 15) * 4

    avg_income = float(latest_statement.get('avgMonthlyIncome', 0))
    avg_expenditure = float(latest_statement.get('avgMonthlyExpenditure', 0))
    avg_min_balance = float(latest_statement.get('avgLowestMonthlyBalance', 0))
    volatility_raw = float(latest_statement.get('balanceVolatility', 0))
    disposable_income = float(latest_statement.get('disposableIncome', 0))

    if avg_income == 0:
        print("Warning: Average monthly income is zero. Using default risk values.")
        dti, volatility, min_balance = 1.0, 1.0, 0.0
    else:
        dti = min(1.0, max(0.0, avg_expenditure / avg_income))
        volatility = min(1.0, max(0.0, volatility_raw / avg_income))
        min_balance = min(1.0, max(0.0, avg_min_balance / avg_income))

    print(f"Normalized Inputs -> DTI: {dti:.2f}, Volatility: {volatility:.2f}, MinBalance: {min_balance:.2f}, DebtHonesty: {debt_honesty:.2f}, Character: {character:.2f}")

    # 3. Execute Fuzzy Logic
    risk_score_output = assess_risk(dti, volatility, min_balance, debt_honesty, character)
    user_risk_score = 1.0 - risk_score_output
    print(f"Fuzzy Risk Output: {risk_score_output:.2f}, Inverted User Score: {user_risk_score:.2f}")

    # 4. Calculate Final Credit Limit
    initial_limit = disposable_income * CONFIDENCE_SCORE * user_risk_score
    
    # 5. Apply Business Rules
    if initial_limit < MINIMUM_CREDIT_LIMIT:
        final_limit = MINIMUM_CREDIT_LIMIT
    elif initial_limit > MAXIMUM_CREDIT_LIMIT:
        final_limit = MAXIMUM_CREDIT_LIMIT
    else:
        final_limit = int(initial_limit)
        
    print(f"Calculated initial limit: {initial_limit:.2f}, Final limit after rules: {final_limit}")

    # 6. Save Result to CreditLimitTable
    try:
        item_to_save = {
            'userId': user_id,
            'creditLimit': Decimal(str(final_limit)),
            'scoreLastCalculatedAt': datetime.utcnow().isoformat(),
            'modelVersion': MODEL_VERSION
        }
        credit_limit_table.put_item(Item=item_to_save)
        print(f"Successfully saved credit limit for user {user_id}.")
        return {"status": "success", "userId": user_id, "creditLimit": final_limit}
    except Exception as e:
        print(f"ERROR saving credit limit for user {user_id}: {e}")
        return {"status": "error", "message": str(e)}

# --- AWS Lambda Handler ---

def lambda_handler(event, context):
    """
    AWS Lambda handler function triggered by a DynamoDB Stream from CreditProfileTable.
    """
    print(f"Received event: {json.dumps(event)}")
    
    for record in event.get('Records', []):
        try:
            if record.get('eventName') not in ['INSERT', 'MODIFY']:
                print(f"Skipping event of type {record.get('eventName')}")
                continue

            # Get the full document from the stream's NewImage
            new_image = record['dynamodb'].get('NewImage')
            if not new_image:
                print("Skipping record with no NewImage.")
                continue
            
            # Convert the DynamoDB-formatted JSON into a standard Python dictionary
            profile = deserialize_dynamodb_item(new_image)
            
            print(f"Processing record for userId: {profile.get('userId')}")
            
            result = calculate_initial_limit(profile)
            
            if result.get('status') == 'error':
                print(f"Failed to calculate limit for {profile.get('userId')}. Reason: {result.get('message')}")

        except Exception as e:
            print(f"ERROR processing a record: {e}")
            continue
            
    return {
        'statusCode': 200,
        'body': json.dumps({'status': 'success', 'message': 'Stream processing finished.'})
    }
