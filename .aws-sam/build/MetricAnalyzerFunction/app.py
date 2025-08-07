import json
import boto3
import os
import csv
from io import StringIO
from collections import defaultdict
import re
from datetime import datetime, timedelta
import statistics
from decimal import Decimal

# --- Configuration ---
DYNAMODB_TABLE = os.environ.get('CREDIT_PROFILE_TABLE')

# --- AWS Client Initialization ---
s3_client = boto3.client('s3')
dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(DYNAMODB_TABLE)

# --- Helper Functions ---

def clean_numeric(value_string):
    """Cleans a string to extract a float, handling commas and other characters."""
    if not isinstance(value_string, str):
        return 0.0
    try:
        cleaned_str = re.sub(r'[^\d.]', '', value_string)
        return float(cleaned_str) if cleaned_str else 0.0
    except (ValueError, TypeError):
        return 0.0

def parse_momo_date(date_string):
    """Parses the specific date format from the MTN statement."""
    if not date_string:
        return None
    try:
        cleaned_date_string = re.sub(r'(\d{2}-\w{3}-\d{4})[-\s]', r'\1 ', date_string.strip())
        return datetime.strptime(cleaned_date_string, '%d-%b-%Y %I:%M:%S %p')
    except (ValueError, TypeError):
        return None

def parse_bank_date(date_string):
    """Parses common date formats found in bank statements by trying multiple formats."""
    if not isinstance(date_string, str) or not date_string.strip():
        return None
    
    # List of formats to try in order of expected frequency
    formats_to_try = [
        '%d/%m/%Y',      # e.g., 21/07/2025
        '%d-%b-%Y',      # e.g., 21-Jul-2025
        '%Y-%m-%d',      # e.g., 2025-07-21
        '%d-%m-%Y',      # e.g., 21-07-2025
    ]
    
    for fmt in formats_to_try:
        try:
            return datetime.strptime(date_string.strip(), fmt)
        except (ValueError, TypeError):
            continue # Try the next format
            
    # If all formats fail, return None
    return None

def get_data_without_outliers(data_points):
    """Identifies and removes outliers from a list of numbers using the 3-sigma rule."""
    if len(data_points) < 3:
        return data_points, []
        
    mean = statistics.mean(data_points)
    stdev = statistics.stdev(data_points)
    
    if stdev == 0:
        return data_points, []
        
    outlier_threshold = 3 * stdev
    outliers = {p for p in data_points if abs(p - mean) > outlier_threshold}
    clean_data = [p for p in data_points if p not in outliers]
    
    return clean_data, list(outliers)


# --- Provider-Specific Analysis Functions ---

def analyze_bank_statement_csv(csv_content, user_id):
    """
    Performs data analysis on a processed bank statement CSV.
    This version uses the csv module to correctly handle multi-line headers.
    """
    print(f"Running analysis for Bank Statement CSV for user: {user_id}")
    
    csv_file = StringIO(csv_content)
    reader = csv.reader(csv_file)
    
    header = None
    data_rows_as_lists = []
    header_found = False

    # --- Step 1: Find Header and Collect Data Rows ---
    for row in reader:
        if header_found:
            # Append non-empty rows to our data list
            if row and any(cell.strip() for cell in row):
                data_rows_as_lists.append(row)
            continue

        # Check if the current row is the header by looking for keywords
        row_text = ' '.join(row).upper()
        header_keywords = ["DATE", "DESCRIPTION", "DEBIT", "CREDIT", "BALANCE"]
        
        if all(keyword in row_text for keyword in header_keywords):
            # Clean up the header: remove newlines and extra spaces
            header = [h.replace('\n', ' ').strip() for h in row]
            header_found = True

    if not header:
        raise ValueError("Could not find a valid bank statement data header row in the CSV.")

    # --- Step 2: Dynamically Map Header Columns and Parse Transactions ---
    # Find column names dynamically from the cleaned header list
    date_col = next((h for h in header if "DATE" in h.upper() and "VALUE" not in h.upper()), None)
    desc_col = next((h for h in header if "DESCRIPTION" in h.upper()), None)
    debit_col = next((h for h in header if "DEBIT" in h.upper()), None)
    credit_col = next((h for h in header if "CREDIT" in h.upper()), None)
    balance_col = next((h for h in header if "BALANCE" in h.upper()), None)

    if not all([date_col, desc_col, debit_col, credit_col, balance_col]):
        raise ValueError("Could not map all required columns from the detected header.")

    all_transactions = []
    latest_date = None

    for row_list in data_rows_as_lists:
        row_dict = dict(zip(header, row_list))
        
        date_obj = parse_bank_date(row_dict.get(date_col))
        if not date_obj:
            continue
        
        if latest_date is None or date_obj > latest_date:
            latest_date = date_obj

        all_transactions.append({
            'date': date_obj,
            'description': row_dict.get(desc_col, "").upper(),
            'debit': clean_numeric(row_dict.get(debit_col)),
            'credit': clean_numeric(row_dict.get(credit_col)),
            'balance': clean_numeric(row_dict.get(balance_col))
        })

    if not latest_date:
        raise ValueError("Could not parse any valid dates from the bank statement CSV.")

    # --- Step 3: Define 6-Month Window and Categorize Transactions ---
    six_months_ago = latest_date - timedelta(days=180)

    monthly_income = defaultdict(float)
    monthly_expenditure = defaultdict(float)
    monthly_lowest_balance = defaultdict(lambda: float('inf'))

    for trans in all_transactions:
        date_obj = trans['date']
        if date_obj < six_months_ago:
            continue

        month_key = date_obj.strftime('%Y-%m')

        if trans['balance'] < monthly_lowest_balance[month_key]:
            monthly_lowest_balance[month_key] = trans['balance']

        if trans['credit'] > 0:
            monthly_income[month_key] += trans['credit']
        elif trans['debit'] > 0:
            monthly_expenditure[month_key] += trans['debit']

    
    income_values = list(monthly_income.values())
    expenditure_values = list(monthly_expenditure.values())
    
    income_no_outliers, _ = get_data_without_outliers(income_values)
    expenditure_no_outliers, expenditure_outliers = get_data_without_outliers(expenditure_values)
    print(f"Income: {income_no_outliers}")
    print(f"Expenditure: {expenditure_no_outliers}")

    # Calculate final averages from the cleaned data
    avg_monthly_income = statistics.mean(income_no_outliers) if income_no_outliers else 0.0
    avg_monthly_expenditure = statistics.mean(expenditure_no_outliers) if expenditure_no_outliers else 0.0
    disposable_income = avg_monthly_income - avg_monthly_expenditure 
    
    lowest_balance_values = [v for v in monthly_lowest_balance.values() if v != float('inf')]
    avg_lowest_monthly_balance = statistics.mean(lowest_balance_values) if lowest_balance_values else 0.0
    balance_volatility = statistics.stdev(lowest_balance_values) if len(lowest_balance_values) > 1 else 0.0
    
    return {
        'avgMonthlyIncome': Decimal(str(round(avg_monthly_income, 2))),
        'avgMonthlyExpenditure': Decimal(str(round(avg_monthly_expenditure, 2))),
        'disposableIncome': Decimal(str(round(disposable_income, 2))),
        'avgLowestMonthlyBalance': Decimal(str(round(avg_lowest_monthly_balance, 2))),
        'balanceVolatility': Decimal(str(round(balance_volatility, 2))),
        'expenditureOutlierCount': len(expenditure_outliers),
    }


def analyze_mtn_momo_csv(csv_content, user_id):
    """
    Performs data analysis on the most recent 6 months of transactions from a MoMo statement.
    """
    print(f"Running analysis for MTN MoMo statement for user: {user_id}")
    
    lines = csv_content.splitlines()
    header = None
    data_start_line = 0

    # --- Step 1: Find Data Header ---
    for i, line in enumerate(lines):
        header_keywords = ["TRANSACTION DATE", "TRANS. TYPE", "AMOUNT", "BAL AFTER", "FROM NO.", "TO NO."]
        if all(keyword in line.upper() for keyword in header_keywords):
            header = [h.strip().replace('"', '') for h in line.split(',')]
            data_start_line = i + 1
            break
            
    if not header:
        raise ValueError("Could not find a valid MTN MoMo data header row in the CSV.")

    # --- Step 2: Pre-scan to Find Date Range and Phone Number ---
    all_transactions = []
    latest_date = None
    user_phone_suffix = None
    
    reader_prescan = csv.DictReader(lines[data_start_line:], fieldnames=header)
    for row in reader_prescan:
        date_obj = parse_momo_date(row.get("TRANSACTION DATE"))
        if date_obj:
            all_transactions.append((date_obj, row))
            if latest_date is None or date_obj > latest_date:
                latest_date = date_obj
        
        if not user_phone_suffix:
            trans_type = row.get("TRANS. TYPE", "").upper().strip()
            if trans_type in ["DEBIT", "PAYMENT"]:
                from_phone_raw = row.get("FROM NO.", "").strip()
                if from_phone_raw:
                    from_phone_cleaned = re.sub(r'\D', '', from_phone_raw)
                    if from_phone_cleaned:
                        user_phone_suffix = from_phone_cleaned[-9:]

    if not user_phone_suffix:
        raise ValueError("Could not dynamically identify user's phone number.")
    if not latest_date:
        raise ValueError("Could not find any valid transaction dates in the statement.")

    # --- Step 3: Define 6-Month Window and Categorize Transactions ---
    six_months_ago = latest_date - timedelta(days=180)
    print(f"Analysis window: {six_months_ago.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")

    monthly_income = defaultdict(float)
    monthly_expenditure = defaultdict(float)
    monthly_lowest_balance = defaultdict(lambda: float('inf'))

    for date_obj, row in all_transactions:
        if date_obj < six_months_ago:
            continue

        month_key = date_obj.strftime('%Y-%m')
        balance_after = clean_numeric(row.get("BAL AFTER"))
        if balance_after < monthly_lowest_balance[month_key]:
            monthly_lowest_balance[month_key] = balance_after
        
        trans_type = row.get("TRANS. TYPE", "").upper().strip()
        to_phone_cleaned = re.sub(r'\D', '', row.get("TO NO.", "").strip())
        amount = clean_numeric(row.get("AMOUNT"))
        
        is_income = (
            to_phone_cleaned and 
            to_phone_cleaned.endswith(user_phone_suffix)
        )
        
        if is_income:
            monthly_income[month_key] += amount
        else:
            monthly_expenditure[month_key] += amount

    # --- Step 4: Calculate Final Metrics ---
    income_values = list(monthly_income.values())
    expenditure_values = list(monthly_expenditure.values())
    print(income_values)
    print(expenditure_values)
    
    income_no_outliers, _ = get_data_without_outliers(income_values)
    expenditure_no_outliers, expenditure_outliers = get_data_without_outliers(expenditure_values)
    print(income_no_outliers)
    print(expenditure_no_outliers)
    avg_monthly_income = statistics.mean(income_no_outliers) if income_no_outliers else 0.0
    avg_monthly_expenditure = statistics.mean(expenditure_no_outliers) if expenditure_no_outliers else 0.0
    print(avg_monthly_income)
    print(avg_monthly_expenditure)

    lowest_balance_values = [v for v in monthly_lowest_balance.values() if v != float('inf')]
    avg_lowest_monthly_balance = statistics.mean(lowest_balance_values) if lowest_balance_values else 0.0
    
    disposable_income = avg_monthly_income - avg_monthly_expenditure
    print(disposable_income)
    balance_volatility = statistics.stdev(lowest_balance_values) if len(lowest_balance_values) > 1 else 0.0
    
    return {
        'avgMonthlyIncome': Decimal(str(round(avg_monthly_income, 2))),
        'avgMonthlyExpenditure': Decimal(str(round(avg_monthly_expenditure, 2))),
        'disposableIncome': Decimal(str(round(disposable_income, 2))),
        'avgLowestMonthlyBalance': Decimal(str(round(avg_lowest_monthly_balance, 2))),
        'balanceVolatility': Decimal(str(round(balance_volatility, 2))),
        'expenditureOutlierCount': len(expenditure_outliers),
    }

# --- Main Lambda Handler (Router) ---

def lambda_handler(event, context):
    """
    This Lambda is triggered by S3. It routes to the correct analysis function
    and appends or updates the results in the 'perStatement' list in DynamoDB.
    """
    for record in event['Records']:
        try:
            source_bucket = record['s3']['bucket']['name']
            source_key = record['s3']['object']['key']
            
            parts = source_key.split('/')
            if len(parts) < 3:
                raise ValueError(f"Invalid S3 key format: {source_key}")
            
            statement_type = parts[1]
            user_id = parts[2]
            file_name = os.path.basename(source_key)

            print(f"Routing analysis for statement: {statement_type} for user: {user_id}")

            response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
            content = response['Body'].read().decode('utf-8-sig')
            
            metrics_data = None
            if 'momo-mtn-statement' in statement_type:
                metrics_data = analyze_mtn_momo_csv(content, user_id)
            elif 'bank' in statement_type:
                metrics_data = analyze_bank_statement_csv(content, user_id)
            else:
                raise ValueError(f"No analyzer for type: {statement_type}")

            new_metric_item = {
                'id': file_name, # Unique ID for the statement analysis
                'sourceFile': source_key,
                'statementType': statement_type,
                'analysisDate': datetime.utcnow().isoformat()
            }
            new_metric_item.update(metrics_data)
            
            print(f"Analysis complete. Metrics: {new_metric_item}")

            # --- Update or Append Logic ---
            # 1. Get the current user profile
            user_profile = table.get_item(Key={'userId': user_id}).get('Item', {})
            # Get the existing statementMetrics object, or create one if it doesn't exist
            statement_metrics = user_profile.get('statementMetrics', {'perStatement': []})
            per_statement_list = statement_metrics.get('perStatement', [])

            # 2. Check if an item with the same ID exists
            found_index = -1
            for i, item in enumerate(per_statement_list):
                if item.get('id') == file_name:
                    found_index = i
                    break
            
            # 3. Update the list in memory
            if found_index != -1:
                print(f"Updating existing statement metric at index {found_index}")
                per_statement_list[found_index] = new_metric_item
            else:
                print("Appending new statement metric")
                per_statement_list.append(new_metric_item)
            
            # 4. Update the main statement_metrics object with the modified list
            statement_metrics['perStatement'] = per_statement_list
            
            # 5. Save the entire updated statementMetrics object back to DynamoDB
            table.update_item(
                Key={'userId': user_id},
                UpdateExpression="SET #sm = :new_metrics_object",
                ExpressionAttributeNames={'#sm': 'statementMetrics'},
                ExpressionAttributeValues={':new_metrics_object': statement_metrics}
            )
            print(f"Successfully saved analysis for user {user_id}.")

        except Exception as e:
            print(f"ERROR processing record: {e}")
            
    return {
        'statusCode': 200,
        'body': json.dumps('CSV analysis batch finished.')
    }
