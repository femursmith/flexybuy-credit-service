import json
import boto3
import os
import csv
from io import StringIO
import fitz  # PyMuPDF library

# --- Configuration ---
DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')


# --- AWS Client Initialization ---
s3_client = boto3.client('s3')

# --- Validation and Fraud Detection Functions ---

def perform_common_validation(doc):
    """
    Performs universal checks that apply to all statement types.
    This runs before any provider-specific validation.
    """
    if doc.page_count == 0:
        raise ValueError("Validation FAIL: PDF document has no pages.")
    
    has_tables = any(page.find_tables().tables for page in doc)
    if not has_tables:
        raise ValueError("Validation FAIL: No tables were found in the document.")
    
    print("Common validation PASSED.")
    return True

def validate_mtn_momo_statement(doc, user_id_from_key):
    """
    Performs specific validation and fraud checks for an MTN Mobile Money statement.
    """
    try:
        first_page_text = doc[0].get_text()
        if user_id_from_key not in first_page_text:
            raise ValueError(f"Validation FAIL: MSISDN for user '{user_id_from_key}' not found on the first page.")
            
        print(f"Specific validation PASS for {user_id_from_key}: MSISDN match confirmed.")
        return True
        
    except Exception as e:
        raise ValueError(f"MTN-specific validation failed: {e}")


# --- Main Processing Logic ---

def handle_statement(local_pdf_path, statement_type, user_id):
    """
    Main processing function that runs a multi-step validation process
    before converting the document.
    """
    doc = fitz.open(local_pdf_path)
    
    perform_common_validation(doc)

    #if statement_type == 'momo-mtn-statement':
    #    validate_mtn_momo_statement(doc, user_id)
    #else:
    #    print(f"No specific validator for statement type '{statement_type}'. Skipping specific validation.")

    all_table_data = []
    for page_num, page in enumerate(doc):
        table_objects = page.find_tables()
        if table_objects.tables:
            if page_num > 0:
                all_table_data.append([f"--- Page {page_num + 1} ---"])
            for table in table_objects.tables:
                table_data = table.extract()
                if table_data:
                    all_table_data.extend(table_data)
                    all_table_data.append([]) 

    string_io = StringIO()
    writer = csv.writer(string_io)
    writer.writerows(all_table_data)
    return string_io.getvalue()


# --- Main Lambda Handler ---

def lambda_handler(event, context):
    """
    This Lambda is triggered by SQS. It parses the S3 key to determine the
    statement type, performs validation, and then converts the PDF to CSV.
    If it fails, it raises an exception to let SQS handle the retry/DLQ process.
    """
    if not DESTINATION_BUCKET:
        raise ValueError("Destination S3 bucket not configured.")

    for record in event['Records']:
        source_bucket, source_key, local_pdf_path = "", "", ""
        try:
            sqs_body = json.loads(record['body'])
            s3_info = sqs_body['Records'][0]['s3']
            source_bucket = s3_info['bucket']['name']
            source_key = s3_info['object']['key']

            parts = source_key.split('/')
            if len(parts) < 4:
                raise ValueError(f"Invalid S3 key format: {source_key}")
            
            statement_type = parts[1]
            user_id = parts[2]
            
            print(f"Processing {statement_type} for user {user_id} from s3://{source_bucket}/{source_key}")

            local_pdf_path = f"/tmp/{os.path.basename(source_key)}"
            s3_client.download_file(source_bucket, source_key, local_pdf_path)
            
            final_csv_content = handle_statement(local_pdf_path, statement_type, user_id)

            output_key = f"processed/{statement_type}/{user_id}/{os.path.splitext(os.path.basename(source_key))[0]}.csv"
            s3_client.put_object(
                Bucket=DESTINATION_BUCKET,
                Key=output_key,
                Body=final_csv_content,
                ContentType='text/csv'
            )
            print(f"Successfully validated and uploaded CSV to: s3://{DESTINATION_BUCKET}/{output_key}")
            
        except Exception as e:
            
            print(f"ERROR: Failed to process {source_key}. Reason: {str(e)}")
            raise e
        
        finally:
            if local_pdf_path and os.path.exists(local_pdf_path):
                os.remove(local_pdf_path)
            
    return {
        'statusCode': 200,
        'body': json.dumps('PDF processing batch finished successfully.')
    }
