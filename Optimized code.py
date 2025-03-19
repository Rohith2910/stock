import json
import boto3
import pandas as pd
import requests
from io import StringIO
import consonants as constant
from botocore.exceptions import BotoCoreError, NoCredentialsError
import logging
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
sns = boto3.client("sns", region_name="eu-north-1")
ssm = boto3.client("ssm", region_name="eu-north-1")
s3 = boto3.client('s3')
def send_sns_success():
    try:
        success_sns_arn = ssm.get_parameter(Name=constant.SUCCESSNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
        print("SARN-Rasagna",success_sns_arn)
        component_name = constant.COMPONENT_NAME
        env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
        success_msg = constant.SUCCESS_MSG
        sns_message = f"{component_name} : {success_msg}"
        logger.info(f"Sending SNS Success Message: {sns_message}")
        succ_response = sns.publish(TargetArn=success_sns_arn,Message=json.dumps({'default': sns_message}),Subject=f"{env} : {constant.COMPONENT_NAME}",MessageStructure="json")
        return succ_response
    except Exception as sns_error:
        logger.error("Failed to send success SNS", exc_info=True) 
def send_error_sns(error_message):
    try:
        error_sns_arn = ssm.get_parameter(Name=constant.ERRORNOTIFICATIONARN, WithDecryption=True)["Parameter"]["Value"]
        env = ssm.get_parameter(Name=constant.ENVIRONMENT, WithDecryption=True)['Parameter']['Value']
        #error_message = constant.ERROR_MSG
        component_name = constant.COMPONENT_NAME
        sns_message = f"{component_name} : {error_message}"
        logger.error(f"Sending SNS Error Message: {sns_message}")
        err_response = sns.publish(TargetArn=error_sns_arn,Message=json.dumps({'default': sns_message}),Subject=f"{env} : {constant.COMPONENT_NAME}",MessageStructure="json")
        return err_response
    except Exception as sns_error:
        logger.error("Failed to send error SNS", exc_info=True)

def get_api_url():
    """Fetch API URL from AWS SSM Parameter Store."""
    try:
        return ssm.get_parameter(Name=constant.urlapi, WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        print(f"Error fetching API URL: {str(e)}")
        emsg= str(e)
        print("testing#######", emsg)
        send_error_sns(emsg)
        raise
              
def fetch_api_data_with_retries(url, max_retries=3, wait_time=5):
    """Fetch API data with retries using a while loop and sleep method."""
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(url, timeout=10)
            #response.raise_for_status()
            return response.json()  # Success, return JSON data
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempts + 1}/{max_retries}: API request failed - {e}")
            attempts += 1
            if attempts < max_retries:
                time.sleep(wait_time)  # Wait before retrying
            else:
                logger.error("Max retries reached. API request failed.")
                send_error_sns(f"API request failed after {max_retries} attempts.")
                raise  # Raise error if max retries exceeded        

def extract_api_data():
    """Extracts and processes CSV data from the API."""
    try:
        url = get_api_url()
        files = fetch_api_data_with_retries(url)  # API request with retry logic
        s3 = boto3.client('s3')
    # print(files)
        csv_files = [file['download_url'] for file in files if file['name'].endswith('.csv')]
        a=csv_files[0]
        file_name = a.split("/")[-1].replace(".csv", "")
        csv_file = csv_files.pop()
        d = pd.read_csv(csv_file)
        #print(d)        
        dataframes=[]
        file_names=[]
        for url in csv_files:            
            file_name = url.split("/")[-1].replace(".csv", "")
            df = pd.read_csv(url)
            df['Symbol'] = file_name
            dataframes.append(df)
            file_names.append(file_name)
        combined_df = pd.concat(dataframes, ignore_index=True)
        o_df = pd.merge(combined_df,d,on='Symbol',how='left')
        result = o_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
        o_df["timestamp"] = pd.to_datetime(o_df["timestamp"])
        filtered_df = o_df[(o_df['timestamp'] >= "2021-01-01") & (o_df['timestamp'] <= "2021-05-26")]
        result_time = filtered_df.groupby("Sector").agg({'open':'mean','close':'mean','high':'max','low':'min','volume':'mean'}).reset_index()
        list_sector = ["TECHNOLOGY","FINANCE"]
        df = result_time[result_time["Sector"].isin(list_sector)].reset_index(drop=True)
        # print("print#########")
        # print(df)
    except Exception as e:
        print(f"Lambda function error: {str(e)}")
        send_error_sns(str(e))
        print("Error mail has been sent")
    return df   
  
   
def save_to_s3(df, bucket_name, s3_path):
    """Saves a Pandas DataFrame to an S3 bucket as a CSV file."""
    try:    
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, header=True, index=False)
        s3.put_object(Bucket=bucket_name, Key=s3_path, Body=csv_buffer.getvalue())
        print(f"Data successfully saved to S3: {bucket_name}/{s3_path}")
    except (BotoCoreError, NoCredentialsError) as e:
        print(f"S3 Upload Error: {str(e)}")
        raise

def lambda_handler(event, context):
    """AWS Lambda handler function."""
    try:        
        bucket_name = 'rasagna-1090'
        s3_path = 'output/result.csv'
        df = extract_api_data()
        save_to_s3(df, bucket_name, s3_path)
        send_sns_success()
        print("Success mail has been sent.")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Data saved to {bucket_name}/{s3_path}')
        }

    except Exception as e:
        print(f"Lambda function error: {str(e)}")
        send_error_sns(str(e))
        print("Error mail has been sent.")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
