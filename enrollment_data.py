import requests
import csv
import boto3
from io import StringIO

# S3 client
s3_client = boto3.client('s3')

# S3 bucket name and folder path
BUCKET_NAME = 'pre-k-data-enrollement'  
S3_FOLDER = 'data/' 

# API endpoint
API_URL_TEMPLATE = "https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/state/{grade}.json"

# Parameters, Pre-K grade and Years to fetch
GRADE = "grade-pk"  
YEARS = [2020, 2021, 2022]  

# fetch data from  API
def fetch_enrollment_data(year, grade):
    url = API_URL_TEMPLATE.format(year=year, grade=grade)
    data = []

    while url:
        response = requests.get(url)
        if response.status_code == 200:
            json_data = response.json()
            data.extend(json_data['results'])  
            url = json_data.get('next')  
        else:
            print(f"Failed to fetch data for year {year}: {response.status_code}")
            url = None  # Stop fetching if there is an error
    return data

# Function to extract relevant data from the API response
def extract_data(records):
    extracted = []
    for record in records:
        state_name = record.get('fips', 'Unknown')  # FIPS as state
        year = record.get('year', 'Unknown')
        ncessch = record.get('ncessch', 'Unknown')
        ncessch_num = record.get('ncessch_num', 'Unknown')
        grade = record.get('grade', 'Unknown')
        race = record.get('race', 'Unknown')
        sex = record.get('sex', 'Unknown')
        enrollment = record.get('enrollment', 0)
        leaid = record.get('leaid', 'Unknown')
        
        extracted.append([
            state_name, year, ncessch, ncessch_num, grade, race, sex, enrollment, leaid
        ])
    return extracted

#upload CSV to S3
def upload_to_s3(data, year):
    # Convert data to CSV format
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow([
        "State", "Year", "NCES_School_ID", "NCES_School_Num", "Grade", "Race", "Sex", "Enrollment", "LEA_ID"
    ])  # Header row
    writer.writerows(data)
    
    # Define S3 object key (path)
    s3_key = f"{S3_FOLDER}pre_k_enrollment_{year}.csv" 
    
    # Upload CSV to S3
    s3_client.put_object(
        Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue()
    )
    print(f"Uploaded data to S3: {s3_key}")

# function to fetch and upload data for all years
def allyears(event, context):
    for year in YEARS:
        json_data = fetch_enrollment_data(year, GRADE)
        if json_data:
            extracted_data = extract_data(json_data)
            upload_to_s3(extracted_data, year)
            print(f"Data for {year} uploaded to S3")
