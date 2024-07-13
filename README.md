# AWS Lambda S3: to Parquet Converter
This AWS Lambda function reads CSV files from an S3 bucket whenever added, converts them to Parquet format, and updates the AWS Glue catalog (if not created). 


## Setup Instructions

### 1. Create IAM Policy
Create an IAM role with the following properties:

1. Trusted Entity: AWS Lambda
2. Permissions Policy: 
    - create a custom policy from the [lambda_policy.txt](lambda_policy.txt) file in this repository.
    - IMPORTANT: Replace "INITIALS" with your actual bucket names in the policy.

4. Name the role (e.g., "DataEngLambdaS3CWGlueRole") and create it.

3. Role Name: Choose a descriptive name like "LambdaExecutionRole"



#### 2. Create the IAM role:
1. In IAM, go to "Roles" and click "Create role."
2. Choose "Lambda" as the service.
3. Attach your custom policy (e.g., "DataEngLambdaS3CWGluePolicy").
4. Name the role (e.g., "DataEngLambdaS3CWGlueRole") and create it.



## Setup the Lambda Layer
Download the AWS Wrangler layer: https://github.com/aws/aws-sdk-pandas/releases/download/3.9.0/awswrangler-layer-3.9.0-py3.10.zip


## Setup the AWS Lambda Function
1. Create a Lambda function in the AWS Lambda console.
2. Select the IAM role created earlier.
3. Add the Lambda layer created earlier.
4. Copy and deploy the Python code.
5. Add an S3 trigger with a .csv suffix filter to the Lambda function.


## How it Works
1. Trigger: When a new CSV file is added to your S3 bucket, it automatically activates the Lambda function.
2. Conversion: The function reads the CSV file, transforms it into the Parquet format using AWS Data Wrangler, and saves this new Parquet file back into the S3 bucket.
3. Glue Catalog Update: The function checks the AWS Glue Data Catalog:
    - If no table exists: It creates a new table that links to the location of the Parquet file.
    - If a table exists: It updates the table's details to match the new Parquet file.

## Benefits
1. Efficiency: Parquet format is optimized for big data processing, making your data analysis faster.
2. Integration: Your data is readily available for analysis through tools like Amazon Athena.
3. Cost-Effective: Utilizes AWS Lambda's serverless model, so you only pay for the compute time used during conversion.

## Acknowledgements
This repo was inspired by the work done in [Data Engineering with AWS, Second Edition] by Gareth Eagar.

