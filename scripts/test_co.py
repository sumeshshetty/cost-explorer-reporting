import boto3
from datetime import datetime
import os
import pandas as pd
def get_cost_explorer_data():
    # Set your AWS credentials and region
    region_name = 'ap-south-1'

    # Set the time range
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 30)

    # Create a Cost Explorer client
    ce_client = boto3.client('ce', 
                             region_name=region_name)

    # Define the parameters
    # query = {
    #     'TimePeriod': {
    #         'Start': start_date.strftime('%Y-%m-%d'),
    #         'End': end_date.strftime('%Y-%m-%d')
    #     },
    #     'Granularity': 'MONTHLY',
    #     'Metrics': ['UnblendedCost']
    # }

    query={
        'TimePeriod': {
            'Start': '2023-01-01', 
            'End': '2023-12-30'
            }, 
        'Granularity': 'MONTHLY', 
        'Metrics': ['UnblendedCost'], 
        'Filter': {
            'Not': {
                'Dimensions': {
                    'Key': 'RECORD_TYPE',
                    'Values': ['Credit']
                }
            }
        }    
        
        }

    # Make the request to Cost Explorer
    response = ce_client.get_cost_and_usage(**query)

    # Convert response to Pandas DataFrame
    df = pd.json_normalize(response['ResultsByTime'])
    print(df)
    # Save DataFrame to CSV file
    # df.to_csv('cost_data.csv', index=False)
if __name__ == "__main__":
    get_cost_explorer_data()
