import boto3
from datetime import datetime
import pandas as pd
import yaml
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)

ce_client = boto3.client('ce')

def create_df_with_group_by(response):
    print("in new fnc")
    data = []
    for result in response['ResultsByTime']:
        time_period = result['TimePeriod']
        usage_amount = next((group['Metrics']['UnblendedCost']['Amount'] for group in result['Groups'] if 'Usage' in group['Keys']), '0')
        credit_amount = next((group['Metrics']['UnblendedCost']['Amount'] for group in result['Groups'] if 'Credit' in group['Keys']), '0')
        
        data.append({
            'TimePeriod.Start': time_period['Start'],
            'TimePeriod.End': time_period['End'],
            'usage_Amount': usage_amount,
            'credit_amount': credit_amount
        })
    df = pd.DataFrame(data)
    return df

def df_to_csv(df,folder, filename ):
    folder = os.path.join('..', folder)
    os.makedirs(folder, exist_ok=True)  
    csv_file_path = os.path.join(folder, filename)
            
    
    
    df.to_csv(csv_file_path, index=False)

    print(f'DataFrame saved to {csv_file_path}')
def run_query(query_name, query_filters):
    # Update time period in filters
    query_filters['query']['TimePeriod']['Start'] = query_filters['query']['TimePeriod']['Start'].strftime('%Y-%m-%d')
    query_filters['query']['TimePeriod']['End'] = query_filters['query']['TimePeriod']['End'].strftime('%Y-%m-%d')

    print(query_filters['query'])
    # Make the request to Cost Explorer
    response = ce_client.get_cost_and_usage(**query_filters['query'])

    if query_name =='trend_month_by_month_with_credits_and_usage':
        
        
        df = create_df_with_group_by(response)
        
    
    elif query_name =='top_services_by_cost_for_3_months':
        df = pd.DataFrame()
        
    
    else:

        df = pd.json_normalize(response['ResultsByTime'])

    # Print DataFrame
    print("\n")
    print(f"Query: {query_name}")
    print(df)
    print("\n")
    print(f"Query: {query_name}")
    df_to_csv(df, 'reports',query_name+'.csv')

if __name__ == "__main__":
    # Load filters from YAML file
    with open('../filters/filter_config.yaml', 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    # Iterate through each query in the YAML file
    for query_name, query_filters in config['queries'].items():
        run_query(query_name, query_filters)
