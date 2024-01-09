import json
import boto3
import pandas as pd
from io import StringIO
import yaml

ce_client = boto3.client('ce')
s3 = boto3.client('s3')
bucket_name="cost-explorer-graghical-data-generation"


def generic_report_creation(dict_response, reportName):
    print(reportName)
    print(dict_response)
    var_list = []
    for i in dict_response:
        print(i)

        if "ResultsByTime" == i:
            for j in dict_response[i]:
                start = j["TimePeriod"]["Start"]
                end = j["TimePeriod"]["End"]
                total = j["Total"]
                            
                if  reportName=="trend_month_by_month_with_credits":
                    Amount = total["UnblendedCost"]["Amount"]
                    Unit = total["UnblendedCost"]["Unit"]

                    var_list.append(
                        {
                            "Start": start,
                            "End": end,
                            "Amount": float(Amount),
                            "USD": Unit,
                        }
                    )

                if  reportName=="trend_month_by_month_without_credits":
                    Amount = total["UnblendedCost"]["Amount"]
                    Unit = total["UnblendedCost"]["Unit"]

                    var_list.append(
                        {
                            "Start": start,
                            "End": end,
                            "Amount": float(Amount),
                            "USD": Unit,
                        }
                    )
                    
                if "Groups" in j:
                    for k in j["Groups"]:
                        if (
                            reportName == "top_services_by_cost_for_3_months"
                            or reportName == "last_month_spend_with_credit"
                            or reportName == "charge_type_by_cost_for_3_months"
                            or reportName == "usage_by_cost_for_3_months"
                            or reportName == "linked_account_by_cost_for_3_months"
                            or reportName == "db_engine_by_cost_for_3_months"
                            or reportName == "platform_by_cost_for_3_months"
                            or reportName == "ebs_snapshotUsage_by_cost_for_3_months"
                            or reportName == "purchase_by_cost_for_3_months"
                        ):
                            serviceName = k["Keys"][0]
                            Amount = k["Metrics"]["UnblendedCost"]["Amount"]
                            Unit = k["Metrics"]["UnblendedCost"]["Unit"]

                            var_list.append(
                                {
                                    "Start": start,
                                    "End": end,
                                    "ServiceName": serviceName,
                                    "Amount": float(Amount),
                                    "USD": Unit,
                                }
                            )

                        elif reportName == "s3_spends_by_cost_for_3_months":
                            serviceName = k["Keys"][0]
                            Amount = k["Metrics"]["UnblendedCost"]["Amount"]
                            Unit = k["Metrics"]["UnblendedCost"]["Unit"]
                            s3Amount = k["Metrics"]["UsageQuantity"]["Amount"]
                            s3Unit = k["Metrics"]["UsageQuantity"]["Unit"]
                            var_list.append(
                                {
                                    "Start": start,
                                    "End": end,
                                    "ServiceName": serviceName,
                                    "Amount": float(Amount),
                                    "USD": Unit,
                                    "S3Amount": s3Amount,
                                    "S3Unit": s3Unit,
                                }
                            )

                        elif reportName == "ebs_spends_by_cost_for_3_months":
                            functionality = k["Keys"][0]
                            ebsAmount = k["Metrics"]["UsageQuantity"]["Amount"]
                            Amount = k["Metrics"]["UnblendedCost"]["Amount"]
                            Unit = k["Metrics"]["UnblendedCost"]["Unit"]
                            ebsUnit = k["Metrics"]["UsageQuantity"]["Unit"]
                            var_list.append(
                                {
                                    "Start": start,
                                    "End": end,
                                    "Functionality": functionality,
                                    "Amount": float(Amount),
                                    "USD": Unit,
                                    "EbsAmount": ebsAmount,
                                    "EbsUnit": ebsUnit,
                                }
                            )

                        elif reportName == "regions_by_cost_for_3_months":
                            servregionNameiceName = k["Keys"][0]
                            Amount = k["Metrics"]["UnblendedCost"]["Amount"]
                            Unit = k["Metrics"]["UnblendedCost"]["Unit"]

                            var_list.append(
                                {
                                    "Start": start,
                                    "End": end,
                                    "ServiceName": servregionNameiceName,
                                    "Amount": float(Amount),
                                    "USD": Unit,
                                }
                            )

                        elif reportName == "availbility_zone_by_cost_for_3_months":
                            availbilityZone = k["Keys"][0]
                            Amount = k["Metrics"]["UnblendedCost"]["Amount"]
                            Unit = k["Metrics"]["UnblendedCost"]["Unit"]

                            var_list.append(
                                {
                                    "Start": start,
                                    "End": end,
                                    "AvailbilityZone": availbilityZone,
                                    "Amount": float(Amount),
                                    "USD": Unit,
                                }
                            )

                        elif reportName == "api_operation_by_cost_for_3_months":
                            operation = k["Keys"][0]
                            Amount = k["Metrics"]["UnblendedCost"]["Amount"]
                            Unit = k["Metrics"]["UnblendedCost"]["Unit"]

                            var_list.append(
                                {
                                    "Start": start,
                                    "End": end,
                                    "Operation": operation,
                                    "Amount": float(Amount),
                                    "USD": Unit,
                                }
                            )

    df = pd.DataFrame(var_list)
    csv_buffer = StringIO()
    csv_file_path = f'reports/{reportName}.csv'
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=csv_file_path)
    print(f'CSV file "{csv_file_path}" created successfully.')
    # df.to_csv(f"{reportName}.csv", encoding="utf-8", index=False)


def run_query(query_name, query_filters):
    # Update time period in filters
    query_filters['query']['TimePeriod']['Start'] = query_filters['query']['TimePeriod']['Start'].strftime('%Y-%m-%d')
    query_filters['query']['TimePeriod']['End'] = query_filters['query']['TimePeriod']['End'].strftime('%Y-%m-%d')

    print(query_filters['query'])
    
    response = ce_client.get_cost_and_usage(**query_filters['query'])
    
    generic_report_creation(response, query_name)
    

def creating_csv_from_data(report_data,csv_name):
    
    data = []
    for result in report_data['ResultsByTime']:
        time_period = result['TimePeriod']
        usage_amount = result['Total']['UnblendedCost']['Amount']
        
        data.append({
            'TimePeriod.Start': time_period['Start'],
            'TimePeriod.End': time_period['End'],
            'usage_Amount': usage_amount,
        })
        
    json_data = str(data).replace('\n', '').replace(' ', '').replace("'",'"')
    df = pd.json_normalize(json.loads(json_data))
    csv_buffer = StringIO()
    csv_file_path = 'reports/output.csv'
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=csv_file_path)
    print(f'CSV file "{csv_file_path}" created successfully.')
    



def lambda_handler(event, context):
    print(event)

    ########## Reading Yaml File ########################

    # Load filters from YAML file
    with open('config.yaml', 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    # Iterate through each query in the YAML file
    for query_name, query_filters in config['queries'].items():
        run_query(query_name, query_filters)
    
    
    
    
    
    
    
    
    
