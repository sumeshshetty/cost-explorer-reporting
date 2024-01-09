import json
import boto3
import pandas as pd
from io import StringIO
import yaml
import os

ce_client = boto3.client('ce')


def generic_report_creation(dict_response, reportName,folder):
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
    filename=reportName+".csv"
    folder = os.path.join('..', folder)
    os.makedirs(folder, exist_ok=True)  
    csv_file_path = os.path.join(folder, filename)
    df.to_csv(csv_file_path, index=False)
    print(f'CSV file "{csv_file_path}" created successfully.')
    # df.to_csv(f"{reportName}.csv", encoding="utf-8", index=False)


def run_query(query_name, query_filters,folder):
    # Update time period in filters
    query_filters['query']['TimePeriod']['Start'] = query_filters['query']['TimePeriod']['Start'].strftime('%Y-%m-%d')
    query_filters['query']['TimePeriod']['End'] = query_filters['query']['TimePeriod']['End'].strftime('%Y-%m-%d')

    print(query_filters['query'])
    
    response = ce_client.get_cost_and_usage(**query_filters['query'])
    
    generic_report_creation(response, query_name,folder)
    
 


if __name__ == "__main__":
    # Load filters from YAML file
    with open('../filters/config.yaml', 'r') as yaml_file:
        config = yaml.safe_load(yaml_file)

    # Iterate through each query in the YAML file
    folder = 'reports'
    for query_name, query_filters in config['queries'].items():
        run_query(query_name, query_filters,folder)
    
    
    
    
    
    
    
    
    
