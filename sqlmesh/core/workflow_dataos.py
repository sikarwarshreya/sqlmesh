import os
import yaml
import re
import click

def sanitize_filename(cron_expression):
    """Replace special characters in cron expressions to make a valid filename"""
    return re.sub(r'[^a-zA-Z0-9_]', '_', cron_expression)

def categorize_cron_jobs(cron_list):
    every_x_minutes = []
    specific_time_crons = []
    separate_recurring = []
    has_daily = False
    
    for cron in cron_list:
        parts = cron.split()
        
        # Handling special cases like '@daily', '@monthly'
        if cron.startswith('@'):
            if cron == '@daily':
                has_daily = True  # Mark that @daily is present
            else:
                specific_time_crons.append(cron)
            continue
        
        # Checking if it's an 'every X minutes' cron
        if parts[0].startswith('*/'):
            try:
                x = int(parts[0][2:])
                if x < 30:
                    every_x_minutes.append(f"*/{x} * * * *")
                else:
                    separate_recurring.append(cron)
            except ValueError:
                specific_time_crons.append(cron)
        else:
            specific_time_crons.append(cron)
    
    # Determine the smallest interval for merging crons with X < 30
    merged_cron_list = []
    if every_x_minutes or has_daily:
        if has_daily:
            merged_cron_list.append("@daily")
        if every_x_minutes:
            min_interval = min(int(cron.split()[0][2:]) for cron in every_x_minutes)
            merged_cron_list.append(f"*/{min_interval} * * * *")
    
    result = {
        "specific_time_crons": specific_time_crons,
        "separate_recurring": separate_recurring,
        "merged_recurring": merged_cron_list
    }
    
    return result


import os
import yaml
import click

def create_wf(path, cron_expression):
    """Creates a workflow YAML file in the specified path based on the given cron expression"""
    safe_cron_expr = sanitize_filename(cron_expression)
    print(safe_cron_expr)
    # Define the YAML content
    data = {
        "version": "v1",
        "name": f"wf-sqlmesh-sync-{safe_cron_expr}",
        "type": "sqlmesh",
        "workflow": {
            "cron": cron_expression,
            "dag": [
                {
                    "name": "dg-sqlmesh-sync",
                    "spec": {
                        "stack": "sqlmesh",
                        "tempVolume": "10Gi",
                        "job": {
                            "explain": True,
                            "inputs": [
                                {"name": "sqlmesh_input", "input": path}
                            ],
                            "logLevel": "INFO",
                            "outputs": [
                                {
                                    "name": "output",
                                    "dataset": f"{path}/config.yaml",
                                    "format": "iceberg",
                                    "description": "The dataset contains Adobe 2024 search data",
                                    "options": {"saveMode": "append"}
                                }
                            ],
                            "steps": [
                                "sqlmesh plan dev",
                                "sqlmesh ui"
                            ]
                        }
                    }
                }
            ]
        }
    }

    # Ensure the directory exists
    os.makedirs(path, exist_ok=True)

    # Define file path
    file_path = os.path.join(path, f"workflow_{safe_cron_expr}.yaml")

    # Write the YAML file
    with open(file_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False)

    click.echo(f"Workflow YAML file created at {file_path}")

def generate_wfs(path, cron_list):
    categorized_crons = categorize_cron_jobs(cron_list)
    
    # Create workflow for merged minimum cron
    for cron in categorized_crons["merged_recurring"]:
        create_wf(path, cron)
    
    # Create workflows for other crons in separate lists
    for cron in categorized_crons["specific_time_crons"] + categorized_crons["separate_recurring"]:
        create_wf(path, cron)

# # Example usage
# cron_list = ["*/5 * * * *", "@daily", "0 12 * * *"]
# path = "./workflows"
# generate_wfs(path, cron_list)

# # Example usage
# cron_list_1 = ['@daily', '@monthly', '*/28 * * * *', '5 0 * 8 *', '15 14 1 * *']
# cron_list_2 = ['0 22 * * 1-5', '@monthly', '*/28 * * * *', '5 0 * 8 *', '15 14 1 * *']

# print(categorize_cron_jobs(cron_list_1))
# print(categorize_cron_jobs(cron_list_2))
