# module load miniconda
# mamba activate myenv
from io import StringIO
import pandas as pd
import subprocess
import os
from tqdm import tqdm
import argparse

def read_gpu_records(filepath):
    """Filters only rows containing 'gpus=' using grep and loads them into a DataFrame."""
    # Run grep command
    result = subprocess.run(["grep", "-e", "gpus=", filepath], capture_output=True, text=True)

    # Get header from first row
    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            header = line.split(',')
            break
    header[-1] = header[-1][:-1] # remove new line char

    # Convert grep output to a file-like object and read into pandas
    df = pd.read_csv(StringIO(result.stdout), names=header, quotechar='"')

    return df

gpu_jobs_2024 = read_gpu_records("/projectnb/rcsmetrics/accounting/data/scc/2024.csv")


def clean_gpu_data(filepath):
    """Reads and processes GPU usage data while handling missing values and misaligned JobID."""
    cleaned_data = []

    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.split()
            
            scenario = 0 # GPU unassigned, unused
            time, bus, util, mem_throughput = parts[:4]  # First 4 columns

            # Scenario 3: If job_id appears in column 5 (user column), shift values
            if len(parts) == 5: 
                user, proj = "Missing Values", "Missing Values"
                job_id = parts[4]
                scenario = 3
            else:
                user, proj, job_id = parts[4:7]  # Remaining columns
            

                # Scenario 2: If user and project are "-", but job_id exists, replace "-" with "Missing Values" (i.e. letting gpu idle)
                if user == "-" and proj == "-" and job_id != "-":
                    user, proj = "Missing Values", "Missing Values"
                    scenario = 2
                # Scenario 1: job_id exists, no user and project (i.e. using gpu)
                elif job_id != "-":
                    scenario = 1

            

            # Append cleaned record
            cleaned_data.append({
                "time": int(time),
                "bus": bus,
                "util": float(util),
                "memory_throughput": float(mem_throughput),
                "user": user,
                "project": proj,
                "job_id": job_id,
                "scenario": scenario
            })

    return cleaned_data

# Inputs
parser = argparse.ArgumentParser()
parser.add_argument("-y", "--year", default="24", type=str, help="Year <2Y>")
parser.add_argument("-m", "--month", default="01", type=str, help="Month <MM>")
args = parser.parse_args()

year = args.year # "25"
month = args.month # "01"

# Fetch accounting info for the year
gpu_jobs = read_gpu_records(f"/projectnb/rcsmetrics/accounting/data/scc/20{year}.csv")
gpu_jobs['task_string'] = gpu_jobs['task_number'].astype(str)
gpu_jobs.loc[~(gpu_jobs['options'].str.contains('-t')), 'task_string'] = "undefined"
gpu_jobs['job_task'] = gpu_jobs['job_number'].astype(str) + '.' + gpu_jobs['task_string'].astype(str)

# Fetch node names from directory
nodes = os.listdir("/project/scv/dugan/gpustats/data/")
files = []

# Collect file paths
for node in nodes:
    for date in os.listdir(f"/project/scv/dugan/gpustats/data/{node}"):
        if date == year + month:
            files.append(f"/project/scv/dugan/gpustats/data/{node}/{date}")

# Store merged dataframes
all_merged_dfs = []

# Process each file
for file_name in tqdm(files, desc="Parsing files"):
    try:
        gpu_records = pd.DataFrame(clean_gpu_data(file_name))
    except Exception as e:
        print(f"Skipping missing or corrupted file: {file_name}")
        continue
    
    gpu_records_scenario = gpu_records.loc[gpu_records['scenario'] != 0]
    merged_df = pd.merge(gpu_records_scenario, gpu_jobs, left_on='job_id', right_on='job_task', how='left')
    
    # Append to list
    all_merged_dfs.append(merged_df)

# Concatenate all dataframes into one big dataframe
final_df = pd.concat(all_merged_dfs, ignore_index=True)

# Aggregate statistics by 'owner'
owner_stats = final_df.groupby('owner').agg(
    mean_utilization=('util', 'mean'),
    count=('util', 'count'),
    zero_util_count=('util', lambda x: (x == 0).sum()),
    zero_util_ratio=('util', lambda x: (x == 0).mean())
).reset_index()

# Compute 'Wasted GPU Hours' metric
owner_stats['Wasted GPU Hours'] = (1 - (owner_stats['mean_utilization'] / 100)) * owner_stats['count'] / 12
owner_stats['Fully Wasted GPU Hours'] = owner_stats['zero_util_ratio'] * owner_stats['count'] / 12

# # Sort by 'Wasted GPU Hours' in descending order
# owner_stats = owner_stats.sort_values(by='Wasted GPU Hours', ascending=False)

# # Output final dataframe
# print(owner_stats)
print('Top Users')
print(owner_stats.sort_values(by='Wasted GPU Hours', ascending=False).head(15))

print('\n\nTop qname')
qname = final_df.groupby('qname').agg(
    mean_utilization=('util', 'mean'),
    count=('util', 'count'),
    zero_util_count=('util', lambda x: (x == 0).sum()),
    zero_util_ratio=('util', lambda x: (x == 0).mean())
).reset_index()

qname['Wasted GPU Hours'] = (1 - (qname['mean_utilization'] / 100)) * qname['count'] / 12
qname['Fully Wasted GPU Hours'] = qname['zero_util_ratio'] * qname['count'] / 12
print(qname.sort_values(by='Wasted GPU Hours', ascending=False).head(15))