# module load miniconda
# mamba activate myenv
from io import StringIO
import pandas as pd
import subprocess

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

# Example usage
gpu_records_2402 = clean_gpu_data("/project/scv/dugan/gpustats/data/scc-201/2402")
# for record in gpu_records_2402[:50000]:  # Print first 5 cleaned records
#     print(record)

gpu_records_2402 = pd.DataFrame(gpu_records_2402)

gpu_records_2402_scenario = gpu_records_2402.loc[gpu_records_2402['scenario'] != 0]


# gpu_jobs_2024.fillna('-', inplace=True)
gpu_jobs_2024['task_string'] = gpu_jobs_2024['task_number'].astype(str)
gpu_jobs_2024.loc[~(gpu_jobs_2024['options'].str.contains('-t')), 'task_string'] = "undefined"
gpu_jobs_2024['job_task'] = gpu_jobs_2024['job_number'].astype(str) + '.' + gpu_jobs_2024['task_string'].astype(str)

# useful_columns = ['qname', 'hostname', 'owner', 'job_name', 'job_number', '']
merged_df = pd.merge(gpu_records_2402_scenario, gpu_jobs_2024, left_on='job_id', right_on='job_task', how='left')


owner_stats = merged_df.groupby('owner').agg(
    mean_utilization=('util', 'mean'),
    count=('util', 'count')
).reset_index()
owner_stats['lb'] = (1-(owner_stats['mean_utilization']/100)) * owner_stats['count']

print(owner_stats.sort_values(by='lb', ascending=False).head(10))