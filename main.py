# copying cell content from dev.ipynb (untracked in git)

# module load miniconda
# mamba activate myenv

# **Task1**  
# Create 3 functions to read 3 types of files from the following directory  
# `/projectnb/rcsmetrics/accounting/data/scc`  
# The names for the 2025 file(s) can be taken from documentation:  
# `man accounting`  

import csv

def read_csv(filepath):
    """Reads a CSV file and returns its content via a generator.
    This is due to the large size of these files.
    https://docs.python.org/3/library/csv.html#csv.DictReader"""
    with open(filepath, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            yield row



# Example usage
csv_data = read_csv("/projectnb/rcsmetrics/accounting/data/scc/2016.csv")

# Print first five rows
c = 0
for i in csv_data:
    if c > 3:
        break
    print(i)
    c+=1




# ~10 seconds
# %time from pandas import read_csv as read_csv_pandas



# Example usage
csv_data = read_csv_pandas("/projectnb/rcsmetrics/accounting/data/scc/2016.csv")

# Print first five rows
print(csv_data.head(5))




# note pyarrow req. Takes ~1 sec to load, though
# %time import pyarrow.feather as feather




def read_feather(filepath):
    """Reads a Feather file and returns it as a PyArrow Table."""
    return feather.read_table(filepath)

# Example usage
feather_data = read_feather("/projectnb/rcsmetrics/accounting/data/scc/2016.feather")
print(feather_data.nbytes)




import os


dir = "/projectnb/rcsmetrics/accounting/data/scc"
print(os.listdir(dir)) # contains csv, feather, and dir?

# csv_headers = ['qname', 'hostname', 'owner', 'job_name', 'job_number', 'ux_submission_time', 'ux_start_time', 'ux_end_time', 'failed', 'exit_status', 'ru_wallclock', 'ru_utime', 'ru_maxrss', 'project', 'granted_pe', 'slots', 'task_number', 'cpu', 'options', 'pe_taskid', 'maxvmem', 'n_gpu']




# **Task2**  
# Once this is done, create a function that reads only GPU-related records.  
# This may not work with the feather file (as it is binary file), but should work with  
# 2025 and 2025.csv files. You can use grep command:  
# `grep -e “-l gpus=" <filename>`


def read_gpu_records_csv(filepath):
    """Reads only GPU-related records from a CSV file line by line."""
    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            if 'gpus=' in line: # note task says, -l gpus=, might need regex for whitespacing?
                yield line.strip()  # Yield matching lines one by one

# Example usage
for i, record in enumerate(read_gpu_records_csv("/projectnb/rcsmetrics/accounting/data/scc/2016.csv")):
    if i >= 5:  # Print only first 5 matches
        break
    print(record)


# try loading into memory entire file
all_lines = [i for i in read_gpu_records_csv("/projectnb/rcsmetrics/accounting/data/scc/2024.csv")]





import subprocess

def read_gpu_records_with_grep(filepath):
    """Uses grep to filter and return GPU-related records from the file."""
    try:
        # Run grep command to find lines containing 'gpus='
        result = subprocess.run(
            ['grep', '-e', 'gpus=', filepath],
            text=True, capture_output=True, check=True
        )
        # Split the output into a list of lines and return
        return result.stdout.splitlines()
    except subprocess.CalledProcessError:
        print(f"No matching lines found in {filepath}")
        return []

# Example usage
gpu_records = read_gpu_records_with_grep("/projectnb/rcsmetrics/accounting/data/scc/2025")
for line in gpu_records[:5]:  # Print the first 5 GPU-related records
    print(line)




# **Task3**  
# /project/scv/dugan/gpustats/data  
# Columns: Time, Bus, Util , Memory_throughput, user, proj, jobID.taskIID  
# If GPU is not assigned to a job: the record will contain dashes  
# The third scenario: the job is assigned, but the user name and projectname are still “-“, only JobID. Change “-“ to Missing Values in Python  
# Very occasionally, the JobID is put in the wrong column . Job ID should be in the 7th column, but sometimes it will be in the 5th column. Assume,   
# there should be dashes. This should be treated as scenario #2.  

# THIS NEEDS TO BE TESTED!!! Scenarios may need to be slightly clarified.




def clean_gpu_data(filepath):
    """Reads and processes GPU usage data while handling missing values and misaligned JobID."""
    cleaned_data = []

    with open(filepath, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.split()
            
            scenario = 1
            time, bus, util, mem_throughput = parts[:4]  # First 4 columns

            # Scenario 3: If job_id appears in column 5 (user column), shift values
            if len(parts) == 5: 
                user, proj = "Missing Values", "Missing Values"
                job_id = parts[4]
                scenario = 3
            else:
                user, proj, job_id = parts[4:7]  # Remaining columns
            

            # Scenario 2: If user and project are "-", but job_id exists, replace "-" with "Missing Values"
            if user == "-" and proj == "-" and job_id != "-":
                user, proj = "Missing Values", "Missing Values"
                scenario = 2

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
gpu_records = clean_gpu_data("/project/scv/dugan/gpustats/data/scc-201/2402")
for record in gpu_records[:5]:  # Print first 5 cleaned records
    print(record)


import pandas as pd
df = pd.DataFrame(gpu_records)
df.replace("-", pd.NA, inplace=True)
df.replace("Missing Values", pd.NA, inplace=True)

# df

df['job_id_split'] = df['job_id'].str.split('.').str[0]
# df


# need to join other file on this to get user, project
df_s2 = df[df['scenario'] == 2]


with open("/projectnb/rcsmetrics/accounting/data/scc/2024.csv", 'r', encoding='utf-8') as file:
    for line in file:
        headers = line.split(',')
        break

headers[-1] = headers[-1][:-1]
headers




with open("/projectnb/rcsmetrics/accounting/data/scc/2024.csv", 'r', encoding='utf-8') as file:
    c= 0
    for i in file:
        if c==2:
            break
        print(i)
        c+=1



gpu_records = read_gpu_records_with_grep("/projectnb/rcsmetrics/accounting/data/scc/2024.csv")



from io import StringIO



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

zzz = read_gpu_records("/projectnb/rcsmetrics/accounting/data/scc/2024.csv")



pd.set_option('display.max_columns', None)
zzz


necessary['options'].str.contains('-t').sum()



# need to join on job_number/job_id[0] to fill in missing user info
necessary = zzz[["qname", "owner", "job_number","task_number","options", "n_gpu"]]

necessary['task_string'] = necessary['task_number'].astype(str)
necessary[necessary['options'].str.contains('-t')]['task_string'] = ".undefined"

necessary['job_task'] = necessary['job_number'].astype(str) + '.' + necessary['task_string'].astype(str)



necessary['job_number'] = necessary['job_number'].astype(str)
necessary




zzz[zzz['job_number']==3366863].head(50)
# zzz.head(5)





df_test = df
df_test.fillna('-',inplace=True)
df_test




merged_df = pd.merge(df, necessary, left_on='job_id_split', right_on='job_number', how='left')
merged_df