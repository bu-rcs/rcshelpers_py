import csv

def read_blank(filepath):
    """Reads a Grid Engine accounting file and returns a list of dictionaries.

    Args:
        filepath (str): Path to the accounting file.

    Returns:
        list of dict: Each dictionary represents a job entry with headers as keys.

    Raises:
        ValueError: If the number of fields in a line does not match the number of headers.
    
    Example:
        accounting_data = read_accounting_file("/projectnb/rcsmetrics/accounting/data/scc/2016")
        
        # Get the number of jobs processed
        num_jobs = len(accounting_data)
        print(f"Processed {num_jobs} jobs")
    """
    
    headers = [
        "qname", "hostname", "group", "owner", "job_name", "job_number", "account", "priority",
        "submission_time", "start_time", "end_time", "failed", "exit_status", "ru_wallclock", 
        "ru_utime", "ru_stime", "ru_maxrss", "ru_ixrss", "ru_ismrss", "ru_idrss", "ru_isrss", 
        "ru_minflt", "ru_majflt", "ru_nswap", "ru_inblock", "ru_oublock", "ru_msgsnd", 
        "ru_msgrcv", "ru_nsignals", "ru_nvcsw", "ru_nivcsw", "project", "department", 
        "granted_pe", "slots", "task_number", "cpu", "mem", "io", "category", "iow", "pe_taskid", 
        "maxvmem", "arid", "ar_submission_time"
    ]
    
    jobs = []
    
    with open(filepath, mode='r', encoding='utf-8', errors='replace') as file:
        reader = csv.reader(file, delimiter=':')
        for line in reader:
            # Skip comment lines (those starting with #)
            if line[0].startswith('#') or len(line) == 0:
                continue
            
            # Check if the number of fields matches the header count
            if len(line) != len(headers):
                raise ValueError(f"Line does not match expected format: {line}")
            
            # Create a dictionary for the job entry
            job = dict(zip(headers, line))
            jobs.append(job)
    
    return jobs
