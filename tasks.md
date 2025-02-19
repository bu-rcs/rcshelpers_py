**Task1**  
Create 3 functions to read 3 types of files from the following directory  
`/projectnb/rcsmetrics/accounting/data/scc`  
The names for the 2025 file(s) can be taken from documentation:  
`man accounting`  

- Size of files is potentially too large to read in as csv  
- Fastest loading (import and file) was with pyarrow & feather files  
- Didn't complete base file loading  

**Task2**  
Once this is done, create a function that reads only GPU-related records.  
This may not work with the feather file (as it is binary file), but should work with  
2025 and 2025.csv files. You can use grep command:  
`grep -e “-l gpus=" <filename>`  

- Able to load in csv file well by checking for gpus= line by line (using generator)
- Grep works faster
- If this is for a data analysis pipeline, it probably makes most sense to grep and load with pandas

**Task3**  
/project/scv/dugan/gpustats/data  
Columns: Time, Bus, Util , Memory_throughput, user, proj, jobID.taskIID  
If GPU is not assigned to a job: the record will contain dashes  
The third scenario: the job is assigned, but the user name and projectname are still “-“, only JobID. Change “-“ to Missing Values in Python  
Very occasionally, the JobID is put in the wrong column . Job ID should be in the 7th column, but sometimes it will be in the 5th column. Assume,   
there should be dashes. This should be treated as scenario #2.  