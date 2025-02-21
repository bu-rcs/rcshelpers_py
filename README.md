# RCS Helpers Python functions
Python Functions to read "general" SCC-related files

Updates for next meeting:  
- Completed file reading tasks
- Developed pipeline to parse through both file types (accounting, util)
- Handles the three scenarios (expanded to four with no assignee #0)
- Joins user/job info onto util, allowing access to more info
- Currently aggregates accross all nodes for a given year, month
- Displays "leaderboard" information for owners, queues for wasted gpu hours
- Wasted GPU Hours, Fully Wasted GPU Hours metrics:  
`Count of 5 minute chunks * (100-util percent) / 12`  
`Count of 5 minute chunks with 0 util / 12`  
- To demo:
```bash
module load miniconda
mamba activate myenv

python lbdemo.py
```
Can also explore notebook to explore other possibilities and further analysis.  
More breakdowns on ood jobs, etc. within nodebook