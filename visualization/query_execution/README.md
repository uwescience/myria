Query plan execution visualization
===================================
## Running time of workers
1. Retrieve information from logs
    * Use `./collect_worker_running_time.py <log_files_directory> <query_id>` to extract query execution data from log. The output is in JSON format.
        * `<log_files_directory>`: the directory contains all worker logs. Use `../../myriadeploy/get_logs.py` to collect all logs directly.
        * `<log_file_path>`: the interested query id.
2. Visualization
    * Open `worker_time.html` in a browser and select the .csv file. You could try the example file `worker_running_time_example.csv` to see how the visualizaiton is. 
   
## Gantt chart of query plan on one worker
1. Retrieve information from logs
    * Use `./performance_profile.py <log_file_path> <query_id>` to extract query execution data from log. The output is in JSON format.
2. Visualization
    * Open `query_plan_execution.html` in a browser and select the JSON file. You could try the example file `query_execution_example.json` to see how the visualizaiton is. 

