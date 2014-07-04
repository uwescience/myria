## Run the experiment
1. Import rank table
    `curl -i -XPOST dbserver02.cs.washington.edu:8759/dataset/importDataset -H "Content-type: application/json"  -d @./importRankingTable.json`
2. Import user visit table
    `curl -i -XPOST dbserver02.cs.washington.edu:8759/dataset/importDataset -H "Content-type: application/json"  -d @./importUserVisitsTable.json`
3. Submit broadcast join query
    `curl -i -XPOST dbserver02.cs.washington.edu:8759/query -H "Content-type: application/json"  -d @./broadcast_join.json`
 
## Check the status of workers
    `curl -i dbserver02.cs.washington.edu:8759/workers/alive`
