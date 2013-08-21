 `curl -i -XPOST dbserver02.cs.washington.edu:8759/dataset/importDataset -H "Content-type: application/json"  -d @./importRankingTable.json`
  `curl -i -XPOST dbserver02.cs.washington.edu:8759/dataset/importDataset -H "Content-type: application/json"  -d @./importUserVisitsTable.json`
 `curl -i -XPOST dbserver02.cs.washington.edu:8759/query -H "Content-type: application/json"  -d @./broadcast_join_8_workers_16x.json`
 `curl -i dbserver02.cs.washington.edu:8759/server/shutdown`
 `curl -i dbserver02.cs.washington.edu:8759/workers/alive`