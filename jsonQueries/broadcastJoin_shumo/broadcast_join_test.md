 `curl -i -XPOST dbserver02.cs.washington.edu:8759/dataset/importDataset -H "Content-type: application/json"  -d @./importRankingTable.json`
 `curl -i -XPOST dbserver02.cs.washington.edu:8759/query -H "Content-type: application/json"  -d @./testJdbcScan.json`
 `curl -i dbserver02.cs.washington.edu:8759/server/shutdown`
 `curl -i dbserver02.cs.washington.edu:8759/workers/alive`