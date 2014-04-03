touch "ingest_all_masses.sh"
for VARIABLE in 024 048 072 084 096 128 144 168 192 216 226 240 264 288 312 328 336 384 406 408 432 455 456 466 480 504 512
do
	touch "ingest${VARIABLE}_mass.json"
	echo -e "{
        \"relationKey\" : {
            \"userName\" : \"public\",
            \"programName\" : \"astro\",
            \"relationName\" : \"masstable${VARIABLE}\"
        },
        \"schema\" : {
            \"columnTypes\" : [\"INT_TYPE\", \"DOUBLE_TYPE\"],
            \"columnNames\" : [\"grp\", \"tot_mass\"]
        },
		  \"source\" : {
		  		\"dataType\" : \"File\",
        	 	\"filename\" : \"/projects/db8/leelee_cosmo_masses/cosmo${VARIABLE}_mass.txt\"
			},
        \"delimiter\": \",\"
    }" > "ingest${VARIABLE}_mass.json"
	echo -e "curl -i -XPOST berlin.cs.washington.edu:8753/dataset -H \"Content-type: application/json\"  -d @./ingest${VARIABLE}_mass.json\n" >> "ingest_all_masses.sh"
done