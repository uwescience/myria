touch "ingest_all_masses.sh"
for VARIABLE in 024 048 072 084 096 128 144 168 192 216 226 240 264 288 312 328 336 384 406 408 432 455 456 466 480 504 512
do
	touch "ingest${VARIABLE}_mass.json"
	echo -e "{
        \"relation_key\" : {
            \"user_name\" : \"leelee\",
            \"program_name\" : \"astro\",
            \"relation_name\" : \"masstable${VARIABLE}\"
        },
        \"schema\" : {
            \"column_types\" : [\"INT_TYPE\", \"DOUBLE_TYPE\"],
            \"column_names\" : [\"grp\", \"tot_mass\"]
        },
        \"file_name\" : \"/projects/db8/leelee_cosmo_masses/cosmo${VARIABLE}_mass.txt\",
        \"delimiter\": \",\"
    }" > "ingest${VARIABLE}_mass.json"
	echo -e "curl -i -XPOST beijing.cs.washington.edu:8778/dataset -H \"Content-type: application/json\"  -d @./ingest${VARIABLE}_mass.json\n" >> "ingest_all_masses.sh"
done