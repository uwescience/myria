touch "ingest_all_cosmo.sh"
for VARIABLE in 024 048 072 084 096 128 144 168 192 216 226 240 264 288 312 328 336 384 406 408 432 455 456 466 480 504 512
do
	touch "ingest_cosmo$VARIABLE.json"
	echo -e "{ \n\
    	\"grp_filename\": \"/projects/db8/dataset_astro_2011/cosmo50cmb.256g2MbwK.00$VARIABLE.amiga.grp\",\n\
    	\"iorder_filename\": \"/projects/db8/dataset_astro_2011/cosmo50cmb.256g2MbwK.00$VARIABLE.iord\",\n\
    	\"relation_key\": {\n\
        	\"program_name\": \"astro\",\n\
        	\"relation_name\": \"cosmo$VARIABLE\",\n\
        	\"user_name\": \"leelee\"\n\
    	},\n\
    	\"tipsy_filename\": \"/projects/db8/dataset_astro_2011/cosmo50cmb.256g2MbwK.00$VARIABLE\"\n\
	}" > "ingest_cosmo$VARIABLE.json"
	echo -e "curl -i -XPOST beijing.cs.washington.edu:8778/dataset/tipsy -H \"Content-type: application/json\"  -d @./ingest_cosmo${VARIABLE}.json\n" >> "ingest_all_cosmo.sh"
done