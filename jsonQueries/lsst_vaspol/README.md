LSST Use Case
========================
Dataset stored at vega.cs.washington.edu:/disk1/lsst_data
---------------------------
Each row represents the brightness of the pixel that was captured. Each pixel has multiple snapshots. Thus, the time attribute in the dataset. We can think of the dataset as a cube where we have x, y coordinates and the time is the depth of the cube. The data attribute represents the brightness of that particular pixel.

*Schema: StarInfo(x int, y int, time int, data double)

Goal
---------------------------
Execute the following query iteratively

		SELECT *
		FROM A, StarInfo
		WHERE A.avg + k * A.stdev > StarInfo.data AND
		  	A.avg - k * A.stdev < StarInfo.data
		     	A.x = StarInfo.x AND A.y = StarInfo.y;

		(SELECT x, y, avg(data), stdev(data)
		FROM StarInfo
		GROUP BY x, y) as A;

Current State
---------------------------
An execution of 1 iteration is done. The plan is in the full_one_iteration_LSST.json. The A part is also in another separate file.
Used a small subset of the data (*small.csv*) to execute the query on 1 and 3 machines. The run time is the following
* 1 machine: 25s
* 3 machines: 22s
Another dataset is also available Larger dataset (50GB) (large.csv)
* 3 machines: 4h to ingest the data
* 3 machines: ran for 16h 50m, and got out of memory exception


Ingesting the Dataset
---------------------------
The two dataset are delimited differently. large.csv is delimited with comma (,), and small.csv is delimited with space ( ).  It is important that you specify the “delimiter” field in the ingest json when you ingest large.csv file.

TODO
---------------------------
* run the query iteratively
* measure the performance

Query Plan
---------------------------

		                                     Output
		                                        |
		                                     Filter
		 (A.avg + k * A.stdev > StarInfo.data AND A.avg - k * A.stdev < StarInfo.data)
		              |
		Join (A.x = StarInfo.A AND A.y = StarInfo.y)
		              |                          |________________________________
		              |                                                           |
		MultiGroupByAggregate(x,y,avg(data),stdev(data))  as A              Scan(starInfo)
		              |
		       Scan(StarInfo)

Shuffling is needed when the query is executed parallel as defined in the query plan.
If you have any question please email me at vaspol@cs.washington.edu
