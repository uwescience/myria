java \
	-cp myriad-0.1.jar:conf \
	-Djava.library.path=sqlite4java-282 \
	edu.washington.escience.myriad.daemon.MasterDaemon $1
