#!/bin/bash
./jre1.7.0_13/bin/java -cp myriad-0.1.jar -Djava.library.path=sqlite4java-282/ edu.washington.escience.myriad.coordinator.catalog.CatalogMaker $@
