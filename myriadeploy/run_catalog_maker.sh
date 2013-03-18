#!/bin/bash
java -cp myriad-0.1.jar -Djava.library.path=sqlite4java-282/ edu.washington.escience.myriad.coordinator.catalog.CatalogMaker $@
