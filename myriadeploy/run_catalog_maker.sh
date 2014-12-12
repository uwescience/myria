#!/bin/bash
java -cp 'conf:libs/*' -Djava.library.path=sqlite4java-392/ edu.washington.escience.myria.coordinator.catalog.CatalogMaker $@
