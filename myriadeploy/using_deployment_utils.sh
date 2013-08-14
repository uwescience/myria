#!/bin/bash
java -cp 'libs/*' -Djava.library.path=sqlite4java-282 edu.washington.escience.myria.util.DeploymentUtils $@
