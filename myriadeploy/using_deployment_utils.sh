#!/bin/bash
java -cp 'conf:libs/*' -Djava.library.path=sqlite4java-282 edu.washington.escience.myria.util.DeploymentUtils $@
