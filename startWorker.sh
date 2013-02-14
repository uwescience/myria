java $(java -cp $2 edu.washington.escience.myriad.tool.EclipseClasspathReader .classpath 1 $2) edu.washington.escience.myriad.parallel.Worker --workingDir $1
