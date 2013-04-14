for i in `seq 1000`;
do
  echo starting number $i round
  java -ea -Xdebug -Xrunjdwp:transport=dt_socket,address=21001,server=y,suspend=n $(java -cp build/main:build/test edu.washington.escience.myriad.tool.EclipseClasspathReader .classpath) org.junit.runner.JUnitCore edu.washington.escience.myriad.testsuites.IterativeSystemTests
done
