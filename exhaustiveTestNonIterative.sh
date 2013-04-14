for i in `seq 1000`;
do
  echo starting number $i round
  java $(java -cp build/main:build/test edu.washington.escience.myriad.tool.EclipseClasspathReader .classpath) org.junit.runner.JUnitCore edu.washington.escience.myriad.testsuites.NonIterativeSystemTests
done
