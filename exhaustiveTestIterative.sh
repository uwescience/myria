i=1
true
while [[ $? -eq 0 ]]
do
  echo starting number $i round
  java -ea -Xdebug \
  -Xrunjdwp:transport=dt_socket,address=21001,server=y,suspend=n \
  -cp $(java -cp build/main  \
  edu.washington.escience.myriad.tool.EclipseClasspathReader \
  .classpath) org.junit.runner.JUnitCore \
  edu.washington.escience.myriad.testsuites.IterativeSystemTests && \
  i=$((i+1))
done
