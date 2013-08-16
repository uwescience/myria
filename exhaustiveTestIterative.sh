i=1
classpath="$(java -cp build/main  \
  edu.washington.escience.myria.tool.EclipseClasspathReader \
  .classpath)"
libpath="$(java -cp build/main  \
  edu.washington.escience.myria.tool.EclipseClasspathReader \
  .classpath lib)"
true
while [[ $? -eq 0 ]]
do
  echo starting number $i round
  java -ea -Xdebug \
  -Xrunjdwp:transport=dt_socket,address=21001,server=y,suspend=n \
  -cp "$classpath" -Djava.library.path="$libpath" org.junit.runner.JUnitCore \
  edu.washington.escience.myria.testsuites.IterativeSystemTests \
  && i=$((i+1))
done
