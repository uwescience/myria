package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.Counter;
import edu.washington.escience.myria.parallel.Server;

public class CounterEncoding extends UnaryOperatorEncoding<Counter> {

  @Required
  public String columnName;

  @Override
  public Counter construct(Server server) {
    return new Counter(columnName);
  }

}
