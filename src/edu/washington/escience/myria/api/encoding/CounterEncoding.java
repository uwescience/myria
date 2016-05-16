package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Counter;

public class CounterEncoding extends UnaryOperatorEncoding<Counter> {

  @Required public String columnName;

  @Override
  public Counter construct(ConstructArgs args) {
    return new Counter(columnName);
  }
}
