package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Counter;

public class CounterEncoding extends UnaryOperatorEncoding<Counter> {

  @Required
  public String columnName;

  @Override
  public Counter construct(@Nonnull ConstructArgs args) {
    return new Counter(columnName);
  }

}
