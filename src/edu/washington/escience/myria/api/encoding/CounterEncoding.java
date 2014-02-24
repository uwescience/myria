package edu.washington.escience.myria.api.encoding;

import java.util.Map;

import edu.washington.escience.myria.operator.Counter;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class CounterEncoding extends OperatorEncoding<Counter> {

  @Required
  public String child;
  @Required
  public String columnName;

  @Override
  public Counter construct(Server server) {
    return new Counter(columnName);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(child) });
  }

}
