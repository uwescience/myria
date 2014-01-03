package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.Counter;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class CounterEncoding extends OperatorEncoding<Counter> {

  public String child;
  public String columnName;

  private static final ImmutableList<String> requiredArguments = ImmutableList.of("child", "columnName");

  @Override
  public Counter construct(Server server) {
    return new Counter(columnName);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(child) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
