package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Filter;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class FilterEncoding extends OperatorEncoding<Filter> {

  public String argChild;
  public Expression argPredicate;
  private static List<String> requiredArguments = ImmutableList.of("argChild", "argPredicate");

  @Override
  public void connect(Operator operator, Map<String, Operator> operators) {
    operator.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  public Filter construct(Server server) {
    return new Filter(argPredicate, null);
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
