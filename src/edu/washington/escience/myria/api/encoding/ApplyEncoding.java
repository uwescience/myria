package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.expression.GenericExpression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class ApplyEncoding extends OperatorEncoding<Apply> {

  public String argChild;

  public List<GenericExpression> genericExpressions;

  private static final ImmutableList<String> requiredArguments = ImmutableList.of("argChild", "expressions");

  @Override
  public Apply construct(Server server) {
    return new Apply(null, genericExpressions);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

  @Override
  protected List<String> getRequiredArguments() {
    return requiredArguments;
  }
}
