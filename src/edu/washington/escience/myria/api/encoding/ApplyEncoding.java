package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class ApplyEncoding extends OperatorEncoding<Apply> {

  public String argChild;

  public List<ExpressionEncoding> expressions;

  private static final ImmutableList<String> requiredArguments = ImmutableList.of("argChild", "expressions");

  @Override
  public Apply construct(Server server) {
    ImmutableList.Builder<Expression> eb = ImmutableList.builder();
    for (ExpressionEncoding ee : expressions) {
      eb.add(ee.construct());
    }
    return new Apply(null, eb.build());
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
