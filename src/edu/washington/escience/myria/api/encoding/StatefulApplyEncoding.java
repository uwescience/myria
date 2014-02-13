package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.parallel.Server;

public class StatefulApplyEncoding extends OperatorEncoding<Apply> {

  public String argChild;

  public List<Expression> emitExpressions;
  public List<Expression> initializerExpressions;
  public List<Expression> updaterExpressions;

  private static final ImmutableList<String> requiredArguments = ImmutableList.of("argChild", "emitExpressions",
      "initializerExpressions", "updaterExpressions");

  @Override
  public Apply construct(Server server) {
    return new StatefulApply(null, emitExpressions, initializerExpressions, updaterExpressions);
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
