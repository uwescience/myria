package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.StatefulApply;
import edu.washington.escience.myria.parallel.Server;

public class StatefulApplyEncoding extends OperatorEncoding<Apply> {

  @Required
  public String argChild;
  @Required
  public List<Expression> emitExpressions;
  @Required
  public List<Expression> initializerExpressions;
  @Required
  public List<Expression> updaterExpressions;

  @Override
  public Apply construct(Server server) {
    return new StatefulApply(null, emitExpressions, initializerExpressions, updaterExpressions);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }
}
