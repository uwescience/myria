package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Map;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.parallel.Server;

public class ApplyEncoding extends OperatorEncoding<Apply> {

  @Required
  public String argChild;

  @Required
  public List<Expression> emitExpressions;

  @Override
  public Apply construct(Server server) {
    return new Apply(null, emitExpressions);
  }

  @Override
  public void connect(Operator current, Map<String, Operator> operators) {
    current.setChildren(new Operator[] { operators.get(argChild) });
  }

}
