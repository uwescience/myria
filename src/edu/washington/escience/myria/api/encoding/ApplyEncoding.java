package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.parallel.Server;

public class ApplyEncoding extends UnaryOperatorEncoding<Apply> {

  @Required
  public List<Expression> emitExpressions;

  @Override
  public Apply construct(Server server) {
    return new Apply(null, emitExpressions);
  }
}
