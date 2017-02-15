package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;

public class ApplyEncoding extends UnaryOperatorEncoding<Apply> {

  @Required public List<Expression> emitExpressions;
  public Boolean addCounter;

  @Override
  public Apply construct(final ConstructArgs args) {
    return new Apply(null, emitExpressions, addCounter);
  }
}
