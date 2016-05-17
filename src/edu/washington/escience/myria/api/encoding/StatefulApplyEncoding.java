package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Apply;
import edu.washington.escience.myria.operator.StatefulApply;

public class StatefulApplyEncoding extends UnaryOperatorEncoding<Apply> {

  @Required public List<Expression> emitExpressions;
  @Required public List<Expression> initializerExpressions;
  @Required public List<Expression> updaterExpressions;

  @Override
  public Apply construct(ConstructArgs args) {
    return new StatefulApply(null, emitExpressions, initializerExpressions, updaterExpressions);
  }
}
