package edu.washington.escience.myria.api.encoding;

import java.util.List;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.FlatteningApply;

public class FlatteningApplyEncoding extends UnaryOperatorEncoding<FlatteningApply> {

  @Required
  public List<Expression> emitExpressions;
  public List<Integer> columnsToKeep;

  @Override
  public FlatteningApply construct(final ConstructArgs args) {
    return new FlatteningApply(null, emitExpressions, columnsToKeep);
  }
}
