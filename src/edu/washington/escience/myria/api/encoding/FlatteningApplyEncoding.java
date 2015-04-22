package edu.washington.escience.myria.api.encoding;

import java.util.List;
import java.util.Set;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.FlatteningApply;

public class FlatteningApplyEncoding extends UnaryOperatorEncoding<FlatteningApply> {

  @Required
  public List<Expression> emitExpressions;
  public Set<Integer> columnsToKeep;

  @Override
  public FlatteningApply construct(final ConstructArgs args) {
    return new FlatteningApply(null, emitExpressions, columnsToKeep);
  }
}
