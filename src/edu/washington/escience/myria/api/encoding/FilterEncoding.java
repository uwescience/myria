package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Filter;

public class FilterEncoding extends UnaryOperatorEncoding<Filter> {

  @Required public Expression argPredicate;

  @Override
  public Filter construct(ConstructArgs args) {
    return new Filter(argPredicate, null);
  }
}
