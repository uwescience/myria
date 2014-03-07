package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.operator.Filter;
import edu.washington.escience.myria.parallel.Server;

public class FilterEncoding extends UnaryOperatorEncoding<Filter> {

  @Required
  public Expression argPredicate;

  @Override
  public Filter construct(Server server) {
    return new Filter(argPredicate, null);
  }
}
