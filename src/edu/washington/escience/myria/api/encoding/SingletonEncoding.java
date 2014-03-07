package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.operator.SingletonRelation;
import edu.washington.escience.myria.parallel.Server;

public class SingletonEncoding extends LeafOperatorEncoding<SingletonRelation> {
  @Override
  public SingletonRelation construct(final Server server) {
    return new SingletonRelation();
  }

}