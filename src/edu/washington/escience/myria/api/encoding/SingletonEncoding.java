package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SingletonRelation;

public class SingletonEncoding extends LeafOperatorEncoding<SingletonRelation> {
  @Override
  public SingletonRelation construct(ConstructArgs args) {
    return new SingletonRelation();
  }
}
