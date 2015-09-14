package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SingletonRelation;

public class SingletonEncoding extends LeafOperatorEncoding<SingletonRelation> {
  @Override
  public SingletonRelation construct(@Nonnull ConstructArgs args) {
    return new SingletonRelation();
  }

}