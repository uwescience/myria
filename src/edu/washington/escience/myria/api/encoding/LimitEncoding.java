package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Limit;

public class LimitEncoding extends UnaryOperatorEncoding<Limit> {

  @Required
  public Long numTuples;

  @Override
  public Limit construct(@Nonnull ConstructArgs args) {
    return new Limit(numTuples, null);
  }
}