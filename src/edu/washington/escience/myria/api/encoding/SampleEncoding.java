package edu.washington.escience.myria.api.encoding;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.Sample;

public class SampleEncoding extends BinaryOperatorEncoding<Sample> {

  /** Used to make results deterministic. Null if no specified value. */
  public Long randomSeed;

  @Override
  public Sample construct(@Nonnull final ConstructArgs args) {
    return new Sample(null, null, randomSeed);
  }
}
