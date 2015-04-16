package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SampleWR;

public class SampleWREncoding extends BinaryOperatorEncoding<SampleWR> {

  /** Used to make results deterministic. Null if no specified value. */
  public Long randomSeed;

  @Override
  public SampleWR construct(final ConstructArgs args) {
    return new SampleWR(null, null, randomSeed);
  }
}
