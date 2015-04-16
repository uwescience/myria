package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SampleWoR;

public class SampleWoREncoding extends BinaryOperatorEncoding<SampleWoR> {

  /** Used to make results deterministic. Null if no specified value. */
  public Long randomSeed;

  @Override
  public SampleWoR construct(final ConstructArgs args) {
    return new SampleWoR(null, null, randomSeed);
  }
}
