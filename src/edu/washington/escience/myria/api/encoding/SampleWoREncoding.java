package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SampleWoR;

public class SampleWoREncoding extends BinaryOperatorEncoding<SampleWoR> {

  @Override
  public SampleWoR construct(final ConstructArgs args) {
    return new SampleWoR(null, null);
  }
}
