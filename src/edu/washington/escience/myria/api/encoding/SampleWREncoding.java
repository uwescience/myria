package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SampleWR;

public class SampleWREncoding extends BinaryOperatorEncoding<SampleWR> {

  @Override
  public SampleWR construct(final ConstructArgs args) {
    return new SampleWR(null, null);
  }
}
