package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SamplingDistribution;

public class SamplingDistributionEncoding extends UnaryOperatorEncoding<SamplingDistribution> {

  @Required
  public int sampleSize;

  @Required
  public boolean isWithoutReplacement;

  @Override
  public SamplingDistribution construct(final ConstructArgs args) {
    return new SamplingDistribution(sampleSize, isWithoutReplacement, null);
  }
}
