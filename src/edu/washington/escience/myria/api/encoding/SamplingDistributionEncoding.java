package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SamplingDistribution;

public class SamplingDistributionEncoding extends UnaryOperatorEncoding<SamplingDistribution> {

  @Required
  public int sampleSize;

  @Required
  public boolean isWithReplacement;

  /** Used to make results deterministic. Null if no specified value. */
  public Long randomSeed;

  @Override
  public SamplingDistribution construct(final ConstructArgs args) {
    return new SamplingDistribution(null, sampleSize, isWithReplacement, randomSeed);
  }
}
