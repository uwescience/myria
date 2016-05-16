package edu.washington.escience.myria.api.encoding;

import javax.ws.rs.core.Response;

import edu.washington.escience.myria.api.MyriaApiException;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SamplingDistribution;
import edu.washington.escience.myria.util.SamplingType;

public class SamplingDistributionEncoding extends UnaryOperatorEncoding<SamplingDistribution> {

  /** A specific number of tuples to sample. */
  public Integer sampleSize;

  /** Percentage of total tuples to sample. */
  public Float samplePercentage;

  @Required public SamplingType sampleType;

  /** Used to make results deterministic. Null if no specified value. */
  public Long randomSeed;

  @Override
  public SamplingDistribution construct(final ConstructArgs args) {
    if (sampleSize != null && samplePercentage == null) {
      return new SamplingDistribution(null, sampleSize, sampleType, randomSeed);
    } else if (sampleSize == null && samplePercentage != null) {
      return new SamplingDistribution(null, samplePercentage, sampleType, randomSeed);
    } else {
      throw new MyriaApiException(
          Response.Status.BAD_REQUEST,
          "Must specify exactly one of sampleSize or samplePercentage");
    }
  }
}
