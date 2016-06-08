package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.SampledDbInsertTemp;

/**
 * Encoding for SampledDbInsertTemp oeprator.
 *
 */
public class SampledDbInsertTempEncoding extends UnaryOperatorEncoding<SampledDbInsertTemp> {

  @Required public Integer sampleSize;
  @Required public String sampleTable;
  @Required public String countTable;

  /** Used to make results deterministic. Null if no specified value. */
  public Long randomSeed;

  /**
   * The ConnectionInfo struct determines what database the data will be written
   * to. If null, the worker's default database will be used.
   */
  public ConnectionInfo connectionInfo;

  @Override
  public SampledDbInsertTemp construct(ConstructArgs args) {
    return new SampledDbInsertTemp(
        null,
        sampleSize,
        RelationKey.ofTemp(args.getQueryId(), sampleTable),
        RelationKey.ofTemp(args.getQueryId(), countTable),
        connectionInfo,
        randomSeed);
  }
}
