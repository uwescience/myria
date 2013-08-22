package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.parallel.Producer;

/**
 * Reads data from a list of tuple batches.
 */
public final class TupleBatchListScan extends LeafOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(TupleBatchListScan.class.getName());

  /** the tuple batch buffer. */
  private final List<TupleBatch> tuples;
  /** the next position to fetch from the list of tuple batches. */
  private int currentPos;
  /** the channel index that this operator is recovering for. */
  private final int channelIndx;
  /** the original producer. */
  private final Producer oriProducer;

  /**
   * @param channelIndx the channel index that this operator is recovering for.
   * @param tuples the tuple batch buffer.
   * @param oriProducer the original producer.
   */
  public TupleBatchListScan(final int channelIndx, final List<TupleBatch> tuples, final Producer oriProducer) {
    this.tuples = tuples;
    this.oriProducer = oriProducer;
    this.channelIndx = channelIndx;
    currentPos = 0;
  }

  /**
   * return the original producer that this operator is scanning on.
   * 
   * @return the producer.
   * */
  public Producer getOriProducer() {
    return oriProducer;
  }

  /**
   * return the index of the broken channel that this operator is scanning on.
   * 
   * @return the index.
   * */
  public int getChannelIndx() {
    return channelIndx;
  }

  @Override
  public void cleanup() {
    currentPos = 0;
  }

  @Override
  protected TupleBatch fetchNextReady() {
    if (currentPos >= tuples.size()) {
      setEOS();
      return null;
    }
    TupleBatch ret = tuples.get(currentPos++);
    if (ret.isEOI()) {
      setEOI(true);
      return null;
    }
    return ret;
  }

  @Override
  public Schema getSchema() {
    return oriProducer.getSchema();
  }

  @Override
  protected void checkEOSAndEOI() {
    // do nothing since already did in fetchNextReady
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
  }
}
