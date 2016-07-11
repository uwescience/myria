package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Used by a Producer to append outgoing messages for FTMode.REJOIN. The producer is the only modifier.
 * */
public final class SimpleAppender extends StreamingState {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(SimpleAppender.class);

  /**
   * list of tuple batches.
   * */
  private transient List<TupleBatch> tuples;

  @Override
  public void cleanup() {
    tuples = null;
  }

  @Override
  public Schema getSchema() {
    return getOp().getInputSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    tuples = new ArrayList<TupleBatch>();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    tuples.add(tb);
    numTuples += tb.numTuples();
    return tb;
  }

  /** number of tuples. Used in resource profiling. */
  private volatile int numTuples = 0;

  @Override
  public List<TupleBatch> exportState() {
    return tuples;
  }

  @Override
  public int numTuples() {
    return numTuples;
  }

  @Override
  public StreamingState duplicate() {
    return new SimpleAppender();
  }
}
