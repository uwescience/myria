package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

/**
 * Duplicate elimination. It adds newly meet unique tuples into a buffer so that the source TupleBatches are not
 * referenced. This implementation reduces memory consumption.
 * */
public final class SimpleAppender extends StreamingStateUpdater {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAppender.class.getName());

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
    return op.getSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    tuples = new ArrayList<TupleBatch>();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    tuples.add(tb);
    return null;
  }
}
