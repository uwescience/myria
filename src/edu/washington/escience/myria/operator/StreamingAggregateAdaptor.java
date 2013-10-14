package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;

/**
 */
public class StreamingAggregateAdaptor extends UnaryOperator implements StreamingAggregate {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Its updater. */
  private StreamingStateUpdater updater;

  /**
   * @param child the child operator.
   * @param updater the updater.
   */
  public StreamingAggregateAdaptor(final Operator child, final StreamingStateUpdater updater) {
    super(child);
    if (updater != null) {
      setStateUpdater(updater);
      updater.setAttachedOperator(this);
    }
  }

  @Override
  public void setStateUpdater(final StreamingStateUpdater updater) {
    this.updater = updater;
  }

  @Override
  public StreamingStateUpdater getStateUpdater() {
    return updater;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    updater.init(execEnvVars);
  }

  @Override
  protected void cleanup() throws Exception {
    updater.cleanup();
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    Operator child = getChild();
    TupleBatch tb;
    while ((tb = child.nextReady()) != null) {
      tb = updater.update(tb);
      if (tb != null && tb.numTuples() > 0) {
        return tb;
      }
    }
    return null;
  }

  @Override
  public Schema generateSchema() {
    if (getChild() == null) {
      return null;
    }
    return getChild().getSchema();
  }
}
