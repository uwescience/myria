package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 */
public class StreamingStateWrapper extends UnaryOperator implements StreamingStateful {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Its state. */
  private StreamingState state;

  /**
   * @param child the child operator.
   * @param state the internal state.
   */
  public StreamingStateWrapper(final Operator child, final StreamingState state) {
    super(child);
    if (state != null) {
      setStreamingState(state);
      state.setAttachedOperator(this);
    }
  }

  @Override
  public void setStreamingState(final StreamingState state) {
    this.state = state;
  }

  @Override
  public StreamingState getStreamingState() {
    return state;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    state.init(execEnvVars);
  }

  @Override
  protected void cleanup() throws Exception {
    state.cleanup();
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    Operator child = getChild();
    TupleBatch tb;
    while ((tb = child.nextReady()) != null) {
      tb = state.update(tb);
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

  /*
   * (non-Javadoc)
   * 
   * @see edu.washington.escience.myria.operator.Operator#sendEos()
   */
  @Override
  protected void sendEos() throws DbException {
    // TODO Auto-generated method stub

  }
}
