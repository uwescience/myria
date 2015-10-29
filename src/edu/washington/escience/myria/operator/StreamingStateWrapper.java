package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

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
      setStreamingStates(ImmutableList.of(state));
    }
  }

  @Override
  public void setStreamingStates(final List<StreamingState> states) {
    state = states.get(0);
    state.setAttachedOperator(this);
  }

  @Override
  public List<StreamingState> getStreamingStates() {
    return ImmutableList.of(state);
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
  public Schema getInputSchema() {
    if (getChild() == null) {
      return null;
    }
    return getChild().getSchema();
  }

  @Override
  public Schema generateSchema() {
    return state.getSchema();
  }
}
