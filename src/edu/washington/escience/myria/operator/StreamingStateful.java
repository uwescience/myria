package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;

/**
 */
public interface StreamingStateful {
  /**
   * set its streaming state.
   *
   * @param state the streaming state.
   * */
  void setStreamingState(final StreamingState state);

  /** @return its state. */
  StreamingState getStreamingState();

  /** @return the output schema. */
  Schema getSchema();
}
