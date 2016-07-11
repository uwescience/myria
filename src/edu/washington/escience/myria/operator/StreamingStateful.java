package edu.washington.escience.myria.operator;

import java.util.List;

import edu.washington.escience.myria.Schema;

/**
 */
public interface StreamingStateful {
  /**
   * set its streaming states.
   *
   * @param states the streaming states.
   */
  void setStreamingStates(final List<StreamingState> states);

  /** @return its states. */
  List<StreamingState> getStreamingStates();

  /** @return the schema of the input to the streaming state. */
  Schema getInputSchema();
}
