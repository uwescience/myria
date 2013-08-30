package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;

/**
 */
public interface StreamingAggregate {
  /**
   * set its updater.
   * 
   * @param updater the StreamingStateUpdater.
   * */
  void setStateUpdater(final StreamingStateUpdater updater);

  /** @return its updater. */
  StreamingStateUpdater getStateUpdater();

  /** @return the output schema. */
  Schema getSchema();
}
