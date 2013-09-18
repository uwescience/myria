package edu.washington.escience.myria.operator.agg;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.StreamingStateUpdater;

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
