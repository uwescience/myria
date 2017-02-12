package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * The interface for any aggregator.
 */
public interface Aggregator extends Serializable {

  /**
   * Update the aggregate state using the specified row of the specified table.
   *
   * @param from the MutableTupleBuffer containing the source tuple.
   * @param fromRow the row index of the source tuple.
   * @param to the MutableTupleBuffer containing the state.
   * @param toRow the row index of the state.
   */
  public abstract void addRow(
      final TupleBatch from,
      final int fromRow,
      final MutableTupleBuffer to,
      final int toRow,
      final int offset);

  /**
   * @return the size of the state schema
   */
  int getStateSize();

  /**
   * Initialize a new state by appending initial values to a new row.
   *
   * @param state the table containing internal states
   * @param offset the column index of state to start from
   */
  void initState(final MutableTupleBuffer state, final int offset);
}
