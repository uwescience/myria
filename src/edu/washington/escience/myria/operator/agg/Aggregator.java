package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;
import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableTable;
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
      final ReadableTable from, final int fromRow, final MutableTupleBuffer to, final int toRow);

  /**
   * @param tb the internal states
   * @return output states as columns
   */
  public List<Column<?>> emitOutput(final TupleBatch tb);

  /**
   * @return the output schema
   */
  Schema getOutputSchema();

  /**
   * Initialize a new state by appending initial values to a new row.
   *
   * @param data the table containing internal states
   */
  void initState(AppendableTable data);
}
