package edu.washington.escience.myriad;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;

/**
 * Used for creating TupleBatch objects on the fly. A helper class used in, e.g., the Scatter operator.
 * 
 * @author dhalperi
 * 
 */
public class TupleBatchBuffer {
  /** Format of the emitted tuples. */
  private final Schema schema;
  /** Convenience constant; must match schema.numColumns() and currentColumns.size(). */
  private final int numColumns;
  /** List of completed TupleBatch objects. */
  private final List<TupleBatch> readyTuples;
  /** Internal state used to build up a TupleBatch. */
  private List<Column> currentColumns;
  /** Internal state representing which columns are ready in the current tuple. */
  private final BitSet columnsReady;
  /** Internal state representing the number of columns that are ready in the current tuple. */
  private int numColumnsReady;
  /** Internal state representing the number of tuples in the in-progress TupleBatch. */
  private int currentNumTuples;

  /**
   * Constructs an empty TupleBatchBuffer to hold tuples matching the specified Schema.
   * 
   * @param schema specified the columns of the emitted TupleBatch objects.
   */
  public TupleBatchBuffer(final Schema schema) {
    this.schema = Objects.requireNonNull(schema);
    readyTuples = new LinkedList<TupleBatch>();
    currentColumns = ColumnFactory.allocateColumns(schema);
    numColumns = schema.numFields();
    columnsReady = new BitSet(numColumns);
    numColumnsReady = 0;
  }

  /**
   * Makes a batch of any tuples in the buffer and appends it to the internal list.
   */
  private void finishBatch() {
    if (numColumnsReady != 0) {
      throw new AssertionError("Can't finish a batch with partially-completed tuples!");
    }
    if (currentNumTuples == 0) {
      return;
    }
    readyTuples.add(new TupleBatch(schema, currentColumns, currentNumTuples));
    currentColumns = ColumnFactory.allocateColumns(schema);
    currentNumTuples = 0;
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void put(final int column, final Object value) {
    Preconditions.checkElementIndex(column, numColumns);
    if (columnsReady.get(column)) {
      throw new RuntimeException("Need to fill up one row of TupleBatchBuffer before starting new one");
    }
    currentColumns.get(column).putObject(value);
    columnsReady.set(column, true);
    numColumnsReady++;
    if (numColumnsReady == numColumns) {
      currentNumTuples++;
      columnsReady.clear();
      if (currentNumTuples == TupleBatch.BATCH_SIZE) {
        finishBatch();
      }
    }
  }
  
  public final int numTuples()
  {
    return this.readyTuples.size()*TupleBatch.BATCH_SIZE+this.currentNumTuples;
  }

  public final List<TupleBatch> getOutput() {
    return this.readyTuples;
  }

  public final TupleBatch pop() {
    if (this.readyTuples.size() > 0) {
      return this.readyTuples.remove(0);
    }
    return null;
  }

}
