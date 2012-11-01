package edu.washington.escience.myriad;

import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.table._TupleBatch;

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
  private int currentInProgressTuples;

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
    currentInProgressTuples = 0;
  }

  /**
   * Makes a batch of any tuples in the buffer and appends it to the internal list.
   * 
   * @return true if any tuples were added.
   */
  private boolean finishBatch() {
    if (numColumnsReady != 0) {
      throw new AssertionError("Can't finish a batch with partially-completed tuples!");
    }
    if (currentInProgressTuples == 0) {
      return false;
    }
    readyTuples.add(new TupleBatch(schema, currentColumns, currentInProgressTuples));
    currentColumns = ColumnFactory.allocateColumns(schema);
    currentInProgressTuples = 0;
    return true;
  }

  /**
   * Return all tuples in this buffer. The data do not get removed.
   * 
   * @return a List<TupleBatch> containing all complete tuples that have been inserted into this buffer.
   */
  public final List<TupleBatch> getAll() {
    final List<TupleBatch> output = new LinkedList<TupleBatch>();
    output.addAll(readyTuples);
    if (currentInProgressTuples > 0) {
      output.add(new TupleBatch(schema, currentColumns, currentInProgressTuples));
    }
    return output;
  }

  public final void clear() {
    columnsReady.clear();
    currentColumns.clear();
    currentInProgressTuples = 0;
    numColumnsReady = 0;
    readyTuples.clear();
  }

  /**
   * @return the number of complete tuples stored in this TupleBatchBuffer.
   */
  public final int numTuples() {
    return readyTuples.size() * TupleBatch.BATCH_SIZE + currentInProgressTuples;
  }

  /**
   * Extract and return the first complete TupleBatch in this Buffer.
   * 
   * @return the first complete TupleBatch in this buffer, or null if none is ready.
   */
  public final TupleBatch popFilled() {
    if (readyTuples.size() > 0) {
      return readyTuples.remove(0);
    }
    return null;
  }

  /**
   * @return pop filled and non-filled TupleBatch
   * */
  public final TupleBatch popAny() {
    TupleBatch tb = popFilled();
    if (tb != null) {
      return tb;
    } else {
      if (currentInProgressTuples > 0) {
        finishBatch();
        return popFilled();
      } else {
        return null;
      }
    }
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
      currentInProgressTuples++;
      numColumnsReady = 0;
      columnsReady.clear();
      if (currentInProgressTuples == TupleBatch.BATCH_SIZE) {
        finishBatch();
      }
    }
  }

  public final void putAll(_TupleBatch data) {
    List<Column> output = data.outputRawData();
    Preconditions.checkState(output.size() == numColumns);
    if (columnsReady.get(0)) {
      throw new RuntimeException("Need to fill up one row of TupleBatchBuffer before starting new one");
    }
    int numRow = output.get(0).size();
    for (int row = 0; row < numRow; row++) {
      for (int column = 0; column < numColumns; column++) {
        currentColumns.get(column).putObject(output.get(column).get(row));
        columnsReady.set(column, true);
        numColumnsReady++;
        if (numColumnsReady == numColumns) {
          currentInProgressTuples++;
          numColumnsReady = 0;
          columnsReady.clear();
          if (currentInProgressTuples == TupleBatch.BATCH_SIZE) {
            finishBatch();
          }
        }
      }
    }
  }

  public final void merge(TupleBatchBuffer another) {
    Iterator<TupleBatch> it = another.getAll().iterator();
    while (it.hasNext()) {
      putAll(it.next());
    }
  }

  /**
   * @return the Schema of the tuples in this buffer.
   */
  public final Schema getSchema() {
    return schema;
  }

}
