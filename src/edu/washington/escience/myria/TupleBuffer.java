package edu.washington.escience.myria;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.column.BooleanColumnBuilder;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ColumnBuilder;
import edu.washington.escience.myria.column.ColumnFactory;
import edu.washington.escience.myria.column.DateTimeColumnBuilder;
import edu.washington.escience.myria.column.DoubleColumnBuilder;
import edu.washington.escience.myria.column.FloatColumnBuilder;
import edu.washington.escience.myria.column.IntColumnBuilder;
import edu.washington.escience.myria.column.LongColumnBuilder;
import edu.washington.escience.myria.column.StringColumnBuilder;

/** A simplified TupleBatchBuffer which supports random access. Designed for hash tables to use. */

public class TupleBuffer {
  /** Format of the emitted tuples. */
  private final Schema schema;
  /** Convenience constant; must match schema.numColumns() and currentColumns.size(). */
  private final int numColumns;
  /** List of completed TupleBatch objects. */
  private final List<List<Column<?>>> readyTuples;
  /** Internal state used to build up a TupleBatch. */
  private List<ColumnBuilder<?>> currentBuildingColumns;
  /** Internal state representing which columns are ready in the current tuple. */
  private final BitSet columnsReady;
  /** Internal state representing the number of columns that are ready in the current tuple. */
  private int numColumnsReady;
  /** Internal state representing the number of tuples in the in-progress TupleBatch. */
  private int currentInProgressTuples;

  /**
   * Constructs an empty TupleBuffer to hold tuples matching the specified Schema.
   * 
   * @param schema specified the columns of the emitted TupleBatch objects.
   */
  public TupleBuffer(final Schema schema) {
    this.schema = Objects.requireNonNull(schema);
    readyTuples = new LinkedList<List<Column<?>>>();
    currentBuildingColumns = ColumnFactory.allocateColumns(schema);
    numColumns = schema.numColumns();
    columnsReady = new BitSet(numColumns);
    numColumnsReady = 0;
    currentInProgressTuples = 0;
  }

  /**
   * clear this TBB.
   * */
  public final void clear() {
    columnsReady.clear();
    currentBuildingColumns.clear();
    currentInProgressTuples = 0;
    numColumnsReady = 0;
    readyTuples.clear();
  }

  /**
   * Makes a batch of any tuples in the buffer and appends it to the internal list.
   * 
   */
  private void finishBatch() {
    Preconditions.checkArgument(numColumnsReady == 0);
    Preconditions.checkArgument(currentInProgressTuples == TupleBatch.BATCH_SIZE);
    List<Column<?>> buildingColumns = new ArrayList<Column<?>>(currentBuildingColumns.size());
    for (ColumnBuilder<?> cb : currentBuildingColumns) {
      buildingColumns.add(cb.build());
    }
    readyTuples.add(buildingColumns);
    currentBuildingColumns = ColumnFactory.allocateColumns(schema);
    currentInProgressTuples = 0;
  }

  /**
   * @return the Schema of the tuples in this buffer.
   */
  public final Schema getSchema() {
    return schema;
  }

  /**
   * @return the number of complete tuples stored in this TupleBuffer.
   */
  public final int numTuples() {
    return readyTuples.size() * TupleBatch.BATCH_SIZE + currentInProgressTuples;
  }

  /**
   * @param colIndex column index
   * @param rowIndex row index
   * @return the element at (rowIndex, colIndex)
   * @throws IndexOutOfBoundsException if indices are out of bounds.
   * */
  public final Object get(final int colIndex, final int rowIndex) throws IndexOutOfBoundsException {
    int tupleBatchIndex = rowIndex / TupleBatch.BATCH_SIZE;
    int tupleIndex = rowIndex % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex).get(colIndex).get(tupleIndex);
    }
    return currentBuildingColumns.get(colIndex).get(tupleIndex);
  }

  /**
   * @return num columns.
   * */
  public final int numColumns() {
    return numColumns;
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void put(final int column, final Object value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendObject(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putBoolean(final int column, final boolean value) {
    checkPutIndex(column);
    ((BooleanColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putDateTime(final int column, final DateTime value) {
    checkPutIndex(column);
    ((DateTimeColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putDouble(final int column, final double value) {
    checkPutIndex(column);
    ((DoubleColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putFloat(final int column, final float value) {
    checkPutIndex(column);
    ((FloatColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putInt(final int column, final int value) {
    checkPutIndex(column);
    ((IntColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putLong(final int column, final long value) {
    checkPutIndex(column);
    ((LongColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Append the specified value to the specified column.
   * 
   * @param column index of the column.
   * @param value value to be appended.
   */
  public final void putString(final int column, final String value) {
    checkPutIndex(column);
    ((StringColumnBuilder) currentBuildingColumns.get(column)).append(value);
    columnPut(column);
  }

  /**
   * Helper function: checks whether the specified column can be inserted into.
   * 
   * @param column the column in which the value should be put.
   */
  private void checkPutIndex(final int column) {
    Preconditions.checkElementIndex(column, numColumns);
    if (columnsReady.get(column)) {
      throw new RuntimeException("Need to fill up one row of TupleBatchBuffer before starting new one");
    }
  }

  /**
   * Helper function to update the internal state after a value has been inserted into the specified column.
   * 
   * @param column the column in which the value was put.
   */
  private void columnPut(final int column) {
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

  /**
   * @param colIndex column index
   * @param rowIndex row index
   * @param value the new value
   * @throws IndexOutOfBoundsException if indices are out of bounds.
   * */
  public final void replace(final int colIndex, final int rowIndex, final Object value)
      throws IndexOutOfBoundsException {
    int tupleBatchIndex = rowIndex / TupleBatch.BATCH_SIZE;
    int tupleIndex = rowIndex % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      readyTuples.get(tupleBatchIndex).get(colIndex).replace(tupleIndex, value);
    } else {
      currentBuildingColumns.get(colIndex).replace(tupleIndex, value);
    }
  }

  /**
   * Return all tuples in this buffer. The data do not get removed.
   * 
   * @return a List<TupleBatch> containing all complete tuples that have been inserted into this buffer.
   */
  public final List<TupleBatch> getAll() {
    final List<TupleBatch> output = new ArrayList<TupleBatch>();
    for (final List<Column<?>> columns : readyTuples) {
      output.add(new TupleBatch(schema, columns, TupleBatch.BATCH_SIZE));
    }
    if (currentInProgressTuples > 0) {
      output.add(new TupleBatch(schema, getInProgressColumns(), currentInProgressTuples));
    }
    return output;
  }

  /**
   * Build the in progress columns. The builders' states are untouched. They can keep building.
   * 
   * @return the built in progress columns.
   * */
  private List<Column<?>> getInProgressColumns() {
    List<Column<?>> newColumns = new ArrayList<Column<?>>(currentBuildingColumns.size());
    for (ColumnBuilder<?> cb : currentBuildingColumns) {
      newColumns.add(cb.forkNewBuilder().build());
    }
    return newColumns;
  }
}
