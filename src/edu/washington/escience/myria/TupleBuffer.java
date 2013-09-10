package edu.washington.escience.myria;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.column.BooleanColumnBuilder;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ColumnBuilder;
import edu.washington.escience.myria.column.ColumnFactory;
import edu.washington.escience.myria.column.DateTimeColumn;
import edu.washington.escience.myria.column.DateTimeColumnBuilder;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.column.DoubleColumnBuilder;
import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.FloatColumnBuilder;
import edu.washington.escience.myria.column.IntColumn;
import edu.washington.escience.myria.column.IntColumnBuilder;
import edu.washington.escience.myria.column.LongColumn;
import edu.washington.escience.myria.column.LongColumnBuilder;
import edu.washington.escience.myria.column.StringColumn;
import edu.washington.escience.myria.column.StringColumnBuilder;
import edu.washington.escience.myria.column.mutable.BooleanMutableColumn;
import edu.washington.escience.myria.column.mutable.DateTimeMutableColumn;
import edu.washington.escience.myria.column.mutable.DoubleMutableColumn;
import edu.washington.escience.myria.column.mutable.FloatMutableColumn;
import edu.washington.escience.myria.column.mutable.IntMutableColumn;
import edu.washington.escience.myria.column.mutable.LongMutableColumn;
import edu.washington.escience.myria.column.mutable.MutableColumn;
import edu.washington.escience.myria.column.mutable.StringMutableColumn;

/** A simplified TupleBatchBuffer which supports random access. Designed for hash tables to use. */

public class TupleBuffer {
  /** Format of the emitted tuples. */
  private final Schema schema;
  /** Convenience constant; must match schema.numColumns() and currentColumns.size(). */
  private final int numColumns;
  /** List of completed TupleBatch objects. */
  private final List<MutableColumn<?>[]> readyTuples;
  /** Internal state used to build up a TupleBatch. */
  private ColumnBuilder<?>[] currentBuildingColumns;
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
    readyTuples = new ArrayList<MutableColumn<?>[]>();
    currentBuildingColumns = ColumnFactory.allocateColumns(schema).toArray(new ColumnBuilder<?>[] {});
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
    currentBuildingColumns = null;
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
    MutableColumn<?>[] buildingColumns = new MutableColumn<?>[numColumns];
    int i = 0;
    for (ColumnBuilder<?> cb : currentBuildingColumns) {
      buildingColumns[i++] = cb.buildMutable();
    }
    readyTuples.add(buildingColumns);
    currentBuildingColumns = ColumnFactory.allocateColumns(schema).toArray(new ColumnBuilder<?>[] {});
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
  @Deprecated
  public final Object get(final int colIndex, final int rowIndex) throws IndexOutOfBoundsException {
    int tupleBatchIndex = rowIndex / TupleBatch.BATCH_SIZE;
    int tupleIndex = rowIndex % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[colIndex].get(tupleIndex);
    }
    return currentBuildingColumns[colIndex].get(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final boolean getBoolean(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((BooleanMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getBoolean(tupleIndex);
    }
    return ((BooleanColumnBuilder) (currentBuildingColumns[column])).getBoolean(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final double getDouble(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((DoubleMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getDouble(tupleIndex);
    }
    return ((DoubleColumnBuilder) (currentBuildingColumns[column])).getDouble(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final float getFloat(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((FloatMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getFloat(tupleIndex);
    }
    return ((FloatColumnBuilder) (currentBuildingColumns[column])).getFloat(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final long getLong(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((LongMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getLong(tupleIndex);
    }
    return ((LongColumnBuilder) (currentBuildingColumns[column])).getLong(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final int getInt(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((IntMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getInt(tupleIndex);
    }
    return ((IntColumnBuilder) (currentBuildingColumns[column])).getInt(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final String getString(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((StringMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getString(tupleIndex);
    }
    return ((StringColumnBuilder) (currentBuildingColumns[column])).get(tupleIndex);
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final DateTime getDateTime(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return ((DateTimeMutableColumn) (readyTuples.get(tupleBatchIndex)[column])).getDateTime(tupleIndex);
    }
    return ((DateTimeColumnBuilder) (currentBuildingColumns[column])).get(tupleIndex);
  }

  /**
   * @param row the row number
   * @return the columns of the TB that the row resides.
   * */
  public Column<?>[] getColumns(final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex);
    }
    return null;
  }

  /**
   * @param row the row number
   * @return the index of the row in the containing TB.
   * */
  public final int getTupleIndexInContainingTB(final int row) {
    return row % TupleBatch.BATCH_SIZE;
  }

  /**
   * @param row the row number
   * @return the ColumnBuilder if the row resides in a in-building TB
   * */
  public ColumnBuilder<?>[] getColumnBuilders(final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return null;
    }
    return currentBuildingColumns;
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
  @Deprecated
  public final void put(final int column, final Object value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendObject(value);
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
    ((BooleanColumnBuilder) currentBuildingColumns[column]).append(value);
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
    ((DateTimeColumnBuilder) currentBuildingColumns[column]).append(value);
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
    ((DoubleColumnBuilder) currentBuildingColumns[column]).append(value);
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
    ((FloatColumnBuilder) currentBuildingColumns[column]).append(value);
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
    ((IntColumnBuilder) currentBuildingColumns[column]).append(value);
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
    ((LongColumnBuilder) currentBuildingColumns[column]).append(value);
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
    ((StringColumnBuilder) currentBuildingColumns[column]).append(value);
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
   * Append the specified value to the specified destination column in this TupleBatchBuffer from the source column.
   * 
   * @param destColumn which column in this TBB the value will be inserted.
   * @param sourceColumn the column from which data will be retrieved.
   * @param sourceRow the row in the source column from which data will be retrieved.
   */
  public final void put(final int destColumn, final Column<?> sourceColumn, final int sourceRow) {
    checkPutIndex(destColumn);
    ColumnBuilder<?> dest = currentBuildingColumns[destColumn];
    switch (dest.getType()) {
      case BOOLEAN_TYPE:
        ((BooleanColumnBuilder) dest).append(((BooleanColumn) sourceColumn).getBoolean(sourceRow));
        break;
      case DATETIME_TYPE:
        ((DateTimeColumnBuilder) dest).append(((DateTimeColumn) sourceColumn).getDateTime(sourceRow));
        break;
      case DOUBLE_TYPE:
        ((DoubleColumnBuilder) dest).append(((DoubleColumn) sourceColumn).getDouble(sourceRow));
        break;
      case FLOAT_TYPE:
        ((FloatColumnBuilder) dest).append(((FloatColumn) sourceColumn).getFloat(sourceRow));
        break;
      case INT_TYPE:
        ((IntColumnBuilder) dest).append(((IntColumn) sourceColumn).getInt(sourceRow));
        break;
      case LONG_TYPE:
        ((LongColumnBuilder) dest).append(((LongColumn) sourceColumn).getLong(sourceRow));
        break;
      case STRING_TYPE:
        ((StringColumnBuilder) dest).append(((StringColumn) sourceColumn).getString(sourceRow));
        break;
    }
    columnPut(destColumn);
  }

  /**
   * @param colIndex column index
   * 
   * @param rowIndex row index
   * 
   * @param value the new value
   * 
   * @throws IndexOutOfBoundsException if indices are out of bounds.
   */
  public final void replace(final int colIndex, final int rowIndex, final Object value)
      throws IndexOutOfBoundsException {
    int tupleBatchIndex = rowIndex / TupleBatch.BATCH_SIZE;
    int tupleIndex = rowIndex % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      MutableColumn<?> dest = readyTuples.get(tupleBatchIndex)[colIndex];
      switch (dest.getType()) {
        case BOOLEAN_TYPE:
          ((BooleanMutableColumn) dest).replace(tupleIndex, (Boolean) value);
          break;
        case DATETIME_TYPE:
          ((DateTimeMutableColumn) dest).replace(tupleIndex, (DateTime) value);
          break;
        case DOUBLE_TYPE:
          ((DoubleMutableColumn) dest).replace(tupleIndex, (Double) value);
          break;
        case FLOAT_TYPE:
          ((FloatMutableColumn) dest).replace(tupleIndex, (Float) value);
          break;
        case INT_TYPE:
          ((IntMutableColumn) dest).replace(tupleIndex, (Integer) value);
          break;
        case LONG_TYPE:
          ((LongMutableColumn) dest).replace(tupleIndex, (Long) value);
          break;
        case STRING_TYPE:
          ((StringMutableColumn) dest).replace(tupleIndex, (String) value);
          break;
      }
    } else {
      ColumnBuilder<?> dest = currentBuildingColumns[colIndex];
      switch (dest.getType()) {
        case BOOLEAN_TYPE:
          ((BooleanColumnBuilder) dest).replace(tupleIndex, (boolean) value);
          break;
        case DATETIME_TYPE:
          ((DateTimeColumnBuilder) dest).replace(tupleIndex, (DateTime) value);
          break;
        case DOUBLE_TYPE:
          ((DoubleColumnBuilder) dest).replace(tupleIndex, (double) value);
          break;
        case FLOAT_TYPE:
          ((FloatColumnBuilder) dest).replace(tupleIndex, (float) value);
          break;
        case INT_TYPE:
          ((IntColumnBuilder) dest).replace(tupleIndex, (int) value);
          break;
        case LONG_TYPE:
          ((LongColumnBuilder) dest).replace(tupleIndex, (long) value);
          break;
        case STRING_TYPE:
          ((StringColumnBuilder) dest).replace(tupleIndex, (String) value);
          break;
      }
    }
  }

  /**
   * Return all tuples in this buffer. The data do not get removed.
   * 
   * @return a List<TupleBatch> containing all complete tuples that have been inserted into this buffer.
   */
  public final List<TupleBatch> getAll() {
    final List<TupleBatch> output = new ArrayList<TupleBatch>();
    for (final MutableColumn<?>[] mutableColumns : readyTuples) {
      List<Column<?>> columns = new ArrayList<Column<?>>();
      for (MutableColumn<?> mutableColumn : mutableColumns) {
        columns.add(mutableColumn);
      }
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
    List<Column<?>> newColumns = new ArrayList<Column<?>>(currentBuildingColumns.length);
    for (ColumnBuilder<?> cb : currentBuildingColumns) {
      newColumns.add(cb.forkNewBuilder().build());
    }
    return newColumns;
  }
}
