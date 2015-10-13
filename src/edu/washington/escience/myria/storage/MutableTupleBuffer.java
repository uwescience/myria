package edu.washington.escience.myria.storage;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.DateTimeColumnBuilder;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.column.mutable.MutableColumn;
import edu.washington.escience.myria.util.MyriaUtils;

/** A simplified TupleBatchBuffer which supports random access. Designed for hash tables to use. */

public class MutableTupleBuffer implements ReadableTable, AppendableTable, Cloneable {
  /** Format of the emitted tuples. */
  private final Schema schema;
  /** Convenience constant; must match schema.numColumns() and currentColumns.size(). */
  private final int numColumns;
  /** List of completed TupleBatch objects. */
  private final List<MutableColumn<?>[]> readyTuples;
  /** Internal state used to build up a TupleBatch. */
  private ColumnBuilder<?>[] currentBuildingColumns;
  /** Internal state representing which columns are ready in the current tuple. */
  private BitSet columnsReady;
  /** Internal state representing the number of columns that are ready in the current tuple. */
  private int numColumnsReady;
  /** Internal state representing the number of tuples in the in-progress TupleBatch. */
  private int currentInProgressTuples;

  /**
   * Constructs an empty TupleBuffer to hold tuples matching the specified Schema.
   * 
   * @param schema specified the columns of the emitted TupleBatch objects.
   */
  public MutableTupleBuffer(final Schema schema) {
    this.schema = Objects.requireNonNull(schema);
    readyTuples = new ArrayList<MutableColumn<?>[]>();
    currentBuildingColumns =
        ColumnFactory.allocateColumns(schema).toArray(new ColumnBuilder<?>[] {});
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
    currentBuildingColumns =
        ColumnFactory.allocateColumns(schema).toArray(new ColumnBuilder<?>[] {});
    currentInProgressTuples = 0;
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public final int numTuples() {
    return readyTuples.size() * TupleBatch.BATCH_SIZE + currentInProgressTuples;
  }

  @Override
  @Deprecated
  public final Object getObject(final int colIndex, final int rowIndex)
      throws IndexOutOfBoundsException {
    int tupleBatchIndex = rowIndex / TupleBatch.BATCH_SIZE;
    int tupleIndex = rowIndex % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[colIndex].getObject(tupleIndex);
    }
    return currentBuildingColumns[colIndex].getObject(tupleIndex);
  }

  @Override
  public final boolean getBoolean(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getBoolean(tupleIndex);
    }
    return currentBuildingColumns[column].getBoolean(tupleIndex);
  }

  @Override
  public final double getDouble(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getDouble(tupleIndex);
    }
    return currentBuildingColumns[column].getDouble(tupleIndex);
  }

  @Override
  public final float getFloat(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getFloat(tupleIndex);
    }
    return currentBuildingColumns[column].getFloat(tupleIndex);
  }

  @Override
  public final long getLong(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getLong(tupleIndex);
    }
    return currentBuildingColumns[column].getLong(tupleIndex);
  }

  @Override
  public final int getInt(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getInt(tupleIndex);
    }
    return currentBuildingColumns[column].getInt(tupleIndex);
  }

  @Override
  public final String getString(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getString(tupleIndex);
    }
    return currentBuildingColumns[column].getString(tupleIndex);
  }

  @Override
  public final DateTime getDateTime(final int column, final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex)[column].getDateTime(tupleIndex);
    }
    return ((DateTimeColumnBuilder) (currentBuildingColumns[column])).getDateTime(tupleIndex);
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
   * @return the columns
   */
  public ReadableColumn[] getColumns(final int row) {
    int tupleBatchIndex = row / TupleBatch.BATCH_SIZE;
    int tupleIndex = row % TupleBatch.BATCH_SIZE;

    if (tupleBatchIndex < readyTuples.size()) {
      return readyTuples.get(tupleBatchIndex);
    } else if (tupleBatchIndex == readyTuples.size() && tupleIndex < currentInProgressTuples) {
      return currentBuildingColumns;
    } else {
      throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public final int numColumns() {
    return numColumns;
  }

  @Override
  public final void putBoolean(final int column, final boolean value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendBoolean(value);
    columnPut(column);
  }

  @Override
  public final void putDateTime(final int column, final DateTime value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendDateTime(value);
    columnPut(column);
  }

  @Override
  public final void putDouble(final int column, final double value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendDouble(value);
    columnPut(column);
  }

  @Override
  public final void putFloat(final int column, final float value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendFloat(value);
    columnPut(column);
  }

  @Override
  public final void putInt(final int column, final int value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendInt(value);
    columnPut(column);
  }

  @Override
  public final void putLong(final int column, final long value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendLong(value);
    columnPut(column);
  }

  @Override
  @Deprecated
  public final void putObject(final int column, final Object value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendObject(MyriaUtils.ensureObjectIsValidType(value));
    columnPut(column);
  }

  @Override
  public final void putString(final int column, final String value) {
    checkPutIndex(column);
    currentBuildingColumns[column].appendString(value);
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
      throw new RuntimeException(
          "Need to fill up one row of TupleBatchBuffer before starting new one");
    }
  }

  /**
   * Helper function to update the internal state after a value has been inserted into the specified
   * column.
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
   * Append the specified value to the specified destination column in this TupleBatchBuffer from
   * the source column.
   * 
   * @param destColumn which column in this TB the value will be inserted.
   * @param sourceColumn the column from which data will be retrieved.
   * @param sourceRow the row in the source column from which data will be retrieved.
   */
  public final void put(final int destColumn, final Column<?> sourceColumn, final int sourceRow) {
    TupleUtils.copyValue(sourceColumn, sourceRow, this, destColumn);
  }

  /**
   * Swap the specified values from sourceRow to destRow in this TupleBuffer from the given column.
   * 
   * @param column which column in this TB the value will be inserted.
   * @param destRow the row in the dest column from which data will be retrieved.
   * @param sourceRow the row in the source column from which data will be retrieved.
   */
  public final void swap(final int column, final int destRow, final int sourceRow) {

    final int numTuples = numTuples();
    Preconditions.checkElementIndex(destRow, numTuples);
    Preconditions.checkElementIndex(sourceRow, numTuples);

    int destBatch = destRow / TupleBatch.BATCH_SIZE;
    int destBatchRow = destRow % TupleBatch.BATCH_SIZE;
    int sourceBatch = sourceRow / TupleBatch.BATCH_SIZE;
    int sourceBatchRow = sourceRow % TupleBatch.BATCH_SIZE;

    ReplaceableColumn sourceColumn;
    if (sourceBatch < readyTuples.size()) {
      sourceColumn = readyTuples.get(sourceBatch)[column];
    } else {
      sourceColumn = currentBuildingColumns[column];
    }
    ReplaceableColumn destColumn;
    if (destBatch < readyTuples.size()) {
      destColumn = readyTuples.get(destBatch)[column];
    } else {
      destColumn = currentBuildingColumns[column];
    }

    Type t = getSchema().getColumnType(column);
    switch (t) {
      case LONG_TYPE:
        long long1 = sourceColumn.getLong(sourceBatchRow);
        long long2 = destColumn.getLong(destBatchRow);
        sourceColumn.replaceLong(long2, sourceBatchRow);
        destColumn.replaceLong(long1, destBatchRow);
        break;
      case INT_TYPE:
        int int1 = sourceColumn.getInt(sourceBatchRow);
        int int2 = destColumn.getInt(destBatchRow);
        sourceColumn.replaceInt(int2, sourceBatchRow);
        destColumn.replaceInt(int1, destBatchRow);
        break;
      case DOUBLE_TYPE:
        double double1 = sourceColumn.getDouble(sourceBatchRow);
        double double2 = destColumn.getDouble(destBatchRow);
        sourceColumn.replaceDouble(double2, sourceBatchRow);
        destColumn.replaceDouble(double1, destBatchRow);
        break;
      case FLOAT_TYPE:
        float float1 = sourceColumn.getFloat(sourceBatchRow);
        float float2 = destColumn.getFloat(destBatchRow);
        sourceColumn.replaceFloat(float2, sourceBatchRow);
        destColumn.replaceFloat(float1, destBatchRow);
        break;
      case BOOLEAN_TYPE:
        boolean boolean1 = sourceColumn.getBoolean(sourceBatchRow);
        boolean boolean2 = destColumn.getBoolean(destBatchRow);
        sourceColumn.replaceBoolean(boolean2, sourceBatchRow);
        destColumn.replaceBoolean(boolean1, destBatchRow);
        break;
      case STRING_TYPE:
        String string1 = sourceColumn.getString(sourceBatchRow);
        String string2 = destColumn.getString(destBatchRow);
        sourceColumn.replaceString(string2, sourceBatchRow);
        destColumn.replaceString(string1, destBatchRow);
        break;
      case DATETIME_TYPE:
        DateTime date1 = sourceColumn.getDateTime(sourceBatchRow);
        DateTime date2 = destColumn.getDateTime(destBatchRow);
        sourceColumn.replaceDateTime(date2, sourceBatchRow);
        destColumn.replaceDateTime(date1, destBatchRow);
        break;
    }
  }

  /**
   * Replace the specified value to the specified destination column in this TupleBuffer from the
   * source column.
   * 
   * @param destColumn which column in this TB the value will be inserted.
   * @param destRow the row in the dest column from which data will be retrieved.
   * @param sourceColumn the column from which data will be retrieved.
   * @param sourceRow the row in the source column from which data will be retrieved.
   */
  public final void replace(final int destColumn, final int destRow, final Column<?> sourceColumn,
      final int sourceRow) {
    checkPutIndex(destColumn);
    int tupleBatchIndex = destRow / TupleBatch.BATCH_SIZE;
    int tupleIndex = destRow % TupleBatch.BATCH_SIZE;
    if (tupleBatchIndex > readyTuples.size() || tupleBatchIndex == readyTuples.size()
        && tupleIndex >= currentInProgressTuples) {
      throw new IndexOutOfBoundsException();
    }
    ReplaceableColumn dest;
    if (tupleBatchIndex < readyTuples.size()) {
      dest = readyTuples.get(tupleBatchIndex)[destColumn];
    } else {
      dest = currentBuildingColumns[destColumn];
    }

    switch (dest.getType()) {
      case BOOLEAN_TYPE:
        dest.replaceBoolean(sourceColumn.getBoolean(sourceRow), tupleIndex);
        break;
      case DATETIME_TYPE:
        dest.replaceDateTime(sourceColumn.getDateTime(sourceRow), tupleIndex);
        break;
      case DOUBLE_TYPE:
        dest.replaceDouble(sourceColumn.getDouble(sourceRow), tupleIndex);
        break;
      case FLOAT_TYPE:
        dest.replaceFloat(sourceColumn.getFloat(sourceRow), tupleIndex);
        break;
      case INT_TYPE:
        dest.replaceInt(sourceColumn.getInt(sourceRow), tupleIndex);
        break;
      case LONG_TYPE:
        dest.replaceLong(sourceColumn.getLong(sourceRow), tupleIndex);
        break;
      case STRING_TYPE:
        dest.replaceString(sourceColumn.getString(sourceRow), tupleIndex);
        break;
    }
  }

  /**
   * Return all tuples in this buffer. The data do not get removed.
   * 
   * @return a List<TupleBatch> containing all complete tuples that have been inserted into this
   *         buffer.
   */
  public final List<TupleBatch> getAll() {
    final List<TupleBatch> output = new ArrayList<TupleBatch>();
    for (final MutableColumn<?>[] mutableColumns : readyTuples) {
      List<Column<?>> columns = new ArrayList<Column<?>>();
      for (MutableColumn<?> mutableColumn : mutableColumns) {
        columns.add(mutableColumn.toColumn());
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

  @Override
  public MutableTupleBuffer clone() {
    MutableTupleBuffer ret = new MutableTupleBuffer(getSchema());
    ret.columnsReady = (BitSet) columnsReady.clone();
    ret.numColumnsReady = numColumnsReady;
    ret.currentInProgressTuples = currentInProgressTuples;
    for (MutableColumn<?>[] columns : readyTuples) {
      MutableColumn<?>[] tmp = new MutableColumn<?>[columns.length];
      for (int i = 0; i < columns.length; ++i) {
        tmp[i] = columns[i].clone();
      }
      ret.readyTuples.add(tmp);
    }
    for (int i = 0; i < currentBuildingColumns.length; ++i) {
      ret.currentBuildingColumns[i] = currentBuildingColumns[i].forkNewBuilder();
    }
    return ret;
  }

  @Override
  public ReadableColumn asColumn(final int column) {
    return new ReadableSubColumn(this, Preconditions.checkElementIndex(column, numColumns));
  }

  @Override
  public WritableColumn asWritableColumn(final int column) {
    return new WritableSubColumn(this, column);
  }
}
