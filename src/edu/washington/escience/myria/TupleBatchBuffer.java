package edu.washington.escience.myria;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
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

/**
 * Used for creating TupleBatch objects on the fly. A helper class used in, e.g., the Scatter operator. Currently it
 * doesn't support random access to a specific cell. Use TupleBuffer instead.
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
  private final List<List<Column<?>>> readyTuples;
  /** Internal state used to build up a TupleBatch. */
  private List<ColumnBuilder<?>> currentBuildingColumns;
  /** Internal state representing which columns are ready in the current tuple. */
  private final BitSet columnsReady;
  /** Internal state representing the number of columns that are ready in the current tuple. */
  private int numColumnsReady;
  /** Internal state representing the number of tuples in the in-progress TupleBatch. */
  private int currentInProgressTuples;

  /** The last time this operator returned a TupleBatch. */
  private long lastPoppedTime;
  /** the total number of tuples in readyTuples. */
  private int readyTuplesNum;

  /**
   * Constructs an empty TupleBatchBuffer to hold tuples matching the specified Schema.
   * 
   * @param schema specified the columns of the emitted TupleBatch objects.
   */
  public TupleBatchBuffer(final Schema schema) {
    this.schema = Objects.requireNonNull(schema);
    readyTuples = new LinkedList<List<Column<?>>>();
    currentBuildingColumns = ColumnFactory.allocateColumns(schema);
    numColumns = schema.numColumns();
    columnsReady = new BitSet(numColumns);
    numColumnsReady = 0;
    currentInProgressTuples = 0;
    lastPoppedTime = System.nanoTime();
    readyTuplesNum = 0;
  }

  /**
   * Append the tuple batch directly into readTuples.
   * 
   * @param tb the TB.
   */
  public final void appendTB(final TupleBatch tb) {
    finishBatch();
    readyTuplesNum += tb.numTuples();
    readyTuples.add(tb.getDataColumns());
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
   * clear this TBB.
   * */
  public final void clear() {
    columnsReady.clear();
    currentBuildingColumns.clear();
    currentInProgressTuples = 0;
    numColumnsReady = 0;
    readyTuples.clear();
    readyTuplesNum = 0;
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
    List<Column<?>> buildingColumns = new ArrayList<Column<?>>(currentBuildingColumns.size());
    for (ColumnBuilder<?> cb : currentBuildingColumns) {
      buildingColumns.add(cb.build());
    }
    readyTuples.add(buildingColumns);
    if (buildingColumns.size() > 0) {
      readyTuplesNum += buildingColumns.get(0).size();
    }
    currentBuildingColumns = ColumnFactory.allocateColumns(schema);
    currentInProgressTuples = 0;
    return true;
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
   * Return all tuples in this buffer. The data do not get removed.
   * 
   * @return a List<TupleBatch> containing all complete tuples that have been inserted into this buffer.
   */
  public final List<List<Column<?>>> getAllAsRawColumn() {
    final List<List<Column<?>>> output = new ArrayList<List<Column<?>>>();
    for (final List<Column<?>> columns : readyTuples) {
      output.add(columns);
    }
    if (currentInProgressTuples > 0) {
      output.add(getInProgressColumns());
    }
    return output;
  }

  /**
   * Get elapsed time since the last time when a TB is poped.
   * 
   * @return the elapsed time from lastPopedTime to present
   */
  private long getElapsedTime() {
    return System.nanoTime() - lastPoppedTime;
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

  /**
   * @return the number of ready tuples.
   */
  public final int getReadyTuplesNum() {
    return readyTuplesNum;
  }

  /**
   * @return the Schema of the tuples in this buffer.
   */
  public final Schema getSchema() {
    return schema;
  }

  /**
   * @return if there is filled TupleBatches ready for pop.
   * */
  public final boolean hasFilledTB() {
    return readyTuples.size() > 0;
  }

  /**
   * @param another TBB.
   * */
  public final void merge(final TupleBatchBuffer another) {
    readyTuples.addAll(another.readyTuples);
    readyTuplesNum += another.getReadyTuplesNum();
    if (another.currentInProgressTuples > 0) {
      for (int row = 0; row < another.currentInProgressTuples; row++) {
        int column = 0;
        for (final Column<?> c : another.getInProgressColumns()) {
          put(column, c, row);
          column++;
        }
      }
    }
  }

  /**
   * @return num columns.
   * */
  public final int numColumns() {
    return numColumns;
  }

  /**
   * @return the number of complete tuples stored in this TupleBatchBuffer.
   */
  public final int numTuples() {
    return readyTuplesNum + currentInProgressTuples;
  }

  /**
   * @return pop filled and non-filled TupleBatch
   * */
  public final TupleBatch popAny() {
    final TupleBatch tb = popFilled();
    if (tb != null) {
      updateLastPopedTime();
      return tb;
    } else {
      if (currentInProgressTuples > 0) {
        final int size = currentInProgressTuples;
        finishBatch();
        updateLastPopedTime();
        readyTuplesNum -= size;
        return new TupleBatch(schema, readyTuples.remove(0), size);
      } else {
        return null;
      }
    }
  }

  /**
   * @return pop filled or non-filled as list of columns.
   * */
  public final List<Column<?>> popAnyAsRawColumn() {
    final List<Column<?>> rc = popFilledAsRawColumn();
    if (rc != null) {
      updateLastPopedTime();
      return rc;
    } else {
      if (currentInProgressTuples > 0) {
        finishBatch();
        updateLastPopedTime();
        readyTuplesNum -= currentInProgressTuples;
        return readyTuples.remove(0);
      } else {
        return null;
      }
    }
  }

  /**
   * @return pop filled and non-filled TupleBatch
   * */
  public final TupleBatch popAnyUsingTimeout() {
    final TupleBatch tb = popFilled();
    if (tb != null) {
      updateLastPopedTime();
      return tb;
    } else {
      if (currentInProgressTuples > 0 && getElapsedTime() >= MyriaConstants.PUSHING_TB_TIMEOUT) {
        final int size = currentInProgressTuples;
        finishBatch();
        updateLastPopedTime();
        readyTuplesNum -= size;
        return new TupleBatch(schema, readyTuples.remove(0), size);
      } else {
        return null;
      }
    }
  }

  /**
   * Extract and return the first complete TupleBatch in this Buffer.
   * 
   * @return the first complete TupleBatch in this buffer, or null if none is ready.
   */
  public final TupleBatch popFilled() {
    if (readyTuples.size() > 0) {
      updateLastPopedTime();
      if (readyTuples.get(0).size() > 0) {
        readyTuplesNum -= readyTuples.get(0).get(0).size();
      }
      List<Column<?>> cols = readyTuples.remove(0);
      if (cols.size() > 0) {
        return new TupleBatch(schema, cols, cols.get(0).size());
      } else {
        return TupleBatch.eoiTupleBatch(schema);
      }
    }
    return null;
  }

  /**
   * Pop filled as list of columns. Avoid the overhead of creating TupleBatch instances if needed such as in many tests.
   * 
   * @return list of columns popped or null if no filled tuples ready yet.
   * */
  public final List<Column<?>> popFilledAsRawColumn() {
    if (readyTuples.size() > 0) {
      updateLastPopedTime();
      if (readyTuples.get(0).size() > 0) {
        readyTuplesNum -= readyTuples.get(0).get(0).size();
      }
      return readyTuples.remove(0);
    }
    return null;
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
   * Append a complete tuple coming from two tuple batches: left and right. Used in join operators.
   * 
   * @param leftTb the left tuple batch
   * @param leftIdx the index of the left tuple in the tuple batch
   * @param leftAnswerColumns an array that specifies which columns from the left tuple batch
   * @param rightTb the right tuple batch
   * @param rightIdx the index of the right tuple in the tuple batch
   * @param rightAnswerColumns an array that specifies which columns from the right tuple batch
   * 
   */
  public final void put(final TupleBatch leftTb, final int leftIdx, final int[] leftAnswerColumns,
      final TupleBatch rightTb, final int rightIdx, final int[] rightAnswerColumns) {
    for (int i = 0; i < leftAnswerColumns.length; ++i) {
      leftTb.getDataColumns().get(leftAnswerColumns[i]).append(leftTb.getValidIndices().get(leftIdx),
          currentBuildingColumns.get(i));
    }
    for (int i = 0; i < rightAnswerColumns.length; ++i) {
      rightTb.getDataColumns().get(rightAnswerColumns[i]).append(rightTb.getValidIndices().get(rightIdx),
          currentBuildingColumns.get(i + leftAnswerColumns.length));
    }
    currentInProgressTuples++;
    if (currentInProgressTuples == TupleBatch.BATCH_SIZE) {
      finishBatch();
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
    ColumnBuilder<?> dest = currentBuildingColumns.get(destColumn);
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
   * Update lastPopedTime to be the current time.
   */
  private void updateLastPopedTime() {
    lastPoppedTime = System.nanoTime();
  }

}
