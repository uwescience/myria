package edu.washington.escience.myria.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * Used for creating TupleBatch objects on the fly. A helper class used in, e.g., the Scatter operator. Currently it
 * doesn't support random access to a specific cell. Use TupleBuffer instead.
 *
 *
 */
public class TupleBatchBuffer implements AppendableTable {
  /** Format of the emitted tuples. */
  private final Schema schema;
  /** Convenience constant; must match schema.numColumns() and currentColumns.size(). */
  private final int numColumns;
  /** List of completed TupleBatch objects. */
  private final List<TupleBatch> readyTuples;
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
    readyTuples = new LinkedList<TupleBatch>();
    currentBuildingColumns = ColumnFactory.allocateColumns(schema);
    numColumns = schema.numColumns();
    columnsReady = new BitSet(numColumns);
    numColumnsReady = 0;
    currentInProgressTuples = 0;
    lastPoppedTime = System.nanoTime();
    readyTuplesNum = 0;
  }

  /**
   * Append the tuple batch directly into readyTuples.
   *
   * @param tb the TB.
   */
  public final void appendTB(final TupleBatch tb) {
    /*
     * If we're currently building a batch, we better finish it before we append this one to the list. Otherwise
     * reordering will happen.
     */
    finishBatch();

    readyTuplesNum += tb.numTuples();
    readyTuples.add(tb);
  }

  /**
   * Helper function: checks whether the specified column can be inserted into.
   *
   * @param column the column in which the value should be put.
   */
  private void checkPutIndex(final int column) {
    Preconditions.checkElementIndex(column, numColumns);
    Preconditions.checkState(
        !columnsReady.get(column),
        "need to fill up one row of TupleBatchBuffer before starting new one");
  }

  /**
   * clear this TBB.
   */
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

    /* All columns are full, move to next line. */
    if (numColumnsReady == numColumns) {
      currentInProgressTuples++;
      numColumnsReady = 0;
      columnsReady.clear();
      /* See if the current batch is full and finish it if so. */
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
    Preconditions.checkState(
        numColumnsReady == 0, "Cannot finish a batch with partially-completed tuples");
    if (currentInProgressTuples == 0) {
      return false;
    }

    /* Build the batch */
    List<Column<?>> buildingColumns = new ArrayList<Column<?>>(currentBuildingColumns.size());
    for (ColumnBuilder<?> cb : currentBuildingColumns) {
      buildingColumns.add(cb.build());
    }
    readyTuples.add(new TupleBatch(schema, buildingColumns, currentInProgressTuples));

    /* Update the metadata and refresh the building state. */
    readyTuplesNum += buildingColumns.get(0).size();
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
    final List<TupleBatch> output = new ArrayList<TupleBatch>(readyTuples.size() + 1);
    output.addAll(readyTuples);
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
  public final List<List<? extends Column<?>>> getAllAsRawColumn() {
    final List<List<? extends Column<?>>> output = new ArrayList<>();
    for (final TupleBatch batch : readyTuples) {
      output.add(batch.getDataColumns());
    }
    if (currentInProgressTuples > 0) {
      output.add(getInProgressColumns());
    }
    return output;
  }

  /**
   * Get elapsed time since the last time when a TB is popped.
   *
   * @return the elapsed time from lastPoppedTime to present
   */
  private long getElapsedTime() {
    return System.nanoTime() - lastPoppedTime;
  }

  /**
   * Build the in progress columns. The builders' states are untouched. They can keep building.
   *
   * @return the built in progress columns.
   */
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
  @Override
  public final Schema getSchema() {
    return schema;
  }

  /**
   * @return if there is filled TupleBatches ready for pop.
   */
  public final boolean hasFilledTB() {
    return readyTuples.size() > 0;
  }

  /**
   * @return num columns.
   */
  @Override
  public final int numColumns() {
    return numColumns;
  }

  /**
   * @return the number of complete tuples stored in this TupleBatchBuffer.
   */
  @Override
  public final int numTuples() {
    return readyTuplesNum + currentInProgressTuples;
  }

  /**
   * @return pop filled and non-filled TupleBatch
   */
  public final TupleBatch popAny() {
    final TupleBatch tb = popFilled();
    if (tb != null) {
      updateLastPoppedTime();
      return tb;
    } else {
      if (currentInProgressTuples > 0) {
        final int size = currentInProgressTuples;
        finishBatch();
        updateLastPoppedTime();
        readyTuplesNum -= size;
        TupleBatch batch = readyTuples.remove(0);
        Preconditions.checkState(size == batch.numTuples(), "Error with number of tuples");
        Preconditions.checkState(currentInProgressTuples == 0, "Error with in progress tuples");
        return batch;
      } else {
        return null;
      }
    }
  }

  /**
   * @return pop filled and non-filled TupleBatch
   */
  public final TupleBatch popAnyUsingTimeout() {
    final TupleBatch tb = popFilled();
    if (tb != null) {
      updateLastPoppedTime();
      return tb;
    } else {
      if (currentInProgressTuples > 0 && getElapsedTime() >= MyriaConstants.PUSHING_TB_TIMEOUT) {
        final int size = currentInProgressTuples;
        finishBatch();
        updateLastPoppedTime();
        readyTuplesNum -= size;
        TupleBatch batch = readyTuples.remove(0);
        Preconditions.checkState(size == batch.numTuples(), "Error with number of tuples");
        Preconditions.checkState(currentInProgressTuples == 0, "Error with in progress tuples");
        return batch;
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
      updateLastPoppedTime();
      TupleBatch batch = readyTuples.remove(0);
      readyTuplesNum -= batch.numTuples();
      return batch;
    }
    return null;
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
  public final void put(
      final TupleBatch leftTb,
      final int leftIdx,
      final int[] leftAnswerColumns,
      final TupleBatch rightTb,
      final int rightIdx,
      final int[] rightAnswerColumns) {
    for (int i = 0; i < leftAnswerColumns.length; ++i) {
      TupleUtils.copyValue(
          leftTb.getDataColumns().get(leftAnswerColumns[i]),
          leftIdx,
          currentBuildingColumns.get(i));
    }
    for (int i = 0; i < rightAnswerColumns.length; ++i) {
      TupleUtils.copyValue(
          rightTb.getDataColumns().get(rightAnswerColumns[i]),
          rightIdx,
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
  public final void appendFromColumn(
      final int destColumn, final ReadableColumn sourceColumn, final int sourceRow) {
    TupleUtils.copyValue(sourceColumn, sourceRow, this, destColumn);
  }

  /**
   * Append the referenced row from the source {@link TupleBatch} to this {@link TupleBatchBuffer}.
   *
   * @param tb the source tuple batch.
   * @param row the row index.
   */
  public final void append(final TupleBatch tb, final int row) {
    for (int col = 0; col < tb.numColumns(); ++col) {
      append(tb, col, row);
    }
  }

  /**
   * Append the referenced value from the source {@link TupleBatch} to this {@link TupleBatchBuffer}.
   *
   * @param tb the source tuple batch.
   * @param col the col index.
   * @param row the row index.
   */
  public final void append(final TupleBatch tb, final int col, final int row) {
    appendFromColumn(columnsReady.nextClearBit(0), tb.getDataColumns().get(col), row);
  }

  /**
   * Append the referenced value from the source {@link MutableTupleBuffer} to this {@link TupleBatchBuffer}.
   *
   * @param tuples the source tuple buffer.
   * @param col the column index.
   * @param row the row index.
   */
  public final void append(final MutableTupleBuffer tuples, final int col, final int row) {
    appendFromColumn(
        columnsReady.nextClearBit(0), tuples.getColumn(col, row), row % TupleBatch.BATCH_SIZE);
  }

  /**
   * Append the referenced row from the source {@link MutableTupleBuffer} to this {@link TupleBatchBuffer}.
   *
   * @param tuples the source tuple buffer.
   * @param row the row index.
   */
  public final void append(final MutableTupleBuffer tuples, final int row) {
    for (int col = 0; col < tuples.numColumns(); ++col) {
      append(tuples, col, row);
    }
  }

  @Override
  public final void putBoolean(final int column, final boolean value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendBoolean(value);
    columnPut(column);
  }

  @Override
  public final void putDateTime(final int column, final DateTime value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendDateTime(value);
    columnPut(column);
  }

  @Override
  public final void putDouble(final int column, final double value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendDouble(value);
    columnPut(column);
  }

  @Override
  public final void putFloat(final int column, final float value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendFloat(value);
    columnPut(column);
  }

  @Override
  public final void putInt(final int column, final int value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendInt(value);
    columnPut(column);
  }

  @Override
  public final void putLong(final int column, final long value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendLong(value);
    columnPut(column);
  }

  @Override
  @Deprecated
  public void putObject(final int column, final Object value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendObject(MyriaUtils.ensureObjectIsValidType(value));
    columnPut(column);
  }

  @Override
  public final void putString(final int column, final String value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendString(value);
    columnPut(column);
  }

  @Override
  public final void putByteBuffer(final int column, final ByteBuffer value) {
    checkPutIndex(column);
    currentBuildingColumns.get(column).appendByteBuffer(value);
    columnPut(column);
  }

  /**
   * Update lastPoppedTime to be the current time.
   */
  private void updateLastPoppedTime() {
    lastPoppedTime = System.nanoTime();
  }

  /**
   * Add the specified {@link TupleBatch} to this buffer. The implementation is O(1) when possible, i.e. if the
   * TupleBatch is full and this buffer is not building a partially-complete TupleBatch. Otherwise, it's O(N) in the
   * size of the TupleBatch because it is a full copy.
   *
   * @param tupleBatch the tuple data to be added to this buffer.
   */
  public void absorb(final TupleBatch tupleBatch) {
    if (currentInProgressTuples == 0) {
      appendTB(tupleBatch);
    } else {
      tupleBatch.compactInto(this);
    }
  }

  @Override
  public WritableColumn asWritableColumn(final int column) {
    return new WritableSubColumn(this, column);
  }
}
