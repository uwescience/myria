package edu.washington.escience.myria.storage;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A simple collection of tuples that provides random access to the inner tuples and can be appended to. Once the user
 * is done appending, they may fetch the {@link #finalResult} of the {@link #TupleBuffer}, which finalizes it and they
 * may access its built contents as a list of {@link TupleBatch}. After finalizing, the {@link TupleBuffer} can no
 * longer have values appended to it.
 */
public class TupleBuffer implements ReadableTable, AppendableTable {
  /** Format of the emitted tuples. */
  private final Schema schema;
  /** Convenience constant; must match schema.numColumns() and currentColumns.size(). */
  private final int numColumns;
  /** List of completed TupleBatch objects. */
  private final List<TupleBatch> readyBatches;
  /** Internal state used to build up a TupleBatch. */
  private List<ColumnBuilder<?>> currentBatch;
  /** Internal state representing the number of columns that are ready in the current tuple. */
  private int numColumnsReady;
  /** Internal state representing which columns are ready in the current tuple. */
  private final BitSet columnsReady;
  /** Internal state representing the number of tuples in the in-progress TupleBatch. */
  private int currentBatchSize;
  /** Whether this buffer has been finalized. */
  private boolean finalized;
  /** The final results of thus buffer. Null until finalized. */
  private ImmutableList<TupleBatch> finalBatches;
  /** The number of tuples in this buffer. */
  private int numTuples;

  /**
   * Constructs an empty TupleBuffer to hold tuples matching the specified Schema.
   *
   * @param schema specified the columns of the emitted TupleBatch objects.
   */
  public TupleBuffer(final Schema schema) {
    this.schema = Objects.requireNonNull(schema, "schema");
    numColumns = schema.numColumns();
    Preconditions.checkArgument(numColumns > 0, "cannot create a buffer with no columns");
    readyBatches = new ArrayList<>();
    currentBatch = ColumnFactory.allocateColumns(schema);
    columnsReady = new BitSet(numColumns);
    numColumnsReady = 0;
    currentBatchSize = 0;
    finalized = false;
    numTuples = 0;
  }

  /**
   * Makes a batch of any tuples in the buffer and appends it to the internal list.
   *
   */
  private void finishBatch() {
    Preconditions.checkState(
        currentBatchSize == TupleBatch.BATCH_SIZE,
        "cannot finish a batch with %s < %s rows ready",
        currentBatchSize,
        TupleBatch.BATCH_SIZE);
    finishBatchEvenIfSmall();
    currentBatch = ColumnFactory.allocateColumns(schema);
  }

  /**
   * Actually finish the batch. Does not ensure that the batch is full, and thus can only be used when finalizing.
   */
  private void finishBatchEvenIfSmall() {
    Preconditions.checkState(
        numColumnsReady == 0,
        "cannot finish a batch with with %s != 0 columns ready",
        numColumnsReady);
    Preconditions.checkState(!finalized, "cannot force finish a batch once finalized");
    if (currentBatchSize == 0) {
      return;
    }
    ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
    for (ColumnBuilder<?> cb : currentBatch) {
      columns.add(cb.build());
    }
    readyBatches.add(new TupleBatch(schema, columns.build()));
    currentBatchSize = 0;
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public final int numTuples() {
    return numTuples;
  }

  @Override
  @Deprecated
  public final Object getObject(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getObject(column, localRow);
    }
    return currentBatch.get(column).getObject(localRow);
  }

  @Override
  public final boolean getBoolean(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getBoolean(column, localRow);
    }
    return currentBatch.get(column).getBoolean(localRow);
  }

  @Override
  public final DateTime getDateTime(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getDateTime(column, localRow);
    }
    return currentBatch.get(column).getDateTime(localRow);
  }

  @Override
  public final double getDouble(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getDouble(column, localRow);
    }
    return currentBatch.get(column).getDouble(localRow);
  }

  @Override
  public final float getFloat(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getFloat(column, localRow);
    }
    return currentBatch.get(column).getFloat(localRow);
  }

  @Override
  public final long getLong(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getLong(column, localRow);
    }
    return currentBatch.get(column).getLong(localRow);
  }

  @Override
  public final int getInt(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getInt(column, localRow);
    }
    return currentBatch.get(column).getInt(localRow);
  }

  @Override
  public final String getString(final int column, final int row) {
    Preconditions.checkElementIndex(row, numTuples());
    int batchIndex = row / TupleBatch.BATCH_SIZE;
    int localRow = row % TupleBatch.BATCH_SIZE;
    if (batchIndex < readyBatches.size()) {
      return readyBatches.get(batchIndex).getString(column, localRow);
    }
    return currentBatch.get(column).getString(localRow);
  }

  @Override
  public final int numColumns() {
    return numColumns;
  }

  @Override
  public final void putBoolean(final int column, final boolean value) {
    checkPutIndex(column);
    currentBatch.get(column).appendBoolean(value);
    columnPut(column);
  }

  @Override
  public final void putDateTime(final int column, final DateTime value) {
    checkPutIndex(column);
    currentBatch.get(column).appendDateTime(value);
    columnPut(column);
  }

  @Override
  public final void putDouble(final int column, final double value) {
    checkPutIndex(column);
    currentBatch.get(column).appendDouble(value);
    columnPut(column);
  }

  @Override
  public final void putFloat(final int column, final float value) {
    checkPutIndex(column);
    currentBatch.get(column).appendFloat(value);
    columnPut(column);
  }

  @Override
  public final void putInt(final int column, final int value) {
    checkPutIndex(column);
    currentBatch.get(column).appendInt(value);
    columnPut(column);
  }

  @Override
  public final void putLong(final int column, final long value) {
    checkPutIndex(column);
    currentBatch.get(column).appendLong(value);
    columnPut(column);
  }

  @Override
  @Deprecated
  public final void putObject(final int column, final Object value) {
    checkPutIndex(column);
    currentBatch.get(column).appendObject(MyriaUtils.ensureObjectIsValidType(value));
    columnPut(column);
  }

  @Override
  public final void putString(final int column, final String value) {
    checkPutIndex(column);
    currentBatch.get(column).appendString(value);
    columnPut(column);
  }

  /**
   * Helper function: checks whether the specified column can be inserted into.
   *
   * @param column the column in which the value should be put.
   */
  private void checkPutIndex(final int column) {
    Preconditions.checkState(!finalized, "cannot append to a TupleBuffer once finalized");
    Preconditions.checkElementIndex(column, numColumns);
    Preconditions.checkState(
        !columnsReady.get(column), "need to fill up one row before starting new one");
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
      currentBatchSize++;
      numTuples++;
      numColumnsReady = 0;
      columnsReady.clear();
      if (currentBatchSize == TupleBatch.BATCH_SIZE) {
        finishBatch();
      }
    }
  }

  /**
   * @return a list of all {@link TupleBatch}es in this buffer.
   */
  public ImmutableList<TupleBatch> finalResult() {
    if (finalized) {
      return finalBatches;
    }
    finishBatchEvenIfSmall();
    finalBatches = ImmutableList.copyOf(readyBatches);
    finalized = true;
    return finalBatches;
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
