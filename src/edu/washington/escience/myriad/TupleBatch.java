package edu.washington.escience.myriad;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;

import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.util.ImmutableBitSet;
import edu.washington.escience.myriad.util.TypeFunnel;

/**
 * Container class for a batch of tuples. The goal is to amortize memory management overhead.
 * 
 * @author dhalperi
 * 
 */
public class TupleBatch {
  /** The hard-coded number of tuples in a batch. */
  public static final int BATCH_SIZE = 10 * 1000;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE = 243;
  /** The hash function for this class. */
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(MAGIC_HASHCODE);

  /** Schema of tuples in this batch. */
  private final Schema schema;
  /** Tuple data stored as columns in this batch. */
  private final ImmutableList<Column<?>> columns;
  /** Number of valid tuples in this TB. */
  private final int numValidTuples;
  /** Which tuples are valid in this batch. */
  private final ImmutableBitSet validTuples;
  /** An ImmutableList<Integer> view of the indices of validTuples. */
  private ImmutableList<Integer> validIndices;

  /** Identity mapping. */
  protected static final ImmutableList<Integer> IDENTITY_MAPPING;

  static {
    ImmutableList.Builder<Integer> tmp = new ImmutableList.Builder<Integer>();
    for (int i = 0; i < BATCH_SIZE; i++) {
      tmp.add(i);
    }
    IDENTITY_MAPPING = tmp.build();
  }

  /**
   * Broken-out copy constructor. Shallow copy of the schema, column list, and the number of tuples; deep copy of the
   * valid tuples since that's what we mutate.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param validTuples BitSet determines which tuples are valid tuples in this batch.
   * @param validIndices valid tuple indices.
   */
  protected TupleBatch(final Schema schema, final ImmutableList<Column<?>> columns, final ImmutableBitSet validTuples,
      final ImmutableList<Integer> validIndices) {
    /** For a private copy constructor, no data checks are needed. Checks are only needed in the public constructor. */
    this.schema = schema;
    this.columns = columns;

    numValidTuples = validTuples.cardinality();

    this.validTuples = validTuples;

    this.validIndices = validIndices;
  }

  /**
   * Call this method instead of the copy constructor for a new TupleBatch copy.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param validTuples BitSet determines which tuples are valid tuples in this batch.
   * @param validIndices valid tuple indices.
   * @return shallow copy
   */
  protected TupleBatch shallowCopy(final Schema schema, final ImmutableList<Column<?>> columns,
      final ImmutableBitSet validTuples, final ImmutableList<Integer> validIndices) {
    return new TupleBatch(schema, columns, validTuples, validIndices);
  }

  /**
   * Standard immutable TupleBatch constructor. All fields must be populated before creation and cannot be changed.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param numTuples number of tuples in the batch.
   */
  public TupleBatch(final Schema schema, final List<Column<?>> columns, final int numTuples) {
    /* Take the input arguments directly */
    this.schema = Objects.requireNonNull(schema);
    Objects.requireNonNull(columns);
    Preconditions.checkArgument(columns.size() == schema.numColumns(),
        "Number of columns in data must equal to the number of fields in schema");
    if (columns instanceof ImmutableList) {
      this.columns = (ImmutableList<Column<?>>) columns;
    } else {
      this.columns = ImmutableList.copyOf(columns);
    }
    Preconditions.checkArgument(numTuples >= 0 && numTuples <= BATCH_SIZE,
        "numTuples must be at least 1 and no more than TupleBatch.BATCH_SIZE");
    numValidTuples = numTuples;
    validIndices = IDENTITY_MAPPING.subList(0, numTuples);
    /* All tuples are valid */
    final BitSet tmp = new BitSet(numTuples);
    tmp.set(0, numTuples);
    validTuples = new ImmutableBitSet(tmp);
  }

  /**
   * Helper function to append the specified row into the specified TupleBatchBuffer.
   * 
   * @param mappedRow the true row in column list to append to the buffer.
   * @param buffer buffer the row is appended to.
   */
  private void appendTupleInto(final int mappedRow, final TupleBatchBuffer buffer) {
    Objects.requireNonNull(buffer);
    for (int i = 0; i < numColumns(); ++i) {
      buffer.put(i, columns.get(i).get(mappedRow));
    }
  }

  /**
   * put the valid tuples into tbb.
   * 
   * @param tbb the TBB buffer.
   * */
  public final void compactInto(final TupleBatchBuffer tbb) {
    final int numColumns = columns.size();
    for (int row : getValidIndices()) {
      for (int column = 0; column < numColumns; column++) {
        tbb.put(column, columns.get(column).get(row));
      }
    }
  }

  /**
   * Returns a new TupleBatch where all rows that do not match the specified predicate have been removed. Makes a
   * shallow copy of the data, possibly resulting in slowed access to the result, but no copies.
   * 
   * Internal implementation of a SELECT statement.
   * 
   * @param predicate predicate by which to filter rows
   * @return a new TupleBatch where all rows that do not match the specified predicate have been removed.
   */
  public final TupleBatch filter(final Predicate predicate) {
    BitSet newValidTuples = null;
    if (numValidTuples > 0) {
      ImmutableBitSet filterResult = predicate.filter(this);
      newValidTuples = validTuples.cloneAsBitSet();
      newValidTuples.and(filterResult);
    }

    if (newValidTuples != null && newValidTuples.cardinality() != validTuples.cardinality()) {
      return shallowCopy(schema, columns, new ImmutableBitSet(newValidTuples), null);
    }

    /* If no tuples are filtered, new TupleBatch instance is not needed */
    return this;

  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final boolean getBoolean(final int column, final int row) {
    return ((BooleanColumn) columns.get(column)).getBoolean(getValidIndices().get(row));
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final double getDouble(final int column, final int row) {
    return ((DoubleColumn) columns.get(column)).getDouble(getValidIndices().get(row));
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final float getFloat(final int column, final int row) {
    return ((FloatColumn) columns.get(column)).getFloat(getValidIndices().get(row));
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final int getInt(final int column, final int row) {
    return ((IntColumn) columns.get(column)).getInt(getValidIndices().get(row));
  }

  /**
   * store this TB into JDBC.
   * 
   * @param statement JDBC statement.
   * @throws SQLException any exception caused by JDBC.
   * */
  public final void getIntoJdbc(final PreparedStatement statement) throws SQLException {
    for (int row : getValidIndices()) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoJdbc(row, statement, ++column);
      }
      statement.addBatch();
    }
  }

  /**
   * store this TB into SQLite.
   * 
   * @param statement SQLite statement.
   * @throws SQLiteException any exception caused by SQLite.
   * */
  public final void getIntoSQLite(final SQLiteStatement statement) throws SQLiteException {
    for (int row : getValidIndices()) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoSQLite(row, statement, ++column);
      }
      statement.step();
      statement.reset();
    }
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final long getLong(final int column, final int row) {
    return ((LongColumn) columns.get(column)).getLong(getValidIndices().get(row));
  }

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  public final Object getObject(final int column, final int row) {
    return columns.get(column).get(getValidIndices().get(row));
  }

  /**
   * Returns the Schema of the tuples in this batch.
   * 
   * @return the Schema of the tuples in this batch.
   */
  public final Schema getSchema() {
    return schema;
  }

  /**
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  public final String getString(final int column, final int row) {
    return ((StringColumn) columns.get(column)).getString(getValidIndices().get(row));
  }

  /**
   * Do groupby on this TupleBatch and return the set of (GroupByKey, TupleBatchBuffer) pairs which have filled
   * TupleBatches.
   * 
   * @return the set of (GroupByKey, TupleBatchBuffer).
   * @param groupByColumn the column index for doing group by.
   * @param buffers the data buffers for holding the groupby results.
   * */
  public final Set<Pair<Object, TupleBatchBuffer>> groupby(final int groupByColumn,
      final Map<Object, Pair<Object, TupleBatchBuffer>> buffers) {
    Set<Pair<Object, TupleBatchBuffer>> ready = null;
    final Column<?> gC = columns.get(groupByColumn);

    for (int row : getValidIndices()) {
      final Object v = gC.get(row);
      Pair<Object, TupleBatchBuffer> kvPair = buffers.get(v);
      TupleBatchBuffer tbb = null;
      if (kvPair == null) {
        tbb = new TupleBatchBuffer(getSchema());
        kvPair = Pair.of(v, tbb);
        buffers.put(v, kvPair);
      } else {
        tbb = kvPair.getRight();
      }
      int j = 0;
      for (final Column<?> c : columns) {
        tbb.put(j++, c.get(row));
      }
      if (tbb.hasFilledTB()) {
        if (ready == null) {
          ready = new HashSet<Pair<Object, TupleBatchBuffer>>();
        }
        ready.add(kvPair);
      }
    }
    return ready;
  }

  /**
   * @param row the row to be hashed.
   * @return the hash of the tuples in the specified row.
   */
  public final int hashCode(final int row) {
    Hasher hasher = HASH_FUNCTION.newHasher();
    final int mappedRow = getValidIndices().get(row);
    for (Column<?> c : columns) {
      hasher.putObject(c.get(mappedRow), TypeFunnel.INSTANCE);
    }
    return hasher.hash().asInt();
  }

  /**
   * Returns the hash code for the specified tuple using the specified key columns.
   * 
   * @param row row of tuple to hash.
   * @param hashColumns key columns for the hash.
   * @return the hash code value for the specified tuple using the specified key columns.
   */
  public final int hashCode(final int row, final int[] hashColumns) {
    Objects.requireNonNull(hashColumns);
    Hasher hasher = HASH_FUNCTION.newHasher();
    final int mappedRow = getValidIndices().get(row);
    for (final int i : hashColumns) {
      Column<?> c = columns.get(i);
      hasher.putObject(c.get(mappedRow), TypeFunnel.INSTANCE);
    }
    return hasher.hash().asInt();
  }

  /**
   * Returns the hash code for a single cell.
   * 
   * @param row row of tuple to hash.
   * @param hashColumn the key column for the hash.
   * @return the hash code value for the specified tuple using the specified key columns.
   */
  public final int hashCode(final int row, final int hashColumn) {
    Hasher hasher = HASH_FUNCTION.newHasher();
    final int mappedRow = getValidIndices().get(row);
    Column<?> c = columns.get(hashColumn);
    hasher.putObject(c.get(mappedRow), TypeFunnel.INSTANCE);
    return hasher.hash().asInt();
  }

  /**
   * The number of columns in this TupleBatch.
   * 
   * @return number of columns in this TupleBatch.
   */
  public final int numColumns() {
    return schema.numColumns();
  }

  /**
   * Returns the number of valid tuples in this TupleBatch.
   * 
   * @return the number of valid tuples in this TupleBatch.
   */
  public final int numTuples() {
    return numValidTuples;
  }

  /**
   * Partition this TB using the partition function.
   * 
   * @param pf the partition function.
   * @param buffers the buffers storing the partitioned data.
   * */
  public final void partition(final PartitionFunction<?, ?> pf, final TupleBatchBuffer[] buffers) {
    final int numColumns = numColumns();

    final int[] partitions = pf.partition(this);

    final List<Integer> mapping = getValidIndices();
    for (int i = 0; i < partitions.length; i++) {
      final int pOfTuple = partitions[i];
      final int mappedI = mapping.get(i);
      for (int j = 0; j < numColumns; j++) {
        buffers[pOfTuple].put(j, columns.get(j).get(mappedI));
      }
    }
  }

  /**
   * Hash the valid tuples in this batch and partition them into the supplied TupleBatchBuffers. This is a useful helper
   * primitive for, e.g., the Scatter operator.
   * 
   * @param destinations TupleBatchBuffers into which these tuples will be partitioned.
   * @param hashColumns determines the key columns for the hash.
   */
  final void partitionInto(final TupleBatchBuffer[] destinations, final int[] hashColumns) {
    Objects.requireNonNull(destinations);
    Objects.requireNonNull(hashColumns);
    for (int j : getValidIndices()) {
      int dest = hashCode(j, hashColumns) % destinations.length;
      /* hashCode can be negative, so wrap positive if necessary */
      if (dest < destinations.length) {
        dest += destinations.length;
      }
      appendTupleInto(j, destinations[dest]);
    }
  }

  /**
   * Creates a new TupleBatch with only the indicated columns.
   * 
   * Internal implementation of a (non-duplicate-eliminating) PROJECT statement.
   * 
   * @param remainingColumns zero-indexed array of columns to retain.
   * @return a projected TupleBatch.
   */
  public final TupleBatch project(final int[] remainingColumns) {
    Objects.requireNonNull(remainingColumns);
    final ImmutableList.Builder<Type> newTypes = new ImmutableList.Builder<Type>();
    final ImmutableList.Builder<String> newNames = new ImmutableList.Builder<String>();
    final ImmutableList.Builder<Column<?>> newColumns = new ImmutableList.Builder<Column<?>>();
    for (final int i : remainingColumns) {
      newColumns.add(columns.get(i));
      newTypes.add(schema.getColumnType(i));
      newNames.add(schema.getColumnName(i));
    }
    return shallowCopy(new Schema(newTypes, newNames), newColumns.build(), validTuples, validIndices);
  }

  /**
   * Creates a new TupleBatch with only the indicated columns.
   * 
   * Internal implementation of a (non-duplicate-eliminating) PROJECT statement.
   * 
   * @param remainingColumns zero-indexed array of columns to retain.
   * @return a projected TupleBatch.
   */
  public final TupleBatch project(final Integer[] remainingColumns) {
    Objects.requireNonNull(remainingColumns);
    return project(Ints.toArray(Arrays.asList(remainingColumns)));
  }

  /**
   * @param tupleIndicesToRemove the indices to remove
   * @return a new TB.
   * */
  public final TupleBatch remove(final BitSet tupleIndicesToRemove) {
    final List<Integer> mapping = getValidIndices();
    final BitSet newValidTuples = validTuples.cloneAsBitSet();
    for (int i = tupleIndicesToRemove.nextSetBit(0); i >= 0; i = tupleIndicesToRemove.nextSetBit(i + 1)) {
      newValidTuples.clear(mapping.get(i));
    }
    if (newValidTuples.cardinality() != numValidTuples) {
      return shallowCopy(schema, columns, new ImmutableBitSet(newValidTuples), null);
    } else {
      return this;
    }
  }

  @Override
  public final String toString() {
    final List<Type> columnTypes = schema.getColumnTypes();
    final StringBuilder sb = new StringBuilder();
    for (final int i : getValidIndices()) {
      sb.append("|\t");
      for (int j = 0; j < schema.numColumns(); j++) {
        sb.append(columnTypes.get(j).toString(columns.get(j), i));
        sb.append("\t|\t");
      }
      sb.append('\n');
    }
    return sb.toString();

  }

  /**
   * For the representation with a BitSet listing which rows are valid, generate and return an array containing the
   * indices of all valid rows.
   * 
   * Since we are now using index mapping, it's unnecessary to expose the filtered/removed tuples
   * 
   * @return a list containing the indices of all valid rows.
   */
  public final ImmutableList<Integer> getValidIndices() {
    if (validIndices != null) {
      return validIndices;
    }

    ImmutableList.Builder<Integer> tmp = new ImmutableList.Builder<Integer>();
    for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
      tmp.add(i);
    }
    validIndices = tmp.build();
    return validIndices;
  }

  /**
   * @return the data columns. Work together with the valid indices.
   * */
  public final ImmutableList<Column<?>> getDataColumns() {
    return columns;
  }

  /**
   * Check whether left (in this tuple batch) and right (in the tuple batch rightTb) tuples match w.r.t. columns in
   * leftCompareIndx and rightCompareIndex. This method is used in equi-join operators.
   * 
   * @param leftIdx the index of the left tuple in this tuple batch
   * @param leftCompareIdx an array specifying the columns of the left tuple to be used in the comparison
   * @param rightTb the tuple batch containing the right tuple
   * @param rightIdx the index of the right tuple in the rightTb tuple batch
   * @param rightCompareIdx an array specifying the columns of the right tuple to be used in the comparison
   * @return true if the tuples match
   * */
  public final boolean tupleMatch(final int leftIdx, final int[] leftCompareIdx, final TupleBatch rightTb,
      final int rightIdx, final int[] rightCompareIdx) {
    for (int i = 0; i < leftCompareIdx.length; ++i) {
      if (!columns.get(leftCompareIdx[i]).equals(leftIdx, rightTb.columns.get(rightCompareIdx[i]), rightIdx)) {
        return false;
      }
    }
    return true;
  }

}
