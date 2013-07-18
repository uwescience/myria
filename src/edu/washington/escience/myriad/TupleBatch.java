package edu.washington.escience.myriad;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import net.jcip.annotations.ThreadSafe;

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
import edu.washington.escience.myriad.proto.TransportProto.TransportMessage;
import edu.washington.escience.myriad.util.IPCUtils;
import edu.washington.escience.myriad.util.ImmutableBitSet;
import edu.washington.escience.myriad.util.ImmutableIntArray;

/**
 * Container class for a batch of tuples. The goal is to amortize memory management overhead.
 * 
 * @author dhalperi
 * 
 */
@ThreadSafe
public class TupleBatch implements Serializable {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

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

  /**
   * valid indices.
   * */
  private transient ImmutableIntArray validIndices;

  /**
   * If this TB is an EOI TB.
   * */
  private final boolean isEOI;

  /** Identity mapping. */
  protected static final int[] IDENTITY_MAPPING;

  static {
    IDENTITY_MAPPING = new int[BATCH_SIZE];
    for (int i = 0; i < BATCH_SIZE; i++) {
      IDENTITY_MAPPING[i] = i;
    }
  }

  /**
   * Broken-out copy constructor. Shallow copy of the schema, column list, and the number of tuples; deep copy of the
   * valid tuples since that's what we mutate.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param validTuples BitSet determines which tuples are valid tuples in this batch.
   * @param validIndices valid tuple indices.
   * @param isEOI eoi TB.
   */
  protected TupleBatch(final Schema schema, final ImmutableList<Column<?>> columns, final ImmutableBitSet validTuples,
      final ImmutableIntArray validIndices, final boolean isEOI) {
    /** For a private copy constructor, no data checks are needed. Checks are only needed in the public constructor. */
    this.schema = schema;
    this.columns = columns;
    numValidTuples = validTuples.cardinality();
    this.validTuples = validTuples;
    this.validIndices = validIndices;
    this.isEOI = isEOI;
  }

  /**
   * EOI TB constructor.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * */
  private TupleBatch(final Schema schema) {
    validTuples = new ImmutableBitSet(new BitSet());
    this.schema = schema;
    numValidTuples = 0;
    ImmutableList.Builder<Column<?>> b = ImmutableList.builder();
    columns = b.build();
    isEOI = true;
  }

  /**
   * @return if this TB is compact, i.e. tuples occupy from index 0 to numValidTuples-1.
   * */
  public final boolean isCompact() {
    return validTuples.nextClearBit(0) == numValidTuples;
  }

  /**
   * Call this method instead of the copy constructor for a new TupleBatch copy.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param validTuples BitSet determines which tuples are valid tuples in this batch.
   * @param validIndices valid tuple indices.
   * @param isEOI if is EOI
   * @return shallow copy
   */
  protected TupleBatch shallowCopy(final Schema schema, final ImmutableList<Column<?>> columns,
      final ImmutableBitSet validTuples, final ImmutableIntArray validIndices, final boolean isEOI) {
    return new TupleBatch(schema, columns, validTuples, validIndices, isEOI);
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
        "numTuples must be non negative and no more than TupleBatch.BATCH_SIZE");
    numValidTuples = numTuples;
    validIndices = new ImmutableIntArray(Arrays.copyOfRange(IDENTITY_MAPPING, 0, numTuples));
    /* All tuples are valid */
    final BitSet tmp = new BitSet(numTuples);
    tmp.set(0, numTuples);
    validTuples = new ImmutableBitSet(tmp);
    isEOI = false;
  }

  /**
   * Standard immutable TupleBatch constructor. All fields must be populated before creation and cannot be changed.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param validTuples the valid tuple BitSet
   */
  public TupleBatch(final Schema schema, final List<Column<?>> columns, final ImmutableBitSet validTuples) {
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
    numValidTuples = validTuples.cardinality();
    this.validTuples = validTuples;
    isEOI = false;
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
    ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      for (int column = 0; column < numColumns; column++) {
        tbb.put(column, columns.get(column).get(indices.get(i)));
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
      if (newValidTuples.nextClearBit(0) == newValidTuples.cardinality()) {
        // compact
        return new TupleBatch(schema, columns, newValidTuples.cardinality());
      } else {
        return shallowCopy(schema, columns, new ImmutableBitSet(newValidTuples), null, isEOI);
      }
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
    ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoJdbc(indices.get(i), statement, ++column);
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
    ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoSQLite(indices.get(i), statement, ++column);
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
    ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      int row = indices.get(i);
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
      c.addToHasher(mappedRow, hasher);
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
      c.addToHasher(mappedRow, hasher);
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
    c.addToHasher(mappedRow, hasher);
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

    ImmutableIntArray indices = getValidIndices();

    for (int i = 0; i < partitions.length; i++) {
      final int pOfTuple = partitions[i];
      final int mappedI = indices.get(i);
      for (int j = 0; j < numColumns; j++) {
        buffers[pOfTuple].put(j, columns.get(j).get(mappedI));
      }
    }
  }

  /**
   * Partition this TB using the partition function. The method is implemented by shallow copy of TupleBatches.
   * 
   * @return an array of TBs. The length of the array is the same as the number of partitions. If no tuple presents in a
   *         partition, say the i'th partition, the i'th element in the result array is null.
   * @param pf the partition function.
   * */
  public final TupleBatch[] partition(final PartitionFunction<?, ?> pf) {
    TupleBatch[] result = new TupleBatch[pf.numPartition()];
    if (isEOI) {
      Arrays.fill(result, this);
      return result;
    }

    final int[] partitions = pf.partition(this);
    final ImmutableIntArray mapping = getValidIndices();

    BitSet[] resultBitSet = new BitSet[result.length];
    for (int i = 0; i < partitions.length; i++) {
      int p = partitions[i];
      int actualRow = mapping.get(i);
      if (resultBitSet[p] == null) {
        resultBitSet[p] = new BitSet(actualRow + 1);
      }
      resultBitSet[p].set(actualRow);
    }

    for (int i = 0; i < result.length; i++) {
      if (resultBitSet[i] != null) {
        result[i] = shallowCopy(schema, columns, new ImmutableBitSet(resultBitSet[i]), null, isEOI);
      }
    }
    return result;
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
    final ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      int dest = hashCode(indices.get(i), hashColumns) % destinations.length;
      /* hashCode can be negative, so wrap positive if necessary */
      if (dest < destinations.length) {
        dest += destinations.length;
      }
      appendTupleInto(indices.get(i), destinations[dest]);
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
    return shallowCopy(new Schema(newTypes, newNames), newColumns.build(), validTuples, validIndices, isEOI);
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
    final ImmutableIntArray indices = getValidIndices();
    final BitSet newValidTuples = validTuples.cloneAsBitSet();
    for (int i = tupleIndicesToRemove.nextSetBit(0); i >= 0; i = tupleIndicesToRemove.nextSetBit(i + 1)) {
      newValidTuples.clear(indices.get(i));
    }
    if (newValidTuples.cardinality() != numValidTuples) {
      return shallowCopy(schema, columns, new ImmutableBitSet(newValidTuples), null, isEOI);
    } else {
      return this;
    }
  }

  @Override
  public final String toString() {
    if (isEOI) {
      return "EOI";
    }
    final List<Type> columnTypes = schema.getColumnTypes();
    final StringBuilder sb = new StringBuilder();
    final ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      sb.append("|\t");
      for (int j = 0; j < schema.numColumns(); j++) {
        sb.append(columnTypes.get(j).toString(columns.get(j), indices.get(i)));
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
  public final ImmutableIntArray getValidIndices() {
    if (validIndices != null) {
      return validIndices;
    }

    int[] validIndicesTmp = new int[numValidTuples];
    int i = 0;
    for (int valid = validTuples.nextSetBit(0); valid >= 0; valid = validTuples.nextSetBit(valid + 1)) {
      validIndicesTmp[i] = valid;
      i++;
    }
    if (validIndices == null) {
      validIndices = new ImmutableIntArray(validIndicesTmp);
    }
    return validIndices;
  }

  /**
   * @return the data columns. Work together with the valid indices.
   * */
  public final ImmutableList<Column<?>> getDataColumns() {
    return columns;
  }

  /**
   * @return expose valid tuple BitSet.
   * */
  public final ImmutableBitSet getValidTuples() {
    return validTuples;
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
  public final boolean tupleMatches(final int leftIdx, final int[] leftCompareIdx, final TupleBatch rightTb,
      final int rightIdx, final int[] rightCompareIdx) {
    for (int i = 0; i < leftCompareIdx.length; ++i) {
      if (!columns.get(leftCompareIdx[i]).equals(getValidIndices().get(leftIdx),
          rightTb.columns.get(rightCompareIdx[i]), rightTb.getValidIndices().get(rightIdx))) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return a TransportMessage encoding the TupleBatch.
   * */
  public final TransportMessage toTransportMessage() {
    if (isCompact()) {
      return IPCUtils.normalDataMessage(columns, numValidTuples);
    } else {
      TupleBatchBuffer tbb = new TupleBatchBuffer(getSchema());
      compactInto(tbb);
      return IPCUtils.normalDataMessage(tbb.popAnyAsRawColumn(), numValidTuples);
    }
  }

  /**
   * Create an EOI TupleBatch.
   * 
   * @param schema schema.
   * @return EOI TB for the schema.
   * */
  public static final TupleBatch eoiTupleBatch(final Schema schema) {
    return new TupleBatch(schema);
  }

  /**
   * @return if the TupleBatch is an EOI.
   * */
  public final boolean isEOI() {
    return isEOI;
  }
}
