package edu.washington.escience.myria;

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

import javax.annotation.Nullable;

import net.jcip.annotations.ThreadSafe;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.DateTimeColumn;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.IntColumn;
import edu.washington.escience.myria.column.LongColumn;
import edu.washington.escience.myria.column.StringColumn;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.util.Constants;
import edu.washington.escience.myria.util.IPCUtils;
import edu.washington.escience.myria.util.ImmutableBitSet;
import edu.washington.escience.myria.util.ImmutableIntArray;

/**
 * Container class for a batch of tuples. The goal is to amortize memory management overhead.
 * 
 * @author dhalperi
 * 
 */
@ThreadSafe
public class TupleBatch implements Serializable {
  /***/
  private static final long serialVersionUID = 1L;

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
  private static int[] identityMapping;

  /**
   * Lazy initialization of identity mapping.
   * 
   * @return the identity mapping
   */
  protected static synchronized int[] getIdentityMapping() {
    if (identityMapping == null) {
      identityMapping = new int[Constants.getBatchSize()];
      for (int i = 0; i < Constants.getBatchSize(); i++) {
        identityMapping[i] = i;
      }
    }
    return identityMapping;
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
   * @param columnNames the new column names.
   * @return a shallow copy of the specified TupleBatch with the new column names.
   */
  public TupleBatch rename(final List<String> columnNames) {
    return shallowCopy(Schema.of(getSchema().getColumnTypes(), columnNames), columns, validTuples, validIndices, isEOI);
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
      final ImmutableBitSet validTuples, @Nullable final ImmutableIntArray validIndices, final boolean isEOI) {
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
    Preconditions.checkArgument(numTuples >= 0 && numTuples <= Constants.getBatchSize(),
        "numTuples must be non negative and no more than Constants.getBatchSize()");
    numValidTuples = numTuples;
    validIndices = new ImmutableIntArray(Arrays.copyOfRange(getIdentityMapping(), 0, numTuples));
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
   * Constructor that gets the number of tuples from the columns.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   */
  public TupleBatch(final Schema schema, final List<Column<?>> columns) {
    this(schema, columns, columns.get(0).size());
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
      buffer.put(i, columns.get(i), mappedRow);
    }
  }

  /**
   * put the tuple batch into TBB by smashing it into cells and putting them one by one.
   * 
   * @param tbb the TBB buffer.
   * */
  public final void compactInto(final TupleBatchBuffer tbb) {
    if (isEOI()) {
      /* an EOI TB has no data */
      tbb.appendTB(this);
      return;
    }
    final int numColumns = columns.size();
    ImmutableIntArray indices = getValidIndices();
    for (int i = 0; i < indices.length(); i++) {
      for (int column = 0; column < numColumns; column++) {
        tbb.put(column, columns.get(column), indices.get(i));
      }
    }
  }

  /**
   * Return a new TupleBatch that contains only the filtered rows of the current dataset. Note that if some of the
   * tuples in this batch are invalid, we will have to map the indices in the specified filter to the "real" indices in
   * the tuple.
   * 
   * @param filter the rows to be retained.
   * @return a TupleBatch that contains only the filtered rows of the current dataset.
   */
  public final TupleBatch filter(final BitSet filter) {
    /* Shortcut 1: the filter is full, so all current tuples are retained. Just return this. */
    if (filter.cardinality() == numTuples()) {
      return this;
    }

    /* Shortcut 2: all current tuples in this batch are valid. filter actually is indexed correctly. */
    if (validTuples.cardinality() == validTuples.size()) {
      return new TupleBatch(getSchema(), getDataColumns(), new ImmutableBitSet(filter));
    }

    /* Okay, we have to do work. */
    BitSet realFilter = new BitSet(validTuples.size());
    int[] realValidIndices = new int[filter.cardinality()];
    int row = 0;
    for (int i = filter.nextSetBit(0); i >= 0; i = filter.nextSetBit(i + 1)) {
      int realIndex = getValidIndices().get(i);
      realFilter.set(realIndex);
      realValidIndices[row] = realIndex;
      ++row;
    }
    return new TupleBatch(getSchema(), getDataColumns(), new ImmutableBitSet(realFilter), new ImmutableIntArray(
        realValidIndices), false);
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
   * Returns the element at the specified column and row position.
   * 
   * @param column column in which the element is stored.
   * @param row row in which the element is stored.
   * @return the element at the specified position in this TupleBatch.
   */
  public final DateTime getDateTime(final int column, final int row) {
    return ((DateTimeColumn) columns.get(column)).getDateTime(getValidIndices().get(row));
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
        tbb.put(j++, c, row);
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
  public final void partition(final PartitionFunction pf, final TupleBatchBuffer[] buffers) {
    final int numColumns = numColumns();

    final int[] partitions = pf.partition(this);

    ImmutableIntArray indices = getValidIndices();

    for (int i = 0; i < partitions.length; i++) {
      final int pOfTuple = partitions[i];
      final int mappedI = indices.get(i);
      for (int j = 0; j < numColumns; j++) {
        buffers[pOfTuple].put(j, columns.get(j), mappedI);
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
  public final TupleBatch[] partition(final PartitionFunction pf) {
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
   * Internal implementation of column selection, like a relational algebra project operator but without duplicate
   * elimination.
   * 
   * @param remainingColumns zero-indexed array of columns to retain.
   * @return a TupleBatch with only the specified columns remaining.
   */
  public final TupleBatch selectColumns(final int[] remainingColumns) {
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
   * Internal implementation of column selection, like a relational algebra project operator but without duplicate
   * elimination.
   * 
   * @param remainingColumns zero-indexed array of columns to retain.
   * @return a TupleBatch with only the specified columns remaining.
   */
  public final TupleBatch selectColumns(final Integer[] remainingColumns) {
    Objects.requireNonNull(remainingColumns);
    return selectColumns(Ints.toArray(Arrays.asList(remainingColumns)));
  }

  /**
   * Creates a new TupleBatch with only the indicated columns.
   * 
   * Internal implementation of a (non-duplicate-eliminating) PROJECT statement.
   * 
   * @param remainingColumns zero-indexed array of columns to retain.
   * @param resultSchema computing a schema every time is usually not necessary
   * @return a projected TupleBatch.
   */
  public final TupleBatch selectColumns(final int[] remainingColumns, final Schema resultSchema) {
    Objects.requireNonNull(remainingColumns);
    final ImmutableList.Builder<Column<?>> newColumns = new ImmutableList.Builder<Column<?>>();
    for (final int i : remainingColumns) {
      newColumns.add(columns.get(i));
    }
    return shallowCopy(resultSchema, newColumns.build(), validTuples, validIndices, isEOI);
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

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @param row row number of the tuple to compare
   * @param compareColumns1 the comparing list of columns of cntTuple
   * @param compareColumns2 the comparing list of columns of hashTable
   * @return true if equals.
   */
  public boolean tupleEquals(final int row, final TupleBuffer hashTable, final int index, final int[] compareColumns1,
      final int[] compareColumns2) {
    if (compareColumns1.length != compareColumns2.length) {
      return false;
    }
    for (int i = 0; i < compareColumns1.length; ++i) {
      switch (schema.getColumnType(compareColumns1[i])) {
        case BOOLEAN_TYPE:
          if (getBoolean(compareColumns1[i], row) != hashTable.getBoolean(compareColumns2[i], index)) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (getDouble(compareColumns1[i], row) != hashTable.getDouble(compareColumns2[i], index)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (getFloat(compareColumns1[i], row) != hashTable.getFloat(compareColumns2[i], index)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (getInt(compareColumns1[i], row) != hashTable.getInt(compareColumns2[i], index)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (getLong(compareColumns1[i], row) != hashTable.getLong(compareColumns2[i], index)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!getString(compareColumns1[i], row).equals(hashTable.getString(compareColumns2[i], index))) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!getDateTime(compareColumns1[i], row).equals(hashTable.getDateTime(compareColumns2[i], index))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * Compare tb use compareColumns against a hashtable only containing columns which need to be compared.
   * 
   * @param row number of the tuple to compare
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @param compareColumns the columns of the tuple which will compare
   * @return true if equals
   */
  public boolean tupleEquals(final int row, final TupleBuffer hashTable, final int index, final int[] compareColumns) {
    if (compareColumns.length != hashTable.numColumns()) {
      return false;
    }
    for (int i = 0; i < compareColumns.length; ++i) {
      switch (schema.getColumnType(i)) {
        case BOOLEAN_TYPE:
          if (getBoolean(compareColumns[i], row) != hashTable.getBoolean(i, index)) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (getDouble(compareColumns[i], row) != hashTable.getDouble(i, index)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (getFloat(compareColumns[i], row) != hashTable.getFloat(i, index)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (getInt(compareColumns[i], row) != hashTable.getInt(i, index)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (getLong(compareColumns[i], row) != hashTable.getLong(i, index)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!getString(compareColumns[i], row).equals(hashTable.getString(i, index))) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!getDateTime(compareColumns[i], row).equals(hashTable.getDateTime(i, index))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * Compare tb against a hash table on all columns.
   * 
   * @param row number of the tuple to compare
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @return true if equals
   */
  public boolean tupleEquals(final int row, final TupleBuffer hashTable, final int index) {
    if (numColumns() != hashTable.numColumns()) {
      return false;
    }
    for (int i = 0; i < numColumns(); ++i) {
      switch (schema.getColumnType(i)) {
        case BOOLEAN_TYPE:
          if (getBoolean(i, row) != hashTable.getBoolean(i, index)) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (getDouble(i, row) != hashTable.getDouble(i, index)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (getFloat(i, row) != hashTable.getFloat(i, index)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (getInt(i, row) != hashTable.getInt(i, index)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (getLong(i, row) != hashTable.getLong(i, index)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!getString(i, row).equals(hashTable.getString(i, index))) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!getDateTime(i, row).equals(hashTable.getDateTime(i, index))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * Compares two cells from this and another tuple batch.
   * 
   * @param columnIdx column to compare
   * @param rowIdx row in this tb
   * @param otherColumnIndex the column to compare in the other tb
   * @param otherTb other tb
   * @param otherRowIdx row in other tb
   * @return the result of compare
   */
  public int cellCompare(final int columnIdx, final int rowIdx, final TupleBatch otherTb, final int otherColumnIndex,
      final int otherRowIdx) {
    Type columnType = schema.getColumnType(columnIdx);
    switch (columnType) {
      case INT_TYPE:
        return Type.compareRaw(getInt(columnIdx, rowIdx), otherTb.getInt(otherColumnIndex, otherRowIdx));

      case FLOAT_TYPE:
        return Type.compareRaw(getFloat(columnIdx, rowIdx), otherTb.getFloat(otherColumnIndex, otherRowIdx));

      case LONG_TYPE:
        return Type.compareRaw(getLong(columnIdx, rowIdx), otherTb.getLong(otherColumnIndex, otherRowIdx));

      case DOUBLE_TYPE:
        return Type.compareRaw(getDouble(columnIdx, rowIdx), otherTb.getDouble(otherColumnIndex, otherRowIdx));

      case BOOLEAN_TYPE:
        return Type.compareRaw(getBoolean(columnIdx, rowIdx), otherTb.getBoolean(otherColumnIndex, otherRowIdx));

      case STRING_TYPE:
        return Type.compareRaw(getString(columnIdx, rowIdx), otherTb.getString(otherColumnIndex, otherRowIdx));

      case DATETIME_TYPE:
        return Type.compareRaw(getDateTime(columnIdx, rowIdx), otherTb.getDateTime(otherColumnIndex, otherRowIdx));
    }

    throw new IllegalStateException("We should not be here.");
  }

  /**
   * Compares a whole tuple with a tuple from another batch. The columns from the two compare indexes are compared in
   * order.
   * 
   * @param columnCompareIndexes the columns from this TB that should be compared with the column of the other TB
   * @param rowIdx row in this TB
   * @param otherTb other TB
   * @param otherCompareIndexes the columns from the other TB that should be compared with the column of this TB
   * @param otherRowIdx row in other TB
   * @param ascending true if the column is ordered ascending
   * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
   *         than the second
   */
  public int tupleCompare(final int[] columnCompareIndexes, final int rowIdx, final TupleBatch otherTb,
      final int[] otherCompareIndexes, final int otherRowIdx, final boolean[] ascending) {
    for (int i = 0; i < columnCompareIndexes.length; i++) {
      int compared = cellCompare(columnCompareIndexes[i], rowIdx, otherTb, otherCompareIndexes[i], otherRowIdx);
      if (compared != 0) {
        if (!ascending[i]) {
          return -compared;
        } else {
          return compared;
        }
      }
    }
    return 0;
  }

  /**
   * Same as {@link #tupleCompare(int[], int, TupleBatch, int[], int, boolean[])} but comparioson within the same TB.
   * 
   * @param columnCompareIndexes the columns from this TB that should be compared with the column of the other TB
   * @param rowIdx row in this TB
   * @param rowIdx2 row in this TB that should be compared to the first one
   * @param ascending true if the column is ordered ascending
   * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
   *         than the second
   */
  public int tupleCompare(final int[] columnCompareIndexes, final int rowIdx, final int rowIdx2,
      final boolean[] ascending) {
    return tupleCompare(columnCompareIndexes, rowIdx, this, columnCompareIndexes, rowIdx2, ascending);
  }
}
