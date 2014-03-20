package edu.washington.escience.myria;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import net.jcip.annotations.ThreadSafe;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.parallel.PartitionFunction;
import edu.washington.escience.myria.proto.TransportProto.TransportMessage;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * Container class for a batch of tuples. The goal is to amortize memory management overhead.
 * 
 */
@ThreadSafe
public class TupleBatch extends ReadableTable implements Serializable {
  /** Required for Java serialization. */
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
  /** Number of tuples in this TB. */
  private final int numTuples;
  /** Whether this TB is an EOI TB. */
  private final boolean isEOI;

  /**
   * EOI TB constructor.
   * 
   * @param schema schema of the tuples in this batch.
   * @param isEoi whether this TupleBatch is an EOI TupleBatch.
   * */
  private TupleBatch(final Schema schema, final boolean isEoi) {
    this.schema = schema;
    numTuples = 0;
    ImmutableList.Builder<Column<?>> b = ImmutableList.builder();
    for (Type type : schema.getColumnTypes()) {
      b.add(Column.emptyColumn(type));
    }
    columns = b.build();
    isEOI = isEoi;
  }

  /**
   * @param columnNames the new column names.
   * @return a shallow copy of the specified TupleBatch with the new column names.
   */
  public TupleBatch rename(final List<String> columnNames) {
    Schema newSchema = Schema.of(schema.getColumnTypes(), columnNames);
    return new TupleBatch(newSchema, columns, numTuples, isEOI);
  }

  /**
   * Standard immutable TupleBatch constructor. All fields must be populated before creation and cannot be changed.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns contains the column-stored data. Must match schema.
   * @param numTuples the number of tuples in this TupleBatch.
   */
  public TupleBatch(final Schema schema, final List<Column<?>> columns, final int numTuples) {
    this(schema, columns, numTuples, false);
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
   * Construct a TupleBatch from the specified components.
   * 
   * @param schema schema of the tuples in this batch. Must match columns.
   * @param columns schema of the tuples in this batch. Must match columns.
   * @param numTuples the number of tuples in this batch. Must match columns.
   * @param isEOI whether this is an EOI TupleBatch.
   */
  public TupleBatch(final Schema schema, final List<Column<?>> columns, final int numTuples, final boolean isEOI) {
    this.schema = Objects.requireNonNull(schema);
    Objects.requireNonNull(columns);
    Preconditions.checkArgument(columns.size() == schema.numColumns(),
        "Number of columns in data must equal the number of fields in schema");
    this.columns = ImmutableList.copyOf(columns);
    this.numTuples = numTuples;
    this.isEOI = isEOI;
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
    for (int i = 0; i < numTuples; i++) {
      for (int column = 0; column < numColumns; column++) {
        tbb.put(column, columns.get(column), i);
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
    Preconditions.checkArgument(filter.length() <= numTuples(),
        "Error: trying to filter a TupleBatch of length %s with a filter of length %s", numTuples(), filter.length());
    int newNumTuples = filter.cardinality();

    /* Shortcut: the filter is full, so all current tuples are retained. Just return this. */
    if (newNumTuples == numTuples) {
      return this;
    }

    ImmutableList.Builder<Column<?>> newColumns = ImmutableList.builder();
    for (Column<?> column : columns) {
      newColumns.add(column.filter(filter));
    }
    return new TupleBatch(schema, newColumns.build(), newNumTuples, isEOI);
  }

  @Override
  public final boolean getBoolean(final int column, final int row) {
    return columns.get(column).getBoolean(row);
  }

  @Override
  public final double getDouble(final int column, final int row) {
    return columns.get(column).getDouble(row);
  }

  @Override
  public final float getFloat(final int column, final int row) {
    return columns.get(column).getFloat(row);
  }

  @Override
  public final int getInt(final int column, final int row) {
    return columns.get(column).getInt(row);
  }

  /**
   * store this TB into JDBC.
   * 
   * @param statement JDBC statement.
   * @throws SQLException any exception caused by JDBC.
   * */
  public final void getIntoJdbc(final PreparedStatement statement) throws SQLException {
    for (int i = 0; i < numTuples; i++) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoJdbc(i, statement, ++column);
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
    for (int i = 0; i < numTuples; i++) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoSQLite(i, statement, ++column);
      }
      statement.step();
      statement.reset();
    }
  }

  @Override
  public final long getLong(final int column, final int row) {
    return columns.get(column).getLong(row);
  }

  @Override
  public final Object getObject(final int column, final int row) {
    return columns.get(column).getObject(row);
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  @Override
  public final String getString(final int column, final int row) {
    return columns.get(column).getString(row);
  }

  @Override
  public final DateTime getDateTime(final int column, final int row) {
    return columns.get(column).getDateTime(row);
  }

  /**
   * @param row the row to be hashed.
   * @return the hash of the tuples in the specified row.
   */
  public final int hashCode(final int row) {
    Hasher hasher = HASH_FUNCTION.newHasher();
    for (Column<?> c : columns) {
      c.addToHasher(row, hasher);
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
    for (final int i : hashColumns) {
      Column<?> c = columns.get(i);
      c.addToHasher(row, hasher);
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
    Column<?> c = columns.get(hashColumn);
    c.addToHasher(row, hasher);
    return hasher.hash().asInt();
  }

  @Override
  public final int numColumns() {
    return schema.numColumns();
  }

  @Override
  public final int numTuples() {
    return numTuples;
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

    BitSet[] resultBitSet = new BitSet[result.length];
    for (int i = 0; i < partitions.length; i++) {
      int p = partitions[i];
      if (resultBitSet[p] == null) {
        resultBitSet[p] = new BitSet(result.length);
      }
      resultBitSet[p].set(i);
    }

    for (int i = 0; i < result.length; i++) {
      if (resultBitSet[i] != null) {
        result[i] = filter(resultBitSet[i]);
      }
    }
    return result;
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
    return new TupleBatch(resultSchema, newColumns.build(), numTuples, isEOI);
  }

  /**
   * @param rows a BitSet flagging the rows to be removed.
   * @return a new TB with the specified rows removed.
   * */
  public final TupleBatch filterOut(final BitSet rows) {
    BitSet inverted = (BitSet) rows.clone();
    inverted.flip(0, numTuples);
    return filter(inverted);
  }

  @Override
  public final String toString() {
    if (isEOI) {
      return "EOI";
    }
    final List<Type> columnTypes = schema.getColumnTypes();
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < numTuples; i++) {
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
   * @return the data columns.
   */
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
  public final boolean tupleMatches(final int leftIdx, final int[] leftCompareIdx, final TupleBatch rightTb,
      final int rightIdx, final int[] rightCompareIdx) {
    for (int i = 0; i < leftCompareIdx.length; ++i) {
      if (!columns.get(leftCompareIdx[i]).equals(leftIdx, rightTb.columns.get(rightCompareIdx[i]), rightIdx)) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return a TransportMessage encoding the TupleBatch.
   * */
  public final TransportMessage toTransportMessage() {
    return IPCUtils.normalDataMessage(columns, numTuples);
  }

  /**
   * Create an EOI TupleBatch.
   * 
   * @param schema schema.
   * @return EOI TB for the schema.
   * */
  public static final TupleBatch eoiTupleBatch(final Schema schema) {
    return new TupleBatch(schema, true);
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
  public boolean tupleEquals(final int row, final ReadableTable hashTable, final int index,
      final int[] compareColumns1, final int[] compareColumns2) {
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
  public boolean tupleEquals(final int row, final ReadableTable hashTable, final int index, final int[] compareColumns) {
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
  public boolean tupleEquals(final int row, final ReadableTable hashTable, final int index) {
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
  public int tupleCompare(final int[] columnCompareIndexes, final int rowIdx, final ReadableTable otherTb,
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
   * Same as {@link #tupleCompare(int[], int, TupleBatch, int[], int, boolean[])} but comparison within the same TB.
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

  /**
   * Construct a new TupleBatch that equals the current batch with the specified column appended. The number of valid
   * tuples in this batch must be the same as the size of the other batch. If this batch is not dense, then
   * 
   * @param columnName the name of the column to be added.
   * @param column the column to be added.
   * @return a new TupleBatch containing the tuples of this column plus the tuples of the other.
   */
  public TupleBatch appendColumn(final String columnName, final Column<?> column) {
    Preconditions.checkArgument(numTuples() == column.size(), "Cannot append column of size %s to batch of size %s",
        column.size(), numTuples());
    Schema newSchema = Schema.appendColumn(schema, column.getType(), columnName);
    List<Column<?>> newColumns = ImmutableList.<Column<?>> builder().addAll(columns).add(column).build();
    return new TupleBatch(newSchema, newColumns, numTuples, isEOI);
  }
}
