package edu.washington.escience.myriad;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.tuple.Pair;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.util.ReadOnlyBitSet;

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
  private static final int MAGIC_HASHCODE1 = 243;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 67;

  /** Schema of tuples in this batch. */
  private final Schema schema;
  /** Tuple data stored as columns in this batch. */
  private final List<Column<?>> columns;
  /** Number of valid tuples in this TB. */
  private final int numValidTuples;
  /** Which tuples are valid in this batch. */
  private final ReadOnlyBitSet validTuples;
  // /** Which tuples are valid in this batch light read only version. */
  // private final ReadOnlyBitSet validTuplesRO;
  /** An int[] view of the indices of validTuples. */
  private int[] validIndices;

  // private final boolean distincted = false;

  /** Identity mapping. */
  protected static final int[] IDENTITY_MAPPING;

  static {
    final int[] tmp = new int[BATCH_SIZE];
    for (int i = 0; i < BATCH_SIZE; i++) {
      tmp[i] = i;
    }
    IDENTITY_MAPPING = tmp;
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
  private TupleBatch(final Schema schema, final List<Column<?>> columns, final BitSet validTuples,
      final int[] validIndices) {
    /** For a private copy constructor, no data checks are needed. Checks are only needed in the public constructor. */
    this.schema = schema;
    this.columns = columns;

    numValidTuples = validTuples.cardinality();

    if (validTuples instanceof ReadOnlyBitSet) {
      this.validTuples = (ReadOnlyBitSet) validTuples.clone();
    } else {
      this.validTuples = new ReadOnlyBitSet((BitSet) validTuples.clone());
    }

    // validTuplesRO = new ReadOnlyBitSet(this.validTuples);
    this.validIndices = validIndices;
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
    this.columns = Objects.requireNonNull(columns);
    Preconditions.checkArgument(columns.size() == schema.numFields(),
        "Number of columns in data must equal to the number of fields in schema");
    Preconditions.checkArgument(numTuples >= 0 && numTuples <= BATCH_SIZE,
        "numTuples must be at least 1 and no more than TupleBatch.BATCH_SIZE");
    numValidTuples = numTuples;
    validIndices = IDENTITY_MAPPING;
    /* All tuples are valid */
    final BitSet tmp = new BitSet(numTuples);
    tmp.set(0, numTuples);
    validTuples = new ReadOnlyBitSet(tmp);
    // validTuplesRO = new ReadOnlyBitSet(validTuples);
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
   * Helper function that mutates this TupleBatch to remove all rows that do not match the specified predicate. WARNING:
   * do not use! Use the non-mutating version.
   * 
   * @param column column on which the predicate operates.
   * @param op predicate by which to test rows.
   * @param operand operand to the predicate.
   * @return a new TupleBatch where all rows that do not match the specified predicate have been removed.
   */
  private TupleBatch applyFilter(final int column, final Predicate.Op op, final Object operand) {
    BitSet newValidTuples = null;
    if (numValidTuples > 0) {
      final Column<?> columnValues = columns.get(column);
      final Type columnType = schema.getFieldType(column);
      final int[] mapping = validTupleIndices();
      for (final int validIdx : mapping) {
        if (!columnType.filter(op, columnValues, validIdx, operand)) {
          if (newValidTuples == null) {
            newValidTuples = validTuples.cloneAsBitSet();
          }
          newValidTuples.clear(validIdx);
        }
      }
    }

    if (newValidTuples != null) {
      return new TupleBatch(schema, columns, newValidTuples, null);
    }
    // If no tuples are filtered, new TupleBatch instance is not needed
    return this;
  }

  // /**
  // * Do the distinct on this TupleBatch
  // *
  // * @return a new TupleBatch where all rows that do not match the specified predicate have been removed.
  // */
  // private TupleBatch distinct() {
  // boolean indicesMappingChange = false;
  // if (numValidTuples > 0 && !distincted) {
  //
  // BitSet toRemove = new BitSet(numValidTuples);
  // DupElim.IndexedTuple currentTuple = new DupElim.IndexedTuple(this);
  // for (int i = 0; i < numValidTuples; ++i) {
  // currentTuple.index = i;
  // final int cntHashCode = currentTuple.hashCode();
  // // might need to check invalid | change to use outputTuples later
  // List<IndexedTuple> tupleList = uniqueTuples.get(cntHashCode);
  // if (tupleList == null) {
  // tupleList = new ArrayList<IndexedTuple>();
  // uniqueTuples.put(cntHashCode, tupleList);
  // tupleList.add(new IndexedTuple(tb, i));
  // continue;
  // }
  // boolean unique = true;
  // for (IndexedTuple oldTuple : tupleList) {
  // if (currentTuple.equals(oldTuple)) {
  // unique = false;
  // break;
  // }
  // }
  // if (unique) {
  // tupleList.add(new IndexedTuple(tb, i));
  // } else {
  // toRemove.set(i);
  // }
  // }
  // tb.remove(toRemove);
  // return tb;
  //
  // final Column<?> columnValues = columns.get(column);
  // final Type columnType = schema.getFieldType(column);
  // int nextSet = -1;
  // while ((nextSet = validTuples.nextSetBit(nextSet + 1)) >= 0) {
  // if (!columnType.filter(op, columnValues, nextSet, operand)) {
  // validTuples.clear(nextSet);
  // numValidTuples -= 1;
  // indicesMappingChange = true;
  // }
  // }
  // }
  // if (indicesMappingChange) {
  // validIndices = null;
  // }
  // return this;
  // }

  /**
   * put the valid tuples into tbb.
   * 
   * @param tbb the TBB buffer.
   * */
  public final void compactInto(final TupleBatchBuffer tbb) {
    final int numColumns = columns.size();
    final int[] mapping = validTupleIndices();
    for (int row = 0; row < numValidTuples; row++) {
      final int mappedRow = mapping[row];
      for (int column = 0; column < numColumns; column++) {
        tbb.put(column, columns.get(column).get(mappedRow));
      }
    }
  }

  /**
   * Returns a new TupleBatch where all rows that do not match the specified predicate have been removed. Makes a
   * shallow copy of the data, possibly resulting in slowed access to the result, but no copies.
   * 
   * Internal implementation of a SELECT statement.
   * 
   * @param column column on which the predicate operates.
   * @param op predicate by which to test rows.
   * @param operand operand to the predicate.
   * @return a new TupleBatch where all rows that do not match the specified predicate have been removed.
   */
  public final TupleBatch filter(final int column, final Predicate.Op op, final Object operand) {
    Objects.requireNonNull(op);
    Objects.requireNonNull(operand);
    // final TupleBatch ret = new TupleBatch(this);
    return applyFilter(column, op, operand);
  }

  // /**
  // * Returns the column at the specified index.
  // *
  // * @param index index of the desired column.
  // * @return the column at the specified index.
  // */
  // public final Column<?> getColumn(final int index) {
  // return columns.get(index);
  // }

  public final boolean getBoolean(final int column, final int row) {
    return ((BooleanColumn) columns.get(column)).getBoolean(validTupleIndices()[row]);
  }

  public final double getDouble(final int column, final int row) {
    return ((DoubleColumn) columns.get(column)).getDouble(validTupleIndices()[row]);
  }

  public final float getFloat(final int column, final int row) {
    return ((FloatColumn) columns.get(column)).getFloat(validTupleIndices()[row]);
  }

  public final int getInt(final int column, final int row) {
    return ((IntColumn) columns.get(column)).getInt(validTupleIndices()[row]);
  }

  /**
   * store this TB into JDBC.
   * 
   * @param statement JDBC statement.
   * @throws SQLException any exception caused by JDBC.
   * */
  public final void getIntoJdbc(final PreparedStatement statement) throws SQLException {
    final int[] mapping = validTupleIndices();
    for (int row = 0; row < numValidTuples; row++) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoJdbc(mapping[row], statement, ++column);
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
    final int[] mapping = validTupleIndices();
    for (int row = 0; row < numValidTuples; row++) {
      int column = 0;
      for (final Column<?> c : columns) {
        c.getIntoSQLite(mapping[row], statement, ++column);
      }
      statement.step();
      statement.reset();
    }
  }

  public final long getLong(final int column, final int row) {
    return ((LongColumn) columns.get(column)).getLong(validTupleIndices()[row]);
  }

  public final Object getObject(final int column, final int row) {
    return columns.get(column).get(validTupleIndices()[row]);
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
    return ((StringColumn) columns.get(column)).getString(validTupleIndices()[row]);
  }

  public Set<Pair<Object, TupleBatchBuffer>> groupby(final int groupByColumn,
      final Map<Object, Pair<Object, TupleBatchBuffer>> buffers) {
    Set<Pair<Object, TupleBatchBuffer>> ready = null;
    final Column<?> gC = columns.get(groupByColumn);

    final int[] mapping = validTupleIndices();
    for (int i = 0; i < numValidTuples; i++) {
      final Object v = gC.get(mapping[i]);
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
        tbb.put(j++, c.get(mapping[i]));
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

  public final int hashCode(final int row) {
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    final int mappedRow = validTupleIndices()[row];
    for (final Column<?> c : columns) {
      hb.append(c.get(mappedRow));
    }
    return hb.toHashCode();
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
    /*
     * From http://commons.apache.org/lang/api-2.4/org/apache/commons/lang/builder/HashCodeBuilder.html:
     * 
     * You pick a hard-coded, randomly chosen, non-zero, odd number ideally different for each class.
     */
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    final int mappedRow = validTupleIndices()[row];
    for (final int i : hashColumns) {
      hb.append(columns.get(i).get(mappedRow));
    }
    return hb.toHashCode();
  }

  /**
   * The number of columns in this TupleBatch.
   * 
   * @return number of columns in this TupleBatch.
   */
  public final int numColumns() {
    return schema.numFields();
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
   * Get a COPY of valid tuples held by this TupleBatch.
   * 
   * DO NOT use this method if not necessary.
   * 
   * Call get** methods if single cell values are requested. And call {#link compactInto} if the valid tuples are to be
   * output to somewhere using a {#link TupleBatchBuffer}.
   * 
   * @return a copy of valid tuples
   * */
  public final List<Column<?>> outputRawData() {
    final List<Column<?>> output = ColumnFactory.allocateColumns(schema);
    final int[] mapping = validTupleIndices();
    for (int row = 0; row < numValidTuples; row++) {
      final int mappedRow = mapping[row];
      int i = 0;
      for (final Column<?> c : columns) {
        output.get(i++).putObject(c.get(mappedRow));
      }
    }
    return output;
  }

  /**
   * Partition this TB using the partition function.
   * 
   * @param pf the partition function.
   * @param buffers the buffers storing the partitioned data.
   * */
  public final void partition(final PartitionFunction<?, ?> pf, final TupleBatchBuffer[] buffers) {
    final int numColumns = numColumns();

    final int[] partitions = pf.partition(this); // columns, validTuplesRO, schema);

    final int[] mapping = validTupleIndices();
    for (int i = 0; i < partitions.length; i++) {
      final int pOfTuple = partitions[i];
      final int mappedI = mapping[i];
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
    final int[] indexMapping = validTupleIndices();
    for (int i = 0; i < numValidTuples; i++) {
      final int j = indexMapping[i];
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
    final List<Column<?>> newColumns = new ArrayList<Column<?>>();
    for (final int i : remainingColumns) {
      newColumns.add(columns.get(i));
      newTypes.add(schema.getFieldType(i));
      newNames.add(schema.getFieldName(i));
    }
    return new TupleBatch(new Schema(newTypes, newNames), newColumns, validTuples, validIndices);
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
   * @param
   * */
  public final TupleBatch remove(final BitSet tupleIndicesToRemove) {
    final int[] mapping = validTupleIndices();
    final BitSet newValidTuples = validTuples.cloneAsBitSet();
    for (int i = tupleIndicesToRemove.nextSetBit(0); i >= 0; i = tupleIndicesToRemove.nextSetBit(i + 1)) {
      newValidTuples.clear(mapping[i]);
    }
    if (newValidTuples.cardinality() != numValidTuples) {
      return new TupleBatch(schema, columns, newValidTuples, null);
    } else {
      return this;
    }
  }

  @Override
  public final String toString() {
    final ImmutableList<Type> columnTypes = schema.getTypes();
    final StringBuilder sb = new StringBuilder();
    for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
      sb.append("|\t");
      for (int j = 0; j < schema.numFields(); j++) {
        sb.append(columnTypes.get(j).toString(columns.get(j), i));
        sb.append("\t|\t");
      }
      sb.append("\n");
    }
    return sb.toString();

  }

  /**
   * For the representation with a BitSet listing which rows are valid, generate and return an array containing the
   * indices of all valid rows.
   * 
   * Since we are now using index mapping, it's unnecessary to expose the filtered/removed tuples
   * 
   * @return an array containing the indices of all valid rows.
   */
  private int[] validTupleIndices() {
    if (validIndices != null) {
      return validIndices;
    }

    final int[] validT = new int[numValidTuples];
    int j = 0;
    for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
      // operate on index i here
      validT[j++] = i;
    }
    validIndices = validT;
    return validIndices;
  }
}
