package edu.washington.escience.myriad.parallel;

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

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.annotation.ThreadSafe;
import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.table._TupleBatch;

@ThreadSafe
public final class ImmutableInMemoryTupleBatch implements _TupleBatch {

  private static final long serialVersionUID = 1L;

  public static final int BATCH_SIZE = 100;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE1 = 243;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 67;

  private final Schema inputSchema;
  private final String[] outputColumnNames;
  private final List<Column> inputColumns;
  private final int numInputTuples;
  private final BitSet invalidTuples;
  private final BitSet invalidColumns;

  public ImmutableInMemoryTupleBatch(final Schema inputSchema, final List<Column> columns, final int numTuples) {
    /* Take the input arguments directly */
    this.inputSchema = Objects.requireNonNull(inputSchema);
    inputColumns = Objects.requireNonNull(columns);
    numInputTuples = numTuples;
    /* All tuples are valid */
    invalidTuples = new BitSet(numTuples);
    // validTuples.set(0, numTuples);
    /* All columns are valid */
    invalidColumns = new BitSet(inputSchema.numFields());
    // validColumns.set(0, inputSchema.numFields());
    outputColumnNames = inputSchema.getFieldNames();
  }

  public ImmutableInMemoryTupleBatch(final Schema inputSchema, final List<Column> columns, final int numTuples,
      final BitSet invalidTuples) {
    /* Take the input arguments directly */
    this.inputSchema = Objects.requireNonNull(inputSchema);
    inputColumns = Objects.requireNonNull(columns);
    numInputTuples = numTuples;
    /* All tuples are valid */
    this.invalidTuples = invalidTuples;
    // validTuples.set(0, numTuples);
    /* All columns are valid */
    invalidColumns = new BitSet(inputSchema.numFields());
    // validColumns.set(0, inputSchema.numFields());
    outputColumnNames = inputSchema.getFieldNames();
  }

  /**
   * Only for copy
   */
  protected ImmutableInMemoryTupleBatch(final Schema inputSchema, final String[] outputColumnNames,
      final List<Column> inputColumns, final int numInputTuples, final BitSet invalidTuples, final BitSet invalidColumns) {
    this.inputSchema = inputSchema;
    this.outputColumnNames = outputColumnNames;
    this.inputColumns = inputColumns;
    this.numInputTuples = numInputTuples;
    this.invalidTuples = invalidTuples;
    this.invalidColumns = invalidColumns;
  }

  @Override
  public _TupleBatch append(final _TupleBatch another) {
    // TODO implement
    return null;
  }

  @Override
  public _TupleBatch distinct() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch except(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ImmutableInMemoryTupleBatch filter(final int fieldIdx, final Predicate.Op op, final Object operand) {
    BitSet newInvalidTuples = null;
    if (!invalidColumns.get(fieldIdx) && numInputTuples > 0) {
      final Column columnValues = inputColumns.get(fieldIdx);
      final Type columnType = inputSchema.getFieldType(fieldIdx);
      newInvalidTuples = BitSet.valueOf(invalidTuples.toByteArray());
      for (int i = newInvalidTuples.nextClearBit(0); i >= 0 && i < numInputTuples; i =
          newInvalidTuples.nextClearBit(i + 1)) {
        if (!columnType.filter(op, columnValues, i, operand)) {
          newInvalidTuples.set(i);
        }
      }
    }
    return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples,
        newInvalidTuples, invalidColumns);
  }

  @Override
  public boolean getBoolean(final int column, final int row) {
    return ((BooleanColumn) inputColumns.get(column)).getBoolean(row);
  }

  @Override
  public double getDouble(final int column, final int row) {
    return ((DoubleColumn) inputColumns.get(column)).getDouble(row);
  }

  @Override
  public float getFloat(final int column, final int row) {
    return ((FloatColumn) inputColumns.get(column)).getFloat(row);
  }

  @Override
  public int getInt(final int column, final int row) {
    return ((IntColumn) inputColumns.get(column)).getInt(row);
  }

  @Override
  public long getLong(final int column, final int row) {
    return ((LongColumn) inputColumns.get(column)).getLong(row);
  }

  @Override
  public final Object getObject(final int column, final int row) {
    return inputColumns.get(column).get(row);
  }

  @Override
  public String getString(final int column, final int row) {
    return ((StringColumn) inputColumns.get(column)).getString(row);
  }

  @Override
  public Set<Pair<Object, TupleBatchBuffer>> groupby(int groupByColumn,
      Map<Object, Pair<Object, TupleBatchBuffer>> buffers) {
    Set<Pair<Object, TupleBatchBuffer>> ready = null;
    List<Column> columns = outputRawData();
    Column gC = columns.get(groupByColumn);

    int numR = gC.size();
    for (int i = 0; i < numR; i++) {
      Object v = gC.get(i);
      Pair<Object, TupleBatchBuffer> kvPair = buffers.get(v);
      TupleBatchBuffer tbb = null;
      if (kvPair == null) {
        tbb = new TupleBatchBuffer(inputSchema());
        kvPair = Pair.of(v, tbb);
        buffers.put(v, kvPair);
      } else {
        tbb = kvPair.getRight();
      }
      int j = 0;
      for (Column c : columns) {
        tbb.put(j, c.get(i));
        j++;
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

  @Override
  public int hashCode(final int rowIndx) {
    // return 0;
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    for (int i = 0; i < inputSchema.numFields(); ++i) {
      hb.append(inputColumns.get(i).get(rowIndx));
    }
    return hb.toHashCode();
  }

  @Override
  public int hashCode(final int rowIndx, final int[] colIndx) {
    // return 0;
    final HashCodeBuilder hb = new HashCodeBuilder(MAGIC_HASHCODE1, MAGIC_HASHCODE2);
    for (int element : colIndx) {
      hb.append(inputColumns.get(element).get(rowIndx));
    }
    return hb.toHashCode();
  }

  @Override
  public Schema inputSchema() {
    return inputSchema;
  }

  @Override
  public int numInputTuples() {
    return numInputTuples;
  }

  @Override
  public int numOutputTuples() {
    return numInputTuples - invalidTuples.cardinality();
  }

  protected int[] outputColumnIndices() {
    final int numInputColumns = inputSchema.numFields();
    final int[] validC = new int[numInputColumns - invalidColumns.cardinality()];
    int j = 0;
    for (int i = invalidColumns.nextClearBit(0); i >= 0 && i < numInputColumns; i = invalidColumns.nextClearBit(i + 1)) {
      // operate on index i here
      validC[j++] = i;
    }
    return validC;
  }

  @Override
  public List<Column> outputRawData() {
    if (numInputTuples <= 0 || numOutputTuples() <= 0) {
      return new ArrayList<Column>(0);
    }

    final int[] columnIndices = outputColumnIndices();
    // String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    {
      int j = 0;
      for (final int columnIndx : columnIndices) {
        // columnNames[j] = this.inputSchema.getFieldName(columnIndx);
        columnTypes[j] = inputSchema.getFieldType(columnIndx);
        j++;
      }
    }

    final List<Column> output = ColumnFactory.allocateColumns(outputSchema());
    // StringBuilder sb = new StringBuilder();
    for (int i = invalidTuples.nextClearBit(0); i >= 0 && i < numInputTuples; i = invalidTuples.nextClearBit(i + 1)) {
      // sb.append("|\t");
      for (int j = 0; j < columnIndices.length; j++) {
        output.get(j).putObject(inputColumns.get(columnIndices[j]).get(i));
        // sb.append(columnTypes[j].(this.inputColumns.get(columnIndices[j]), i));
        // sb.append("\t|\t");
      }
      // sb.append("\n");
    }
    // return sb.toString();
    // return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns,
    // numInputTuples, newInvalidTuples, invalidColumns);
    // this.invalidTuples
    // this.inputColumns.get(index)
    return output;
  }

  @Override
  public Schema outputSchema() {

    final int[] columnIndices = outputColumnIndices();
    final String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (final int columnIndx : columnIndices) {
      columnNames[j] = inputSchema.getFieldName(columnIndx);
      columnTypes[j] = inputSchema.getFieldType(columnIndx);
      j++;
    }

    return new Schema(columnTypes, columnNames);
  }

  @Override
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> p, final TupleBatchBuffer[] buffers) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ImmutableInMemoryTupleBatch project(final int[] remainingColumns) {
    final boolean[] columnsToRemain = new boolean[inputSchema.numFields()];
    Arrays.fill(columnsToRemain, false);
    for (final int toRemainIdx : remainingColumns) {
      columnsToRemain[toRemainIdx] = true;
    }
    final BitSet newInvalidColumns = BitSet.valueOf(invalidColumns.toByteArray());
    final int numColumns = inputSchema.numFields();
    for (int i = newInvalidColumns.nextClearBit(0); i >= 0 && i < numColumns; i = newInvalidColumns.nextClearBit(i + 1)) {
      if (!columnsToRemain[i]) {
        newInvalidColumns.set(i);
      }
    }
    return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples, BitSet
        .valueOf(invalidTuples.toByteArray()), newInvalidColumns);
  }

  @Override
  public _TupleBatch purgeFilters() {
    return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples, new BitSet(
        numInputTuples), BitSet.valueOf(invalidColumns.toByteArray()));
  }

  @Override
  public _TupleBatch purgeProjects() {
    return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples, BitSet
        .valueOf(invalidTuples.toByteArray()), new BitSet(inputSchema.numFields()));
  }

  @Override
  public ImmutableInMemoryTupleBatch remove(final int innerIdx) {
    if (innerIdx < numInputTuples && innerIdx >= 0) {
      final BitSet newInvalidTuples = BitSet.valueOf(invalidTuples.toByteArray());
      newInvalidTuples.set(innerIdx);
      return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples,
          newInvalidTuples, invalidColumns);
    }
    return this;
  }

  @Override
  public _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    final String[] newOCN = Arrays.copyOf(outputColumnNames, outputColumnNames.length);
    newOCN[inputColumnIdx] = newName;
    return new ImmutableInMemoryTupleBatch(inputSchema, newOCN, inputColumns, numInputTuples,
        new BitSet(numInputTuples), BitSet.valueOf(invalidColumns.toByteArray()));
  }

  @Override
  public String toString() {
    // int nextSet = -1;
    final int[] columnIndices = outputColumnIndices();
    final String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (final int columnIndx : columnIndices) {
      columnNames[j] = inputSchema.getFieldName(columnIndx);
      columnTypes[j] = inputSchema.getFieldType(columnIndx);
      j++;
    }

    final StringBuilder sb = new StringBuilder();
    for (int i = invalidTuples.nextClearBit(0); i >= 0 && i < numInputTuples; i = invalidTuples.nextClearBit(i + 1)) {
      sb.append("|\t");
      for (j = 0; j < columnIndices.length; j++) {
        sb.append(columnTypes[j].toString(inputColumns.get(columnIndices[j]), i));
        sb.append("\t|\t");
      }
      sb.append("\n");
    }
    return sb.toString();

  }

  @Override
  public _TupleBatch union(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

}
