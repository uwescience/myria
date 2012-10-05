package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Schema.TDItem;
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
// import edu.washington.escience.Predicate.Op;

@ThreadSafe
public class ImmutableInMemoryTupleBatch implements _TupleBatch {

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
    this.inputColumns = Objects.requireNonNull(columns);
    this.numInputTuples = numTuples;
    /* All tuples are valid */
    this.invalidTuples = new BitSet(numTuples);
    // validTuples.set(0, numTuples);
    /* All columns are valid */
    this.invalidColumns = new BitSet(inputSchema.numFields());
    // validColumns.set(0, inputSchema.numFields());
    this.outputColumnNames = new String[inputSchema.numFields()];
    final Iterator<TDItem> it = inputSchema.iterator();
    int i = 0;
    while (it.hasNext()) {
      this.outputColumnNames[i++] = it.next().getName();
    }
  }

  public ImmutableInMemoryTupleBatch(final Schema inputSchema, final List<Column> columns, final int numTuples, final BitSet invalidTuples) {
    /* Take the input arguments directly */
    this.inputSchema = Objects.requireNonNull(inputSchema);
    this.inputColumns = Objects.requireNonNull(columns);
    this.numInputTuples = numTuples;
    /* All tuples are valid */
    this.invalidTuples = invalidTuples;
    // validTuples.set(0, numTuples);
    /* All columns are valid */
    this.invalidColumns = new BitSet(inputSchema.numFields());
    // validColumns.set(0, inputSchema.numFields());
    this.outputColumnNames = new String[inputSchema.numFields()];
    final Iterator<TDItem> it = inputSchema.iterator();
    int i = 0;
    while (it.hasNext()) {
      this.outputColumnNames[i++] = it.next().getName();
    }
  }

  /**
   * Only for copy
   * */
  protected ImmutableInMemoryTupleBatch(final Schema inputSchema, final String[] outputColumnNames, final List<Column> inputColumns,
      final int numInputTuples, final BitSet invalidTuples, final BitSet invalidColumns) {
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
    if (!this.invalidColumns.get(fieldIdx) && this.numInputTuples > 0) {
      final Column columnValues = this.inputColumns.get(fieldIdx);
      final Type columnType = this.inputSchema.getFieldType(fieldIdx);
      newInvalidTuples = BitSet.valueOf(this.invalidTuples.toByteArray());
      for (int i = newInvalidTuples.nextClearBit(0); i >= 0 && i < this.numInputTuples; i =
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
  public String getString(final int column, final int row) {
    return ((StringColumn) inputColumns.get(column)).getString(row);
  }

  @Override
  public _TupleBatch groupby() {
    // TODO Auto-generated method stub
    return null;
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
  public Schema inputSchema() {
    return inputSchema;
  }

  @Override
  public _TupleBatch intersect(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch join(final _TupleBatch other, final Predicate p, final _TupleBatch output) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int numInputTuples() {
    return numInputTuples;
  }

  @Override
  public int numOutputTuples() {
    return this.numInputTuples - this.invalidTuples.cardinality();
  }

  @Override
  public _TupleBatch orderby() {
    // TODO Auto-generated method stub
    return null;
  }

  protected int[] outputColumnIndices() {
    final int numInputColumns = this.inputSchema.numFields();
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
    if (this.numInputTuples <= 0 || this.numOutputTuples() <= 0) {
      return new ArrayList<Column>(0);
    }

    final int[] columnIndices = this.outputColumnIndices();
    // String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    {
      int j = 0;
      for (final int columnIndx : columnIndices) {
        // columnNames[j] = this.inputSchema.getFieldName(columnIndx);
        columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
        j++;
      }
    }

    final List<Column> output = ColumnFactory.allocateColumns(this.outputSchema());
    // StringBuilder sb = new StringBuilder();
    for (int i = invalidTuples.nextClearBit(0); i >= 0 && i < this.numInputTuples; i =
        invalidTuples.nextClearBit(i + 1)) {
      // sb.append("|\t");
      for (int j = 0; j < columnIndices.length; j++) {
        output.get(j).putObject(this.inputColumns.get(columnIndices[j]).get(i));
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

    final int[] columnIndices = this.outputColumnIndices();
    final String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (final int columnIndx : columnIndices) {
      columnNames[j] = this.inputSchema.getFieldName(columnIndx);
      columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
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
    final boolean[] columnsToRemain = new boolean[this.inputSchema.numFields()];
    Arrays.fill(columnsToRemain, false);
    for (final int toRemainIdx : remainingColumns) {
      columnsToRemain[toRemainIdx] = true;
    }
    final BitSet newInvalidColumns = BitSet.valueOf(this.invalidColumns.toByteArray());
    final int numColumns = this.inputSchema.numFields();
    for (int i = newInvalidColumns.nextClearBit(0); i >= 0 && i < numColumns; i = newInvalidColumns.nextClearBit(i + 1)) {
      if (!columnsToRemain[i]) {
        newInvalidColumns.set(i);
      }
    }
    return new ImmutableInMemoryTupleBatch(inputSchema, this.outputColumnNames, inputColumns, numInputTuples, BitSet
        .valueOf(this.invalidTuples.toByteArray()), newInvalidColumns);
  }

  @Override
  public _TupleBatch purgeFilters() {
    return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples, new BitSet(
        this.numInputTuples), BitSet.valueOf(invalidColumns.toByteArray()));
  }

  @Override
  public _TupleBatch purgeProjects() {
    return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples, BitSet
        .valueOf(invalidTuples.toByteArray()), new BitSet(this.inputSchema.numFields()));
  }

  @Override
  public ImmutableInMemoryTupleBatch remove(final int innerIdx) {
    if (innerIdx < this.numInputTuples && innerIdx >= 0) {
      final BitSet newInvalidTuples = BitSet.valueOf(this.invalidTuples.toByteArray());
      newInvalidTuples.set(innerIdx);
      return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples,
          newInvalidTuples, invalidColumns);
    }
    return this;
  }

  @Override
  public _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    final String[] newOCN = Arrays.copyOf(this.outputColumnNames, this.outputColumnNames.length);
    newOCN[inputColumnIdx] = newName;
    return new ImmutableInMemoryTupleBatch(inputSchema, newOCN, inputColumns, numInputTuples, new BitSet(
        this.numInputTuples), BitSet.valueOf(invalidColumns.toByteArray()));
  }

  @Override
  public String toString() {
    // int nextSet = -1;
    final int[] columnIndices = this.outputColumnIndices();
    final String[] columnNames = new String[columnIndices.length];
    final Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (final int columnIndx : columnIndices) {
      columnNames[j] = this.inputSchema.getFieldName(columnIndx);
      columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
      j++;
    }

    final StringBuilder sb = new StringBuilder();
    for (int i = invalidTuples.nextClearBit(0); i >= 0 && i < this.numInputTuples; i =
        invalidTuples.nextClearBit(i + 1)) {
      sb.append("|\t");
      for (j = 0; j < columnIndices.length; j++) {
        sb.append(columnTypes[j].toString(this.inputColumns.get(columnIndices[j]), i));
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
