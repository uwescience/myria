package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

// import edu.washington.escience.Predicate.Op;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Schema.TDItem;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.annotation.ThreadSafe;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.ColumnFactory;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.FloatColumn;

import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.table._TupleBatch;

@ThreadSafe
public class ImmutableInMemoryTupleBatch implements _TupleBatch {

  private static final long serialVersionUID = 1L;

  public static final int BATCH_SIZE = 100;

  private final Schema inputSchema;
  private final String[] outputColumnNames;
  private final List<Column> inputColumns;
  private final int numInputTuples;
  private final BitSet invalidTuples;
  private final BitSet invalidColumns;

  public ImmutableInMemoryTupleBatch(Schema inputSchema, List<Column> columns, int numTuples) {
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
    Iterator<TDItem> it = inputSchema.iterator();
    int i = 0;
    while (it.hasNext())
      this.outputColumnNames[i++] = it.next().getName();
  }

  /**
   * Only for copy
   * */
  protected ImmutableInMemoryTupleBatch(Schema inputSchema, String[] outputColumnNames, List<Column> inputColumns,
      int numInputTuples, BitSet invalidTuples, BitSet invalidColumns) {
    this.inputSchema = inputSchema;
    this.outputColumnNames = outputColumnNames;
    this.inputColumns = inputColumns;
    this.numInputTuples = numInputTuples;
    this.invalidTuples = invalidTuples;
    this.invalidColumns = invalidColumns;
  }

  @Override
  public ImmutableInMemoryTupleBatch filter(int fieldIdx, Predicate.Op op, Object operand) {
    BitSet newInvalidTuples = null;
    if (!this.invalidColumns.get(fieldIdx) && this.numInputTuples > 0) {
      Column columnValues = this.inputColumns.get(fieldIdx);
      Type columnType = this.inputSchema.getFieldType(fieldIdx);
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
  public boolean getBoolean(int column, int row) {
    return ((BooleanColumn) inputColumns.get(column)).getBoolean(row);
  }

  @Override
  public double getDouble(int column, int row) {
    return ((DoubleColumn) inputColumns.get(column)).getDouble(row);
  }

  @Override
  public float getFloat(int column, int row) {
    return ((FloatColumn) inputColumns.get(column)).getFloat(row);
  }

  @Override
  public int getInt(int column, int row) {
    return ((IntColumn) inputColumns.get(column)).getInt(row);
  }

  @Override
  public Schema inputSchema() {
    return inputSchema;
  }

  @Override
  public String getString(int column, int row) {
    return ((StringColumn) inputColumns.get(column)).getString(row);
  }

  @Override
  public int numInputTuples() {
    return numInputTuples;
  }

  @Override
  public ImmutableInMemoryTupleBatch project(int[] remainingColumns) {
    boolean[] columnsToRemain = new boolean[this.inputSchema.numFields()];
    Arrays.fill(columnsToRemain, false);
    for (int toRemainIdx : remainingColumns) {
      columnsToRemain[toRemainIdx] = true;
    }
    BitSet newInvalidColumns = BitSet.valueOf(this.invalidColumns.toByteArray());
    int numColumns = this.inputSchema.numFields();
    for (int i = newInvalidColumns.nextClearBit(0); i >= 0 && i < numColumns; i = newInvalidColumns.nextClearBit(i + 1)) {
      if (!columnsToRemain[i])
        newInvalidColumns.set(i);
    }
    return new ImmutableInMemoryTupleBatch(inputSchema, this.outputColumnNames, inputColumns, numInputTuples, BitSet
        .valueOf(this.invalidTuples.toByteArray()), newInvalidColumns);
  }

  @Override
  public String toString() {
    // int nextSet = -1;
    int[] columnIndices = this.outputColumnIndices();
    String[] columnNames = new String[columnIndices.length];
    Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (int columnIndx : columnIndices) {
      columnNames[j] = this.inputSchema.getFieldName(columnIndx);
      columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
      j++;
    }

    StringBuilder sb = new StringBuilder();
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

  protected int[] outputColumnIndices() {
    int numInputColumns = this.inputSchema.numFields();
    int[] validC = new int[numInputColumns - invalidColumns.cardinality()];
    int j = 0;
    for (int i = invalidColumns.nextClearBit(0); i >= 0 && i < numInputColumns; i = invalidColumns.nextClearBit(i + 1)) {
      // operate on index i here
      validC[j++] = i;
    }
    return validC;
  }

  public Schema outputSchema() {

    int[] columnIndices = this.outputColumnIndices();
    String[] columnNames = new String[columnIndices.length];
    Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (int columnIndx : columnIndices) {
      columnNames[j] = this.inputSchema.getFieldName(columnIndx);
      columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
      j++;
    }

    return new Schema(columnTypes, columnNames);
  }

  @Override
  public int numOutputTuples() {
    return this.numInputTuples - this.invalidTuples.cardinality();
  }

  @Override
  public _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    String[] newOCN = Arrays.copyOf(this.outputColumnNames, this.outputColumnNames.length);
    newOCN[inputColumnIdx] = newName;
    return new ImmutableInMemoryTupleBatch(inputSchema, newOCN, inputColumns, numInputTuples, new BitSet(
        this.numInputTuples), BitSet.valueOf(invalidColumns.toByteArray()));
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
  public _TupleBatch append(_TupleBatch another) {
    // TODO implement
    return null;
  }

  @Override
  public _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch union(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch intersect(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch except(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch distinct() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch groupby() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public _TupleBatch orderby() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Column> outputRawData() {
    if (this.numInputTuples <= 0 || this.numOutputTuples() <= 0)
      return new ArrayList<Column>(0);

    int[] columnIndices = this.outputColumnIndices();
    // String[] columnNames = new String[columnIndices.length];
    Type[] columnTypes = new Type[columnIndices.length];
    {
      int j = 0;
      for (int columnIndx : columnIndices) {
        // columnNames[j] = this.inputSchema.getFieldName(columnIndx);
        columnTypes[j] = this.inputSchema.getFieldType(columnIndx);
        j++;
      }
    }

    List<Column> output = ColumnFactory.allocateColumns(this.outputSchema());
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
  public TupleBatchBuffer[] partition(PartitionFunction<?, ?> p, TupleBatchBuffer[] buffers) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ImmutableInMemoryTupleBatch remove(int innerIdx) {
    if (innerIdx < this.numInputTuples && innerIdx >= 0) {
      BitSet newInvalidTuples = BitSet.valueOf(this.invalidTuples.toByteArray());
      newInvalidTuples.set(innerIdx);
      return new ImmutableInMemoryTupleBatch(inputSchema, outputColumnNames, inputColumns, numInputTuples,
          newInvalidTuples, invalidColumns);
    }
    return this;
  }

}
