package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;

// import edu.washington.escience.Predicate.Op;
import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Schema.TDItem;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.annotation.ThreadSafe;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.FloatColumn;

import edu.washington.escience.myriad.parallel.PartitionFunction;
import edu.washington.escience.myriad.table._TupleBatch;

// Not yet @ThreadSafe
public class ConcurrentInMemoryTupleBatch implements _TupleBatch {

  private static final long serialVersionUID = 1L;

  public static final int BATCH_SIZE = 100;

  private final Schema inputSchema;
  private final String[] outputColumnNames;
  private final List<Column> inputColumns;
  private int numInputTuples;
  private final BitSet invalidTuples;
  private final BitSet invalidColumns;

  public ConcurrentInMemoryTupleBatch(Schema inputSchema) {
    /* Take the input arguments directly */
    this.inputSchema = Objects.requireNonNull(inputSchema);
    this.inputColumns = new ArrayList<Column>();
    this.numInputTuples = 0;
    /* All tuples are valid */
    this.invalidTuples = new BitSet(BATCH_SIZE);
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

  public ConcurrentInMemoryTupleBatch(Schema inputSchema, List<Column> columns, int numTuples) {
    /* Take the input arguments directly */
    this.inputSchema = Objects.requireNonNull(inputSchema);
    this.inputColumns = Objects.requireNonNull(columns);
    this.numInputTuples = numTuples;
    /* All tuples are valid */
    this.invalidTuples = new BitSet(BATCH_SIZE);
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

  // /**
  // * Only for copy
  // * */
  // protected ConcurrentInMemoryTupleBatch(Schema inputSchema, String[] outputColumnNames,
  // List<Column> inputColumns, int numInputTuples, BitSet invalidTuples, BitSet invalidColumns) {
  // this.inputSchema = inputSchema;
  // this.outputColumnNames = outputColumnNames;
  // this.inputColumns = inputColumns;
  // this.numInputTuples = numInputTuples;
  // this.invalidTuples = invalidTuples;
  // this.invalidColumns = invalidColumns;
  // }

  public synchronized ConcurrentInMemoryTupleBatch filter(int fieldIdx, Predicate.Op op, Object operand) {
    if (!this.invalidColumns.get(fieldIdx) && this.numInputTuples > 0) {
      Column columnValues = this.inputColumns.get(fieldIdx);
      Type columnType = this.inputSchema.getFieldType(fieldIdx);
      for (int i = invalidTuples.nextClearBit(0); i >= 0 && i < this.numInputTuples; i =
          invalidTuples.nextClearBit(i + 1)) {
        if (!columnType.filter(op, columnValues, i, operand)) {
          invalidTuples.set(i);
        }
      }
    }
    return this;
  }

  public synchronized boolean getBoolean(int column, int row) {
    return ((BooleanColumn) inputColumns.get(column)).getBoolean(row);
  }

  public synchronized double getDouble(int column, int row) {
    return ((DoubleColumn) inputColumns.get(column)).getDouble(row);
  }

  public synchronized float getFloat(int column, int row) {
    return ((FloatColumn) inputColumns.get(column)).getFloat(row);
  }

  public synchronized int getInt(int column, int row) {
    return ((IntColumn) inputColumns.get(column)).getInt(row);
  }

  public Schema inputSchema() {
    return inputSchema;
  }

  public synchronized String getString(int column, int row) {
    return ((StringColumn) inputColumns.get(column)).getString(row);
  }

  @Override
  public synchronized int numInputTuples() {
    return numInputTuples;
  }

  public synchronized ConcurrentInMemoryTupleBatch project(int[] remainingColumns) {
    boolean[] columnsToRemain = new boolean[this.inputSchema.numFields()];
    Arrays.fill(columnsToRemain, false);
    for (int toRemainIdx : remainingColumns) {
      columnsToRemain[toRemainIdx] = true;
    }
    int numColumns = this.inputSchema.numFields();
    for (int i = invalidColumns.nextClearBit(0); i >= 0 && i < numColumns; i = invalidColumns.nextClearBit(i + 1)) {
      if (!columnsToRemain[i])
        this.invalidColumns.set(i);
    }
    return this;
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

  protected synchronized int[] outputColumnIndices() {
    int numInputColumns = this.inputSchema.numFields();
    int[] validC = new int[numInputColumns - invalidColumns.cardinality()];
    int j = 0;
    for (int i = invalidColumns.nextClearBit(0); i >= 0 && i < numInputColumns; i = invalidColumns.nextClearBit(i + 1)) {
      // operate on index i here
      validC[j++] = i;
    }
    return validC;
  }

  public synchronized Schema outputSchema() {

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
  public synchronized int numOutputTuples() {
    return this.numInputTuples - this.invalidTuples.cardinality();
  }

  @Override
  public synchronized _TupleBatch renameColumn(int inputColumnIdx, String newName) {
    this.outputColumnNames[inputColumnIdx] = newName;
    return this;
  }

  @Override
  public synchronized _TupleBatch purgeFilters() {
    this.invalidTuples.clear();
    return this;
  }

  @Override
  public synchronized _TupleBatch purgeProjects() {
    this.invalidColumns.clear();
    return this;
  }

  @Override
  public synchronized _TupleBatch append(_TupleBatch another) {
    // TODO implement
    Preconditions.checkArgument(this.inputSchema.equals(another.outputSchema()),
        "Another tuplebatch should have the output schema the same as the inputschema of this tuplebatch");

    List<Column> anotherData = another.outputRawData();
    int anotherNumTuples = another.numOutputTuples();
    this.numInputTuples += anotherNumTuples;
    if (this.inputColumns.size() <= 0) {
      this.inputColumns.addAll(anotherData);
    } else {
      for (int i = 0; i < anotherData.size(); i++) {
        Column anotherColumn = anotherData.get(i);
        Column thisColumn = this.inputColumns.get(i);
        for (int j = 0; j < anotherNumTuples; j++)
          thisColumn.putObject(anotherColumn.get(j));
      }
    }

    return this;
  }

  @Override
  public synchronized _TupleBatch union(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch intersect(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch except(_TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch distinct() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch groupby() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch orderby() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch join(_TupleBatch other, Predicate p, _TupleBatch output) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Column> outputRawData() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TupleBatchBuffer[] partition(PartitionFunction<?, ?> pf, TupleBatchBuffer[] buffers) {
//    p.partition(t, td)
    List<Column> outputData = this.outputRawData();
    Schema s = this.outputSchema();
    int numColumns = outputData.size();
    
    int[] partitions = pf.partition(outputData, s);
    
    for (int i=0;i<partitions.length;i++)
    {
      int p_of_tuple = partitions[i];
      for (int j=0;j<numColumns;j++)
      {
        buffers[p_of_tuple].put(j, outputData.get(j).get(i));
      }
    }
    return buffers;
  }

}
