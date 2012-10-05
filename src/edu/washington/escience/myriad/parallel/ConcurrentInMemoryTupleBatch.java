package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Schema.TDItem;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.table._TupleBatch;

// Not yet @ThreadSafe
public class ConcurrentInMemoryTupleBatch implements _TupleBatch {

  private static final long serialVersionUID = 1L;

  public static final int BATCH_SIZE = 100;

  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE1 = 243;
  /** Class-specific magic number used to generate the hash code. */
  private static final int MAGIC_HASHCODE2 = 67;

  private final Schema inputSchema;
  private final String[] outputColumnNames;
  private final List<Column> inputColumns;
  private int numInputTuples;
  private final BitSet invalidTuples;
  private final BitSet invalidColumns;

  public ConcurrentInMemoryTupleBatch(final Schema inputSchema) {
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
    final Iterator<TDItem> it = inputSchema.iterator();
    int i = 0;
    while (it.hasNext()) {
      this.outputColumnNames[i++] = it.next().getName();
    }
  }

  public ConcurrentInMemoryTupleBatch(final Schema inputSchema, final List<Column> columns, final int numTuples) {
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
    final Iterator<TDItem> it = inputSchema.iterator();
    int i = 0;
    while (it.hasNext()) {
      this.outputColumnNames[i++] = it.next().getName();
    }
  }

  // /**
  // * Only for copy
  // */
  // protected ConcurrentInMemoryTupleBatch(Schema inputSchema, String[] outputColumnNames,
  // List<Column> inputColumns, int numInputTuples, BitSet invalidTuples, BitSet invalidColumns) {
  // this.inputSchema = inputSchema;
  // this.outputColumnNames = outputColumnNames;
  // this.inputColumns = inputColumns;
  // this.numInputTuples = numInputTuples;
  // this.invalidTuples = invalidTuples;
  // this.invalidColumns = invalidColumns;
  // }

  @Override
  public synchronized _TupleBatch append(final _TupleBatch another) {
    // TODO implement
    Preconditions.checkArgument(this.inputSchema.equals(another.outputSchema()),
        "Another tuplebatch should have the output schema the same as the inputschema of this tuplebatch");

    final List<Column> anotherData = another.outputRawData();
    final int anotherNumTuples = another.numOutputTuples();
    this.numInputTuples += anotherNumTuples;
    if (this.inputColumns.size() <= 0) {
      this.inputColumns.addAll(anotherData);
    } else {
      for (int i = 0; i < anotherData.size(); i++) {
        final Column anotherColumn = anotherData.get(i);
        final Column thisColumn = this.inputColumns.get(i);
        for (int j = 0; j < anotherNumTuples; j++) {
          thisColumn.putObject(anotherColumn.get(j));
        }
      }
    }

    return this;
  }

  @Override
  public synchronized _TupleBatch distinct() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch except(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized ConcurrentInMemoryTupleBatch filter(final int fieldIdx, final Predicate.Op op, final Object operand) {
    if (!this.invalidColumns.get(fieldIdx) && this.numInputTuples > 0) {
      final Column columnValues = this.inputColumns.get(fieldIdx);
      final Type columnType = this.inputSchema.getFieldType(fieldIdx);
      for (int i = invalidTuples.nextClearBit(0); i >= 0 && i < this.numInputTuples; i =
          invalidTuples.nextClearBit(i + 1)) {
        if (!columnType.filter(op, columnValues, i, operand)) {
          invalidTuples.set(i);
        }
      }
    }
    return this;
  }

  @Override
  public synchronized boolean getBoolean(final int column, final int row) {
    return ((BooleanColumn) inputColumns.get(column)).getBoolean(row);
  }

  @Override
  public synchronized double getDouble(final int column, final int row) {
    return ((DoubleColumn) inputColumns.get(column)).getDouble(row);
  }

  @Override
  public synchronized float getFloat(final int column, final int row) {
    return ((FloatColumn) inputColumns.get(column)).getFloat(row);
  }

  @Override
  public synchronized int getInt(final int column, final int row) {
    return ((IntColumn) inputColumns.get(column)).getInt(row);
  }

  @Override
  public synchronized long getLong(final int column, final int row) {
    return ((LongColumn) inputColumns.get(column)).getLong(row);
  }

  @Override
  public synchronized String getString(final int column, final int row) {
    return ((StringColumn) inputColumns.get(column)).getString(row);
  }

  @Override
  public synchronized _TupleBatch groupby() {
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
  public synchronized _TupleBatch intersect(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized _TupleBatch join(final _TupleBatch other, final Predicate p, final _TupleBatch output) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized int numInputTuples() {
    return numInputTuples;
  }

  @Override
  public synchronized int numOutputTuples() {
    return this.numInputTuples - this.invalidTuples.cardinality();
  }

  @Override
  public synchronized _TupleBatch orderby() {
    // TODO Auto-generated method stub
    return null;
  }

  protected synchronized int[] outputColumnIndices() {
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public synchronized Schema outputSchema() {

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
  public TupleBatchBuffer[] partition(final PartitionFunction<?, ?> pf, final TupleBatchBuffer[] buffers) {
    // p.partition(t, td)
    final List<Column> outputData = this.outputRawData();
    final Schema s = this.outputSchema();
    final int numColumns = outputData.size();

    final int[] partitions = pf.partition(outputData, s);

    for (int i = 0; i < partitions.length; i++) {
      final int p_of_tuple = partitions[i];
      for (int j = 0; j < numColumns; j++) {
        buffers[p_of_tuple].put(j, outputData.get(j).get(i));
      }
    }
    return buffers;
  }

  @Override
  public synchronized ConcurrentInMemoryTupleBatch project(final int[] remainingColumns) {
    final boolean[] columnsToRemain = new boolean[this.inputSchema.numFields()];
    Arrays.fill(columnsToRemain, false);
    for (final int toRemainIdx : remainingColumns) {
      columnsToRemain[toRemainIdx] = true;
    }
    final int numColumns = this.inputSchema.numFields();
    for (int i = invalidColumns.nextClearBit(0); i >= 0 && i < numColumns; i = invalidColumns.nextClearBit(i + 1)) {
      if (!columnsToRemain[i]) {
        this.invalidColumns.set(i);
      }
    }
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
  public ConcurrentInMemoryTupleBatch remove(final int innerIdx) {
    if (innerIdx < this.numInputTuples && innerIdx >= 0) {
      invalidTuples.set(innerIdx);
    }
    return this;
  }

  @Override
  public synchronized _TupleBatch renameColumn(final int inputColumnIdx, final String newName) {
    this.outputColumnNames[inputColumnIdx] = newName;
    return this;
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
  public synchronized _TupleBatch union(final _TupleBatch another) {
    // TODO Auto-generated method stub
    return null;
  }

}
