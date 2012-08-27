package edu.washington.escience;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import edu.washington.escience.parallel.PartitionFunction;

public class TupleBatch {
  public static final int BATCH_SIZE = 100;

  private final Schema schema;
  private final List<Column> columns;
  private final int numTuples;
  private final BitSet validTuples;
  private final BitSet validColumns;

  public TupleBatch(Schema schema, List<Column> columns, int numTuples) {
    /* Take the input arguments directly */
    this.schema = Objects.requireNonNull(schema);
    this.columns = Objects.requireNonNull(columns);
    this.numTuples = Objects.requireNonNull(numTuples);
    /* All tuples are valid */
    this.validTuples = new BitSet(BATCH_SIZE);
    validTuples.set(0, numTuples);
    /* All columns are valid */
    this.validColumns = new BitSet(schema.numFields());
    validColumns.set(0, schema.numFields());
  }

  /**
   * @param fieldIdx the index of all columns, not the currently valid columns
   * */
  public TupleBatch filter(int fieldIdx, Predicate.Op op, Object operand) {
    if (this.validColumns.get(fieldIdx) && this.numTuples > 0) {
      Column columnValues = this.columns.get(fieldIdx);
      Type columnType = this.schema.getFieldType(fieldIdx);
      int nextSet = -1;
      while ((nextSet = this.validTuples.nextSetBit(nextSet + 1)) >= 0) {
        if (!columnType.filter(op, columnValues, nextSet, operand)) {
          this.validTuples.clear(nextSet);
        }
      }
    }
    return this;
  }

  public boolean getBoolean(int column, int row) {
    return ((BooleanColumn) columns.get(column)).getBoolean(row);
  }

  public double getDouble(int column, int row) {
    return ((DoubleColumn) columns.get(column)).getDouble(row);
  }

  public float getFloat(int column, int row) {
    return ((FloatColumn) columns.get(column)).getFloat(row);
  }

  public int getInt(int column, int row) {
    return ((IntColumn) columns.get(column)).getInt(row);
  }

  public Schema getSchema() {
    return schema;
  }

  public String getString(int column, int row) {
    return ((StringColumn) columns.get(column)).getString(row);
  }

  public int numTuples() {
    return numTuples;
  }

  public TupleBatch[] partition(PartitionFunction<?, ?> p) {
    return null;
  }

  public TupleBatch project(int[] remainingColumns) {
    for (int columnIdx : remainingColumns) {
      this.validColumns.clear(columnIdx);
    }
    return this;
  }

  public TupleBatch project(Integer[] remainingColumns) {
    this.validColumns.clear();
    for (int columnIdx : remainingColumns) {
      this.validColumns.set(columnIdx);
    }
    return this;
  }

  @Override
  public String toString() {
    // int nextSet = -1;
    int[] columnIndices = this.validColumnIndices();
    String[] columnNames = new String[columnIndices.length];
    Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (int columnIndx : columnIndices) {
      columnNames[j] = this.schema.getFieldName(columnIndx);
      columnTypes[j] = this.schema.getFieldType(columnIndx);
      j++;
    }

    StringBuilder sb = new StringBuilder();
    for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
      sb.append("|\t");
      for (j = 0; j < columnIndices.length; j++) {
        sb.append(columnTypes[j].toString(this.columns.get(columnIndices[j]), i));
        sb.append("\t|\t");

      }
      sb.append("\n");
    }
    return sb.toString();

  }

  public int[] validColumnIndices() {
    int[] validC = new int[validColumns.cardinality()];
    int j = 0;
    for (int i = validColumns.nextSetBit(0); i >= 0; i = validColumns.nextSetBit(i + 1)) {
      // operate on index i here
      validC[j++] = i;
    }
    return validC;
  }

  public int[] validTupleIndices() {
    int[] validT = new int[validTuples.cardinality()];
    int j = 0;
    for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
      // operate on index i here
      validT[j++] = i;
    }
    return validT;
  }

  public Schema validSchema() {
    int[] columnIndices = this.validColumnIndices();
    String[] columnNames = new String[columnIndices.length];
    Type[] columnTypes = new Type[columnIndices.length];
    int j = 0;
    for (int columnIndx : columnIndices) {
      columnNames[j] = this.schema.getFieldName(columnIndx);
      columnTypes[j] = this.schema.getFieldType(columnIndx);
      j++;
    }
    return new Schema(columnTypes, columnNames);
  }

}
