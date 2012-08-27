package edu.washington.escience.myriad;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import com.google.common.primitives.Ints;

import edu.washington.escience.myriad.parallel.PartitionFunction;

public class TupleBatch {
  public static final int BATCH_SIZE = 100;

  private final Schema schema;
  private final List<Column> columns;
  private final int numTuples;
  private final BitSet validTuples;

  public TupleBatch(Schema schema, List<Column> columns, int numTuples) {
    /* Take the input arguments directly */
    this.schema = Objects.requireNonNull(schema);
    this.columns = Objects.requireNonNull(columns);
    this.numTuples = Objects.requireNonNull(numTuples);
    /* All tuples are valid */
    this.validTuples = new BitSet(BATCH_SIZE);
    validTuples.set(0, numTuples);
  }

  private TupleBatch(Schema schema, List<Column> columns, int numTuples, BitSet validTuples) {
    /* Take the input arguments directly */
    this.schema = Objects.requireNonNull(schema);
    this.columns = Objects.requireNonNull(columns);
    this.numTuples = Objects.requireNonNull(numTuples);
    this.validTuples = (BitSet) validTuples.clone();
  }

  /**
   * @param fieldIdx the index of all columns, not the currently valid columns
   * */
  public TupleBatch filter(int fieldIdx, Predicate.Op op, Object operand) {
    if (this.numTuples > 0) {
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

  public int numColumns() {
    return schema.numFields();
  }

  public TupleBatch[] partition(PartitionFunction<?, ?> p) {
    return null;
  }

  public TupleBatch project(int[] remainingColumns) {
    List<Column> newColumns = new ArrayList<Column>();
    Type[] newTypes = new Type[remainingColumns.length];
    String[] newNames = new String[remainingColumns.length];
    int count = 0;
    for (int i : remainingColumns) {
      newColumns.add(columns.get(i));
      newTypes[count] = schema.getFieldType(remainingColumns[count]);
      newNames[count] = schema.getFieldName(remainingColumns[count]);
      count++;
    }
    return new TupleBatch(new Schema(newTypes, newNames), newColumns, numTuples, validTuples);
  }

  public TupleBatch project(Integer[] remainingColumns) {
    return project(Ints.toArray(Arrays.asList(remainingColumns)));
  }

  @Override
  public String toString() {
    Type[] columnTypes = schema.getTypes();

    StringBuilder sb = new StringBuilder();
    for (int i = validTuples.nextSetBit(0); i >= 0; i = validTuples.nextSetBit(i + 1)) {
      sb.append("|\t");
      for (int j = 0; j < schema.numFields(); j++) {
        sb.append(columnTypes[j].toString(columns.get(j), i));
        sb.append("\t|\t");
      }
      sb.append("\n");
    }
    return sb.toString();

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
}
