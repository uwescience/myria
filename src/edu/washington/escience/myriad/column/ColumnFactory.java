package edu.washington.escience.myriad.column;

import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;

/**
 * A column of a batch of tuples.
 * 
 * @author dhalperi
 * 
 */
public final class ColumnFactory {

  /** Inaccessible. */
  private ColumnFactory() {
    throw new AssertionError();
  }

  /**
   * Allocates an array of Columns to match the given Schema.
   * 
   * @param schema the Schema
   * @return the list of Columns
   */
  public static List<Column> allocateColumns(final Schema schema) {
    int numColumns = schema.numFields();
    Type[] columnTypes = new Type[numColumns];
    for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex) {
      columnTypes[columnIndex] = schema.getFieldType(columnIndex);
    }
    return allocateColumns(columnTypes);
  }

  /**
   * Allocates an array of Columns to match the given Type array.
   * 
   * @param columnTypes the Types of the columns
   * @return the allocated Columns
   */
  public static List<Column> allocateColumns(final Type[] columnTypes) {
    int numColumns = columnTypes.length;
    ArrayList<Column> columns = new ArrayList<Column>(numColumns);

    for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex) {
      switch (columnTypes[columnIndex]) {
        case BOOLEAN_TYPE:
          columns.add(new BooleanColumn());
          break;
        case DOUBLE_TYPE:
          columns.add(new DoubleColumn());
          break;
        case FLOAT_TYPE:
          columns.add(new FloatColumn());
          break;
        case INT_TYPE:
          columns.add(new IntColumn());
          break;
        case LONG_TYPE:
          columns.add(new LongColumn());
          break;
        case STRING_TYPE:
          columns.add(new StringColumn());
          break;
      }
    }
    return columns;
  }
}