package edu.washington.escience.myriad;

import java.util.ArrayList;
import java.util.List;

public abstract class Column {

  /**
   * Allocates an array of Columns to match the given Schema.
   * 
   * @param schema the Schema
   * @return the list of Columns
   */
  public static List<Column> allocateColumns(Schema schema) {
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
  public static List<Column> allocateColumns(Type[] columnTypes) {
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

  abstract Object get(int row);

  abstract void put(Object value);

  abstract int size();
}