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
      if (columnTypes[columnIndex] == Type.INT_TYPE) {
        columns.add(new IntColumn());
      } else if (columnTypes[columnIndex] == Type.STRING_TYPE) {
        columns.add(new StringColumn());
      } else {
        throw new UnsupportedOperationException("type " + columnTypes[columnIndex]);
      }
    }
    return columns;
  }
}
