package edu.washington.escience.myriad;

import java.util.ArrayList;
import java.util.List;

/**
 * A column of a batch of tuples.
 * 
 * @author dhalperi
 * 
 */
public abstract class Column {

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

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public abstract Object get(int row);

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  protected abstract void put(Object value);

  /**
   * Returns the number of elements in this column.
   * 
   * @return the number of elements in this column.
   */
  public abstract int size();
}