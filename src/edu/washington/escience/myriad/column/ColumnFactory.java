package edu.washington.escience.myriad.column;

import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

/**
 * A column of a batch of tuples.
 * 
 * @author dhalperi
 * 
 */
public final class ColumnFactory {

  /**
   * Allocates an array of Columns to match the given Schema.
   * 
   * @param schema the Schema
   * @return the list of Columns
   */
  public static List<Column<?>> allocateColumns(final Schema schema) {
    final int numColumns = schema.numColumns();
    final Type[] columnTypes = new Type[numColumns];
    for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex) {
      columnTypes[columnIndex] = schema.getColumnType(columnIndex);
    }
    return allocateColumns(columnTypes);
  }

  /**
   * Allocates an array of Columns to match the given Type array.
   * 
   * @param columnTypes the Types of the columns
   * @return the allocated Columns
   */
  public static List<Column<?>> allocateColumns(final Type[] columnTypes) {
    final int numColumns = columnTypes.length;
    final ArrayList<Column<?>> columns = new ArrayList<Column<?>>(numColumns);

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
   * Deserializes a ColumnMessage into the appropriate Column.
   * 
   * @param message the ColumnMessage to be deserialized.
   * @return a Column of the appropriate type and contents.
   */
  public static Column<?> columnFromColumnMessage(final ColumnMessage message) {
    switch (message.getType()) {
      case BOOLEAN:
        return new BooleanColumn(message);
      case DOUBLE:
        return new DoubleColumn(message);
      case FLOAT:
        return new FloatColumn(message);
      case INT:
        return new IntColumn(message);
      case LONG:
        return new LongColumn(message);
      case STRING:
        return new StringColumn(message);
    }
    return null;
  }

  /** Inaccessible. */
  private ColumnFactory() {
    throw new AssertionError();
  }
}