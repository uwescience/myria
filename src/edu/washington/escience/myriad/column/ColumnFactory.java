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
  public static List<ColumnBuilder<?>> allocateColumns(final Schema schema) {
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
  public static List<ColumnBuilder<?>> allocateColumns(final Type[] columnTypes) {
    final int numColumns = columnTypes.length;
    final ArrayList<ColumnBuilder<?>> columns = new ArrayList<ColumnBuilder<?>>(numColumns);

    for (int columnIndex = 0; columnIndex < numColumns; ++columnIndex) {
      switch (columnTypes[columnIndex]) {
        case BOOLEAN_TYPE:
          columns.add(new BooleanColumnBuilder());
          break;
        case DOUBLE_TYPE:
          columns.add(new DoubleColumnBuilder());
          break;
        case FLOAT_TYPE:
          columns.add(new FloatColumnBuilder());
          break;
        case INT_TYPE:
          columns.add(new IntColumnBuilder());
          break;
        case LONG_TYPE:
          columns.add(new LongColumnBuilder());
          break;
        case STRING_TYPE:
          columns.add(new StringColumnBuilder());
          break;
      }
    }
    return columns;
  }

  /**
   * Deserializes a ColumnMessage into the appropriate Column.
   * 
   * @param message the ColumnMessage to be deserialized.
   * @param numTuples num tuples in the column message
   * @return a Column of the appropriate type and contents.
   */
  public static Column<?> columnFromColumnMessage(final ColumnMessage message, final int numTuples) {
    switch (message.getType()) {
      case BOOLEAN:
        return BooleanColumnBuilder.buildFromProtobuf(message, numTuples);
      case DOUBLE:
        return DoubleColumnBuilder.buildFromProtobuf(message, numTuples);
      case FLOAT:
        return FloatColumnBuilder.buildFromProtobuf(message, numTuples);
      case INT:
        return IntColumnBuilder.buildFromProtobuf(message, numTuples);
      case LONG:
        return LongColumnBuilder.buildFromProtobuf(message, numTuples);
      case STRING:
        return StringColumnBuilder.buildFromProtobuf(message, numTuples);
    }
    return null;
  }

  /** Inaccessible. */
  private ColumnFactory() {
    throw new AssertionError();
  }
}