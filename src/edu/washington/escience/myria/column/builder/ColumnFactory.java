package edu.washington.escience.myria.column.builder;

import java.util.ArrayList;
import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

/**
 * A column of a batch of tuples.
 *
 *
 */
public final class ColumnFactory {

  /**
   * Allocate a ColumnBuilder for the specified Myria type.
   *
   * @param type the Myria type of the returned Builder.
   * @return a ColumnBuilder for the specified Myria type.
   */
  public static ColumnBuilder<?> allocateColumn(final Type type) {
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanColumnBuilder();
      case DOUBLE_TYPE:
        return new DoubleColumnBuilder();
      case FLOAT_TYPE:
        return new FloatColumnBuilder();
      case INT_TYPE:
        return new IntColumnBuilder();
      case LONG_TYPE:
        return new LongColumnBuilder();
      case STRING_TYPE:
        return new StringColumnBuilder();
      case DATETIME_TYPE:
        return new DateTimeColumnBuilder();
    }
    throw new IllegalArgumentException("Cannot allocate a ColumnBuilder for unknown type " + type);
  }

  /**
   * Allocates an array of Columns to match the given Schema.
   *
   * @param schema the Schema
   * @return the list of Columns
   */
  public static List<ColumnBuilder<?>> allocateColumns(final Schema schema) {
    return allocateColumns(schema.getColumnTypes());
  }

  /**
   * Allocates an array of Columns to match the given Type array.
   *
   * @param columnTypes the Types of the columns
   * @return the allocated Columns
   */
  public static List<ColumnBuilder<?>> allocateColumns(final List<Type> columnTypes) {
    final ArrayList<ColumnBuilder<?>> columns = new ArrayList<ColumnBuilder<?>>(columnTypes.size());
    for (Type type : columnTypes) {
      columns.add(allocateColumn(type));
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
  public static Column<?> columnFromColumnMessage(
      final ColumnMessage message, final int numTuples) {
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
      case DATETIME:
        return DateTimeColumnBuilder.buildFromProtobuf(message, numTuples);
    }
    return null;
  }

  /** Inaccessible. */
  private ColumnFactory() {
    throw new AssertionError();
  }
}
