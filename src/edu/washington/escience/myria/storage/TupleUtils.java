package edu.washington.escience.myria.storage;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.ColumnBuilder;

/**
 * Utility functions for dealing with tuples.
 */
public final class TupleUtils {
  /** Utility class cannot be instantiated. */
  private TupleUtils() {
  }

  /**
   * Copy the specified from a {@link ReadableColumn} to a {@link AppendableTable}.
   * 
   * @param from the source of the value
   * @param fromRow the row of the source value
   * @param to the destination of the value
   * @param toColumn the destination column
   */
  public static void copyValue(final ReadableColumn from, final int fromRow, final ColumnBuilder<?> to,
      final int toColumn) {
    Type t = from.getType();
    switch (t) {
      case BOOLEAN_TYPE:
        to.appendBoolean(from.getBoolean(fromRow));
        break;
      case DATETIME_TYPE:
        to.appendDateTime(from.getDateTime(fromRow));
        break;
      case DOUBLE_TYPE:
        to.appendDouble(from.getDouble(fromRow));
        break;
      case FLOAT_TYPE:
        to.appendFloat(from.getFloat(fromRow));
        break;
      case INT_TYPE:
        to.appendInt(from.getInt(fromRow));
        break;
      case LONG_TYPE:
        to.appendLong(from.getLong(fromRow));
        break;
      case STRING_TYPE:
        to.appendString(from.getString(fromRow));
        break;
    }
  }

  /**
   * Copy the specified from a {@link ReadableColumn} to a {@link AppendableTable}.
   * 
   * @param from the source of the value
   * @param fromRow the row of the source value
   * @param to the destination of the value
   * @param toColumn the destination column
   */
  public static void copyValue(final ReadableColumn from, final int fromRow, final AppendableTable to,
      final int toColumn) {
    Type t = from.getType();
    switch (t) {
      case BOOLEAN_TYPE:
        to.putBoolean(toColumn, from.getBoolean(fromRow));
        break;
      case DATETIME_TYPE:
        to.putDateTime(toColumn, from.getDateTime(fromRow));
        break;
      case DOUBLE_TYPE:
        to.putDouble(toColumn, from.getDouble(fromRow));
        break;
      case FLOAT_TYPE:
        to.putFloat(toColumn, from.getFloat(fromRow));
        break;
      case INT_TYPE:
        to.putInt(toColumn, from.getInt(fromRow));
        break;
      case LONG_TYPE:
        to.putLong(toColumn, from.getLong(fromRow));
        break;
      case STRING_TYPE:
        to.putString(toColumn, from.getString(fromRow));
        break;
    }
  }

  /**
   * Copy the specified from a {@link ReadableTable} to a {@link ColumnBuilder}.
   * 
   * @param from the source of the value
   * @param fromColumn the column of the source value
   * @param fromRow the row of the source value
   * @param to the destination of the value
   * @param toColumn the destination column
   */
  public static void copyValue(final ReadableTable from, final int fromColumn, final int fromRow,
      final ColumnBuilder<?> to, final int toColumn) {
    Type t = from.getSchema().getColumnType(fromColumn);
    switch (t) {
      case BOOLEAN_TYPE:
        to.appendBoolean(from.getBoolean(fromColumn, fromRow));
        break;
      case DATETIME_TYPE:
        to.appendDateTime(from.getDateTime(fromColumn, fromRow));
        break;
      case DOUBLE_TYPE:
        to.appendDouble(from.getDouble(fromColumn, fromRow));
        break;
      case FLOAT_TYPE:
        to.appendFloat(from.getFloat(fromColumn, fromRow));
        break;
      case INT_TYPE:
        to.appendInt(from.getInt(fromColumn, fromRow));
        break;
      case LONG_TYPE:
        to.appendLong(from.getLong(fromColumn, fromRow));
        break;
      case STRING_TYPE:
        to.appendString(from.getString(fromColumn, fromRow));
        break;
    }
  }

  /**
   * Copy the specified from a {@link ReadableTable} to a {@link AppendableTable}.
   * 
   * @param from the source of the value
   * @param fromColumn the column of the source value
   * @param fromRow the row of the source value
   * @param to the destination of the value
   * @param toColumn the destination column
   */
  public static void copyValue(final ReadableTable from, final int fromColumn, final int fromRow,
      final AppendableTable to, final int toColumn) {
    Type t = from.getSchema().getColumnType(fromColumn);
    switch (t) {
      case BOOLEAN_TYPE:
        to.putBoolean(toColumn, from.getBoolean(fromColumn, fromRow));
        break;
      case DATETIME_TYPE:
        to.putDateTime(toColumn, from.getDateTime(fromColumn, fromRow));
        break;
      case DOUBLE_TYPE:
        to.putDouble(toColumn, from.getDouble(fromColumn, fromRow));
        break;
      case FLOAT_TYPE:
        to.putFloat(toColumn, from.getFloat(fromColumn, fromRow));
        break;
      case INT_TYPE:
        to.putInt(toColumn, from.getInt(fromColumn, fromRow));
        break;
      case LONG_TYPE:
        to.putLong(toColumn, from.getLong(fromColumn, fromRow));
        break;
      case STRING_TYPE:
        to.putString(toColumn, from.getString(fromColumn, fromRow));
        break;
    }
  }
}
