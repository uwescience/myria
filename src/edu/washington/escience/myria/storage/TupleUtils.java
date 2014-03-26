package edu.washington.escience.myria.storage;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
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

  /**
   * @param left the left table
   * @param leftRow the row in the left table
   * @param right the right table
   * @param rightRow the row in the right table
   * @return <code>true</code> if the specified rows of the two {@link ReadableTable}s are equal.
   */
  public static boolean equalRows(final ReadableTable left, final int leftRow, final ReadableTable right,
      final int rightRow) {
    Preconditions.checkArgument(left.numColumns() == right.numColumns(), "left %s columns != right %s columns", left
        .numColumns(), right.numColumns());
    Schema schema = left.getSchema();
    for (int column = 0; column < left.numColumns(); ++column) {
      Type t = schema.getColumnType(column);
      switch (t) {
        case BOOLEAN_TYPE:
          if (left.getBoolean(column, leftRow) != right.getBoolean(column, rightRow)) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!left.getDateTime(column, leftRow).equals(right.getDateTime(column, rightRow))) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (left.getDouble(column, leftRow) != right.getDouble(column, rightRow)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (left.getFloat(column, leftRow) != right.getFloat(column, rightRow)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (left.getInt(column, leftRow) != right.getInt(column, rightRow)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (left.getLong(column, leftRow) != right.getLong(column, rightRow)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!left.getString(column, leftRow).equals(right.getString(column, rightRow))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * @param left the left table
   * @param leftRow the row in the left table
   * @param leftColumns which columns to compare in the left table
   * @param right the right table
   * @param rightRow the row in the right table
   * @param rightColumns which columns to compare in the right table
   * @return <code>true</code> if the specified subrows of the two {@link ReadableTable}s are equal.
   */
  public static boolean equalSubRows(final ReadableTable left, final int leftRow, final int[] leftColumns,
      final ReadableTable right, final int rightRow, final int[] rightColumns) {
    Preconditions.checkArgument(leftColumns.length == rightColumns.length, "left %s columns != right %s columns",
        leftColumns.length, rightColumns.length);
    Schema leftSchema = left.getSchema();
    for (int column = 0; column < leftColumns.length; ++column) {
      int leftColumn = leftColumns[column];
      int rightColumn = rightColumns[column];
      Type leftType = leftSchema.getColumnType(leftColumn);
      switch (leftType) {
        case BOOLEAN_TYPE:
          if (left.getBoolean(leftColumn, leftRow) != right.getBoolean(rightColumn, rightRow)) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!left.getDateTime(leftColumn, leftRow).equals(right.getDateTime(rightColumn, rightRow))) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (left.getDouble(leftColumn, leftRow) != right.getDouble(rightColumn, rightRow)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (left.getFloat(leftColumn, leftRow) != right.getFloat(rightColumn, rightRow)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (left.getInt(leftColumn, leftRow) != right.getInt(rightColumn, rightRow)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (left.getLong(leftColumn, leftRow) != right.getLong(rightColumn, rightRow)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!left.getString(leftColumn, leftRow).equals(right.getString(rightColumn, rightRow))) {
            return false;
          }
          break;
      }
    }
    return true;
  }
}
