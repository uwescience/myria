package edu.washington.escience.myria;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

/**
 * An interface for objects that contain a table (2-D) of tuples that is readable.
 */
public abstract class ReadableTable implements TupleTable {
  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract boolean getBoolean(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract DateTime getDateTime(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract double getDouble(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract float getFloat(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract int getInt(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract long getLong(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract Object getObject(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  abstract String getString(final int column, final int row);

  /**
   * Compare two cells in two ReadableTable.
   * 
   * @param columnInThisTB column index of the cell in this Table.
   * @param rowInThisTB row index of the cell in this Table.
   * @param columnInComparedTB column index of the compared cell in compared Table.
   * @param rowInComparedTB row index of the compared cell in compared Table.
   * @param comparedTB the compared Table.
   * 
   * @return compared result.
   */
  public final int cellCompare(final int columnInThisTB, final int rowInThisTB, final ReadableTable comparedTB,
      final int columnInComparedTB, final int rowInComparedTB) {
    Preconditions.checkArgument(getSchema().getColumnType(columnInThisTB).equals(
        comparedTB.getSchema().getColumnType(columnInComparedTB)), "The types of comparing cells are not matched.");
    switch (getSchema().getColumnType(columnInThisTB)) {
      case BOOLEAN_TYPE:
        return Type.compareRaw(getBoolean(columnInThisTB, rowInThisTB), comparedTB.getBoolean(columnInComparedTB,
            rowInComparedTB));
      case DOUBLE_TYPE:
        return Type.compareRaw(getDouble(columnInThisTB, rowInThisTB), comparedTB.getDouble(columnInComparedTB,
            rowInComparedTB));
      case FLOAT_TYPE:
        return Type.compareRaw(getFloat(columnInThisTB, rowInThisTB), comparedTB.getFloat(columnInComparedTB,
            rowInComparedTB));
      case INT_TYPE:
        return Type.compareRaw(getInt(columnInThisTB, rowInThisTB), comparedTB.getInt(columnInComparedTB,
            rowInComparedTB));
      case LONG_TYPE:
        return Type.compareRaw(getLong(columnInThisTB, rowInThisTB), comparedTB.getLong(columnInComparedTB,
            rowInComparedTB));
      case STRING_TYPE:
        return Type.compareRaw(getString(columnInThisTB, rowInThisTB), comparedTB.getString(columnInComparedTB,
            rowInComparedTB));
      case DATETIME_TYPE:
        return Type.compareRaw(getDateTime(columnInThisTB, rowInThisTB), comparedTB.getDateTime(columnInComparedTB,
            rowInComparedTB));
    }

    throw new IllegalStateException("Invalid type in TupleBuffer.");
  }

}
