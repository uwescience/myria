package edu.washington.escience.myria.util;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.ReadableTable;
import edu.washington.escience.myria.Type;

/**
 * helper functions for {@value ReadableTable}.
 */
public final class ReadableTableUtil {
  /**
   * @param table1 the table that cell 1 is in
   * @param column1 column number of cell 1
   * @param row1 row number of cell 1
   * @param table2 the table that cell 2 is in
   * @param column2 column number of cell 2
   * @param row2 row number of cell 2
   * @return comparison result
   */
  public static int cellCompare(final ReadableTable table1, final int column1, final int row1,
      final ReadableTable table2, final int column2, final int row2) {
    Preconditions.checkArgument(table1.getSchema().getColumnType(column1).equals(
        table2.getSchema().getColumnType(column2)), "The types of comparing cells are not matched.");
    switch (table1.getSchema().getColumnType(column1)) {
      case BOOLEAN_TYPE:
        return Type.compareRaw(table1.getBoolean(column1, row1), table2.getBoolean(column2, row2));
      case DOUBLE_TYPE:
        return Type.compareRaw(table1.getDouble(column1, row1), table2.getDouble(column2, row2));
      case FLOAT_TYPE:
        return Type.compareRaw(table1.getFloat(column1, row1), table2.getFloat(column2, row2));
      case INT_TYPE:
        return Type.compareRaw(table1.getInt(column1, row1), table2.getInt(column2, row2));
      case LONG_TYPE:
        return Type.compareRaw(table1.getLong(column1, row1), table2.getLong(column2, row2));
      case STRING_TYPE:
        return Type.compareRaw(table1.getString(column1, row1), table2.getString(column2, row2));
      case DATETIME_TYPE:
        return Type.compareRaw(table1.getDateTime(column1, row1), table2.getDateTime(column2, row2));
    }

    throw new IllegalStateException("Invalid type.");
  }

  /**
   * Compares a whole tuple with a tuple from another batch. The columns from the two compare indexes are compared in
   * order.
   * 
   * @param table1 the first table
   * @param compareIndexes1 the columns from this table that should be compared with the column of the other table
   * @param rowIdx1 row in this table
   * @param table2 other table
   * @param compareIndexes2 the columns from the other TB that should be compared with the column of this table
   * @param rowIdx2 row in other table
   * @param ascending true if the column is ordered ascending
   * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
   *         than the second
   */
  public static int tupleCompare(final ReadableTable table1, final int[] compareIndexes1, final int rowIdx1,
      final ReadableTable table2, final int[] compareIndexes2, final int rowIdx2, final boolean[] ascending) {
    for (int i = 0; i < compareIndexes1.length; i++) {
      int compared =
          ReadableTableUtil.cellCompare(table1, compareIndexes1[i], rowIdx1, table2, compareIndexes2[i], rowIdx2);
      if (compared != 0) {
        if (!ascending[i]) {
          return -compared;
        } else {
          return compared;
        }
      }
    }
    return 0;
  }

  /**
   * Same as {@link #tupleCompare(ReadableTable, int[], int, ReadableTable, int[], int, boolean[])} but comparison
   * within the same table.
   * 
   * @param table the table that compared cells is in
   * @param columnCompareIndexes the columns from this TB that should be compared with the column of the other table
   * @param rowIdx row in this table
   * @param rowIdx2 row in this table that should be compared to the first one
   * @param ascending true if the column is ordered ascending
   * @return a negative integer, zero, or a positive integer as the first argument is less than, equal to, or greater
   *         than the second
   */
  public static int tupleCompare(final ReadableTable table, final int[] columnCompareIndexes, final int rowIdx,
      final int rowIdx2, final boolean[] ascending) {
    return tupleCompare(table, columnCompareIndexes, rowIdx, table, columnCompareIndexes, rowIdx2, ascending);
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param table1 the table holding the comparing tuple
   * @param compareColumns1 the comparing list of columns of cntTuple
   * @param row1 row number of the tuple to compare
   * @param table2 the table holding the tuple to compare against
   * @param compareColumns2 the comparing list of columns of hashTable
   * @param row2 the index in the hashTable
   * 
   * @return true if equals.
   */
  public static boolean tupleEquals(final ReadableTable table1, final int[] compareColumns1, final int row1,
      final ReadableTable table2, final int[] compareColumns2, final int row2) {
    if (compareColumns1.length != compareColumns2.length) {
      return false;
    }
    for (int i = 0; i < compareColumns1.length; ++i) {
      switch (table1.getSchema().getColumnType(compareColumns1[i])) {
        case BOOLEAN_TYPE:
          if (table1.getBoolean(compareColumns1[i], row1) != table2.getBoolean(compareColumns2[i], row2)) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (table1.getDouble(compareColumns1[i], row1) != table2.getDouble(compareColumns2[i], row2)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (table1.getFloat(compareColumns1[i], row1) != table2.getFloat(compareColumns2[i], row2)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (table1.getInt(compareColumns1[i], row1) != table2.getInt(compareColumns2[i], row2)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (table1.getLong(compareColumns1[i], row1) != table2.getLong(compareColumns2[i], row2)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!table1.getString(compareColumns1[i], row1).equals(table2.getString(compareColumns2[i], row2))) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!table1.getDateTime(compareColumns1[i], row1).equals(table2.getDateTime(compareColumns2[i], row2))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * Compare tb against a hash table on all columns.
   * 
   * @param table1 the table holding the comparing tuple
   * @param row1 number of the tuple to compare
   * @param table2 the table holding the tuple to compare against
   * @param row2 the index in the hashTable
   * @return true if equals
   */
  public static boolean tupleEquals(final ReadableTable table1, final int row1, final ReadableTable table2,
      final int row2) {
    if (table1.numColumns() != table2.numColumns()) {
      return false;
    }
    for (int i = 0; i < table1.numColumns(); ++i) {
      switch (table1.getSchema().getColumnType(i)) {
        case BOOLEAN_TYPE:
          if (table1.getBoolean(i, row1) != table2.getBoolean(i, row2)) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (table1.getDouble(i, row1) != table2.getDouble(i, row2)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (table1.getFloat(i, row1) != table2.getFloat(i, row2)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (table1.getInt(i, row1) != table2.getInt(i, row2)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (table1.getLong(i, row1) != table2.getLong(i, row2)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!table1.getString(i, row1).equals(table2.getString(i, row2))) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!table1.getDateTime(i, row1).equals(table2.getDateTime(i, row2))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * Compare {@value table1} use {@value compareColumns} against {@value table2} only containing columns which need to
   * be compared.
   * 
   * @param table1 the table holding comparing tuple
   * @param compareColumns the columns of the tuple which will compare
   * @param row1 number of the tuple to compare
   * @param table2 the table holding the tuple to compare against
   * @param index the index in the hashTable
   * 
   * @return true if equals
   */
  public static boolean tupleEquals(final ReadableTable table1, final int[] compareColumns, final int row1,
      final ReadableTable table2, final int index) {
    if (compareColumns.length != table2.numColumns()) {
      return false;
    }
    for (int i = 0; i < compareColumns.length; ++i) {
      switch (table1.getSchema().getColumnType(i)) {
        case BOOLEAN_TYPE:
          if (table1.getBoolean(compareColumns[i], row1) != table2.getBoolean(i, index)) {
            return false;
          }
          break;
        case DOUBLE_TYPE:
          if (table1.getDouble(compareColumns[i], row1) != table2.getDouble(i, index)) {
            return false;
          }
          break;
        case FLOAT_TYPE:
          if (table1.getFloat(compareColumns[i], row1) != table2.getFloat(i, index)) {
            return false;
          }
          break;
        case INT_TYPE:
          if (table1.getInt(compareColumns[i], row1) != table2.getInt(i, index)) {
            return false;
          }
          break;
        case LONG_TYPE:
          if (table1.getLong(compareColumns[i], row1) != table2.getLong(i, index)) {
            return false;
          }
          break;
        case STRING_TYPE:
          if (!table1.getString(compareColumns[i], row1).equals(table2.getString(i, index))) {
            return false;
          }
          break;
        case DATETIME_TYPE:
          if (!table1.getDateTime(compareColumns[i], row1).equals(table2.getDateTime(i, index))) {
            return false;
          }
          break;
      }
    }
    return true;
  }

  /**
   * util classes are not instantiable.
   * */
  private ReadableTableUtil() {

  }
}
