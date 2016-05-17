package edu.washington.escience.myria.util;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * A utility class for hashing tuples and parts of tuples.
 */
public final class HashUtils {
  /** Utility classes have no constructors. */
  private HashUtils() {}

  /** picked from http://planetmath.org/goodhashtableprimes. */
  private static final int[] SEEDS = {
    243, 402653189, 24593, 786433, 3145739, 12289, 49157, 6151, 98317, 1572869,
  };

  /** The hash functions. */
  private static final HashFunction[] HASH_FUNCTIONS = {
    Hashing.murmur3_128(SEEDS[0]),
    Hashing.murmur3_128(SEEDS[1]),
    Hashing.murmur3_128(SEEDS[2]),
    Hashing.murmur3_128(SEEDS[3]),
    Hashing.murmur3_128(SEEDS[4]),
    Hashing.murmur3_128(SEEDS[5]),
    Hashing.murmur3_128(SEEDS[6]),
    Hashing.murmur3_128(SEEDS[7]),
    Hashing.murmur3_128(SEEDS[8]),
    Hashing.murmur3_128(SEEDS[9])
  };

  /**
   * Size of the hash function pool.
   */
  public static final int NUM_OF_HASHFUNCTIONS = 10;

  /**
   * Compute the hash code of all the values in the specified row, in column order.
   *
   * @param table the table containing the values
   * @param row the row to be hashed
   * @return the hash code of all the values in the specified row, in column order
   */
  public static int hashRow(final ReadableTable table, final int row) {
    Hasher hasher = HASH_FUNCTIONS[0].newHasher();
    for (int i = 0; i < table.numColumns(); ++i) {
      addValue(hasher, table, i, row);
    }
    return hasher.hash().asInt();
  }

  /**
   * Compute the hash code of the value in the specified column and row of the given table.
   *
   * @param table the table containing the values to be hashed
   * @param column the column containing the value to be hashed
   * @param row the row containing the value to be hashed
   * @return the hash code of the specified value
   */
  public static int hashValue(final ReadableTable table, final int column, final int row) {
    Hasher hasher = HASH_FUNCTIONS[0].newHasher();
    addValue(hasher, table, column, row);
    return hasher.hash().asInt();
  }

  /**
   * Compute the hash code of the value in the specified column and row of the given table with specific hashcode.
   *
   * @param table the table containing the values to be hashed
   * @param column the column containing the value to be hashed
   * @param row the row containing the value to be hashed
   * @param seedIndex the index of the chosen hashcode
   * @return hash code of the specified seed
   */
  public static int hashValue(
      final ReadableTable table, final int column, final int row, final int seedIndex) {
    Preconditions.checkPositionIndex(seedIndex, NUM_OF_HASHFUNCTIONS);
    Hasher hasher = HASH_FUNCTIONS[seedIndex].newHasher();
    addValue(hasher, table, column, row);
    return hasher.hash().asInt();
  }

  /**
   * Compute the hash code of the specified columns in the specified row of the given table.
   *
   * @param table the table containing the values to be hashed
   * @param hashColumns the columns to be hashed. Order matters
   * @param row the row containing the values to be hashed
   * @return the hash code of the specified columns in the specified row of the given table
   */
  public static int hashSubRow(final ReadableTable table, final int[] hashColumns, final int row) {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(hashColumns, "hashColumns");
    Hasher hasher = HASH_FUNCTIONS[0].newHasher();
    for (int column : hashColumns) {
      addValue(hasher, table, column, row);
    }
    return hasher.hash().asInt();
  }

  /**
   * Add the value at the specified row and column to the specified hasher.
   *
   * @param hasher the hasher
   * @param table the table containing the value
   * @param column the column containing the value
   * @param row the row containing the value
   * @return the hasher
   */
  private static Hasher addValue(
      final Hasher hasher, final ReadableTable table, final int column, final int row) {
    return addValue(hasher, table.asColumn(column), row);
  }

  /**
   * Add the value at the specified row and column to the specified hasher.
   *
   * @param hasher the hasher
   * @param column the column containing the value
   * @param row the row containing the value
   * @return the hasher
   */
  private static Hasher addValue(final Hasher hasher, final ReadableColumn column, final int row) {
    switch (column.getType()) {
      case BOOLEAN_TYPE:
        return hasher.putBoolean(column.getBoolean(row));
      case DATETIME_TYPE:
        return hasher.putObject(column.getDateTime(row), TypeFunnel.INSTANCE);
      case DOUBLE_TYPE:
        return hasher.putDouble(column.getDouble(row));
      case FLOAT_TYPE:
        return hasher.putFloat(column.getFloat(row));
      case INT_TYPE:
        return hasher.putInt(column.getInt(row));
      case LONG_TYPE:
        return hasher.putLong(column.getLong(row));
      case STRING_TYPE:
        return hasher.putObject(column.getString(row), TypeFunnel.INSTANCE);
    }
    throw new UnsupportedOperationException("Hashing a column of type " + column.getType());
  }
}
