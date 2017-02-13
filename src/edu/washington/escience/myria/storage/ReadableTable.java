package edu.washington.escience.myria.storage;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import org.joda.time.DateTime;

/**
 * An interface for objects that contain a table (2-D) of tuples that is readable.
 */
public interface ReadableTable extends TupleTable {
  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  boolean getBoolean(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  @Nonnull
  DateTime getDateTime(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  double getDouble(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  float getFloat(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  int getInt(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  long getLong(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  @Nonnull
  Object getObject(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  @Nonnull
  String getString(final int column, final int row);

  /**
   * @param column the column of the desired value.
   * @param row the row of the desired value.
   * @return the value in the specified column and row.
   */
  ByteBuffer getBlob(final int column, final int row);

  /**
   * @param column the index of the column to be returned.
   * @return a {@link ReadableColumn} representation of the specified column of this table.
   */
  @Nonnull
  ReadableColumn asColumn(final int column);
}
