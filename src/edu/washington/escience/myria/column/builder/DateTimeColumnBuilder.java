package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.nio.LongBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.joda.time.DateTime;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.DateTimeColumn;
import edu.washington.escience.myria.column.mutable.DateTimeMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DateTimeColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Date values.
 *
 */
public final class DateTimeColumnBuilder extends ColumnBuilder<DateTime> {

  /**
   * The internal representation of the data.
   * */
  private final DateTime[] data;

  /** Number of elements in this column. */
  private int numDates;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public DateTimeColumnBuilder() {
    numDates = 0;
    data = new DateTime[TupleBatch.BATCH_SIZE];
  }

  /**
   * copy.
   *
   * @param numDates the actual num strings in the data
   * @param data the underlying data
   * */
  private DateTimeColumnBuilder(final DateTime[] data, final int numDates) {
    this.numDates = numDates;
    this.data = data;
  }

  /**
   * Constructs a DateColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static DateTimeColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    Preconditions.checkArgument(
        message.getType().ordinal() == ColumnMessage.Type.DATETIME_VALUE,
        "Trying to construct DateColumn from non-DATE ColumnMessage %s",
        message.getType());
    Preconditions.checkArgument(message.hasDateColumn(), "ColumnMessage is missing DateColumn");
    final DateTimeColumnMessage dateColumn = message.getDateColumn();
    DateTime[] newData = new DateTime[numTuples];
    LongBuffer data = dateColumn.getData().asReadOnlyByteBuffer().asLongBuffer();
    for (int i = 0; i < numTuples; i++) {
      newData[i] = new DateTime(data.get());
    }
    return new DateTimeColumnBuilder(newData, numTuples).build();
  }

  @Override
  public DateTimeColumnBuilder appendDateTime(final DateTime value) throws BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    Objects.requireNonNull(value, "value");
    if (numDates >= TupleBatch.BATCH_SIZE) {
      throw new BufferOverflowException();
    }
    data[numDates++] = value;
    return this;
  }

  @Override
  public Type getType() {
    return Type.DATETIME_TYPE;
  }

  @Override
  public DateTimeColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendDateTime(new DateTime(resultSet.getTimestamp(jdbcIndex).getTime()));
  }

  @Override
  public DateTimeColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");

    return appendDateTime(new DateTime(statement.columnLong(index)));
  }

  @Override
  public int size() {
    return numDates;
  }

  @Override
  public DateTimeColumn build() {
    built = true;
    return new DateTimeColumn(data, numDates);
  }

  @Override
  public DateTimeMutableColumn buildMutable() {
    built = true;
    return new DateTimeMutableColumn(data, numDates);
  }

  @Override
  public void replaceDateTime(final DateTime value, final int row)
      throws IndexOutOfBoundsException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, numDates);
    Preconditions.checkNotNull(value);
    data[row] = value;
  }

  @Override
  public DateTimeColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numDates + size > data.length) {
      throw new BufferOverflowException();
    }
    numDates += size;
    return this;
  }

  @Override
  public DateTimeColumnBuilder expandAll() {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    numDates = data.length;
    return this;
  }

  @Override
  public DateTime getDateTime(final int row) {
    Preconditions.checkElementIndex(row, numDates);
    return data[row];
  }

  @Override
  public DateTime getObject(final int row) {
    return getDateTime(row);
  }

  @Deprecated
  @Override
  public DateTimeColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkState(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendDateTime((DateTime) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public DateTimeColumnBuilder forkNewBuilder() {
    DateTime[] newData = new DateTime[data.length];
    System.arraycopy(data, 0, newData, 0, numDates);
    return new DateTimeColumnBuilder(newData, numDates);
  }
}
