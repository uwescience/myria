package edu.washington.escience.myria.column.builder;

import java.nio.BufferOverflowException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.column.mutable.BooleanMutableColumn;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal representation.
 *
 */
public final class BooleanColumnBuilder extends ColumnBuilder<Boolean> {
  /** Internal representation of the column data. */
  private final BitSet data;
  /** Number of valid elements. */
  private int numBits;
  /**
   * max possible size.
   * */
  private final int capacity;

  /**
   * If the builder has built the column.
   * */
  private boolean built = false;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public BooleanColumnBuilder() {
    data = new BitSet();
    numBits = 0;
    capacity = TupleBatch.BATCH_SIZE;
  }

  /**
   * Copy.
   *
   * @param capacity the required capacity
   * @param data the data
   * @param numBits numBits
   * */
  private BooleanColumnBuilder(final int capacity, final BitSet data, final int numBits) {
    this.data = data;
    this.numBits = numBits;
    this.capacity = capacity;
  }

  /**
   * Constructs a BooleanColumn by deserializing the given ColumnMessage.
   *
   * @param message a ColumnMessage containing the contents of this column.
   * @param numTuples num tuples in the column message
   * @return the built column
   */
  public static BooleanColumn buildFromProtobuf(final ColumnMessage message, final int numTuples) {
    if (message.getType().ordinal() != ColumnMessage.Type.BOOLEAN_VALUE) {
      throw new IllegalArgumentException(
          "Trying to construct BooleanColumn from non-BOOLEAN ColumnMessage");
    }
    if (!message.hasBooleanColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type BOOLEAN but no BooleanColumn");
    }
    BooleanColumnBuilder builder =
        new BooleanColumnBuilder(
            numTuples,
            BitSet.valueOf(message.getBooleanColumn().getData().asReadOnlyByteBuffer()),
            numTuples);
    return builder.build();
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  @Override
  public BooleanColumnBuilder appendBoolean(final boolean value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    if (numBits >= TupleBatch.BATCH_SIZE) {
      throw new BufferOverflowException();
    }
    data.set(numBits++, value);
    return this;
  }

  @Deprecated
  @Override
  public BooleanColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendBoolean((Boolean) MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public BooleanColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex)
      throws SQLException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendBoolean(resultSet.getBoolean(jdbcIndex));
  }

  @Override
  public BooleanColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    return appendBoolean(0 != statement.columnInt(index));
  }

  @Override
  public int size() {
    return numBits;
  }

  @Override
  public BooleanColumn build() {
    built = true;
    return new BooleanColumn(data, numBits);
  }

  @Override
  public BooleanMutableColumn buildMutable() {
    built = true;
    return new BooleanMutableColumn(data, numBits);
  }

  @Override
  public void replaceBoolean(final boolean value, final int row) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(row, numBits);
    data.set(row, value);
  }

  @Override
  public BooleanColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numBits + size > capacity) {
      throw new BufferOverflowException();
    }
    numBits = numBits + size;
    return this;
  }

  @Override
  public BooleanColumnBuilder expandAll() {
    Preconditions.checkArgument(
        !built, "No further changes are allowed after the builder has built the column.");
    numBits = capacity;
    return this;
  }

  @Override
  @Deprecated
  public Boolean getObject(final int row) {
    Preconditions.checkArgument(row >= 0 && row < numBits);
    return data.get(row);
  }

  @Override
  public boolean getBoolean(final int row) {
    Preconditions.checkArgument(row >= 0 && row < numBits);
    return data.get(row);
  }

  @Override
  public BooleanColumnBuilder forkNewBuilder() {
    return new BooleanColumnBuilder(capacity, BitSet.valueOf(data.toByteArray()), numBits);
  }
}
