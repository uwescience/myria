package edu.washington.escience.myria.column;

import java.nio.BufferOverflowException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.BitSet;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.Constants;

/**
 * A column of Boolean values. To save space, this implementation uses a BitSet as the internal representation.
 * 
 */
public final class BooleanColumnBuilder implements ColumnBuilder<Boolean> {
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

  /** Constructs an empty column that can hold up to Constants.getBatchSize() elements. */
  public BooleanColumnBuilder() {
    data = new BitSet();
    numBits = 0;
    capacity = Constants.getBatchSize();
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
      throw new IllegalArgumentException("Trying to construct BooleanColumn from non-BOOLEAN ColumnMessage");
    }
    if (!message.hasBooleanColumn()) {
      throw new IllegalArgumentException("ColumnMessage has type BOOLEAN but no BooleanColumn");
    }
    BooleanColumnBuilder builder =
        new BooleanColumnBuilder(numTuples,
            BitSet.valueOf(message.getBooleanColumn().getData().asReadOnlyByteBuffer()), numTuples);
    return builder.build();
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN_TYPE;
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   * @return this column.
   * @throws BufferOverflowException if exceeds buffer up bound.
   */
  public BooleanColumnBuilder append(final boolean value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    if (numBits >= Constants.getBatchSize()) {
      throw new BufferOverflowException();
    }
    data.set(numBits++, value);
    return this;
  }

  @Override
  public BooleanColumnBuilder appendObject(final Object value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append((Boolean) value);
  }

  @Override
  public BooleanColumnBuilder appendFromJdbc(final ResultSet resultSet, final int jdbcIndex) throws SQLException,
      BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(resultSet.getBoolean(jdbcIndex));
  }

  @Override
  public BooleanColumnBuilder appendFromSQLite(final SQLiteStatement statement, final int index)
      throws SQLiteException, BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(0 != statement.columnInt(index));
  }

  @Override
  public BooleanColumnBuilder append(final Boolean value) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    return append(value.booleanValue());
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
  @Deprecated
  public BooleanColumnBuilder replace(final int idx, final Boolean value) throws IndexOutOfBoundsException {
    return replace(idx, value.booleanValue());
  }

  /**
   * Replace the specified element.
   * 
   * @param value element to be inserted.
   * @param idx where to insert the element.
   * @return this column builder.
   * @throws IndexOutOfBoundsException if the idx exceeds the currently valid indices, i.e. the currently built size.
   */
  public BooleanColumnBuilder replace(final int idx, final boolean value) throws IndexOutOfBoundsException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkElementIndex(idx, numBits);
    data.set(idx, value);
    return this;
  }

  @Override
  public BooleanColumnBuilder expand(final int size) throws BufferOverflowException {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    Preconditions.checkArgument(size >= 0);
    if (numBits + size > capacity) {
      throw new BufferOverflowException();
    }
    numBits = numBits + size;
    return this;
  }

  @Override
  public BooleanColumnBuilder expandAll() {
    Preconditions.checkArgument(!built, "No further changes are allowed after the builder has built the column.");
    numBits = capacity;
    return this;
  }

  @Override
  @Deprecated
  public Boolean get(final int row) {
    Preconditions.checkArgument(row >= 0 && row < numBits);
    return data.get(row);
  }

  /**
   * @param row the row to get
   * @return primitive value of the row
   * */
  public boolean getBoolean(final int row) {
    Preconditions.checkArgument(row >= 0 && row < numBits);
    return data.get(row);
  }

  @Override
  public BooleanColumnBuilder forkNewBuilder() {
    return new BooleanColumnBuilder(capacity, BitSet.valueOf(data.toByteArray()), numBits);
  }

}