package edu.washington.escience.myria.column;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.joda.time.DateTime;

import com.google.protobuf.ByteString;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.proto.DataProto.BooleanColumnMessage;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DateTimeColumnMessage;
import edu.washington.escience.myria.proto.DataProto.DoubleColumnMessage;
import edu.washington.escience.myria.proto.DataProto.FloatColumnMessage;
import edu.washington.escience.myria.proto.DataProto.IntColumnMessage;
import edu.washington.escience.myria.proto.DataProto.LongColumnMessage;
import edu.washington.escience.myria.proto.DataProto.StringColumnMessage;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.util.ImmutableIntArray;

/**
 * A column of a batch of tuples.
 *
 * @param <T> type of the objects in this column.
 *
 *
 */
public abstract class Column<T extends Comparable<?>> implements ReadableColumn, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public boolean getBoolean(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public DateTime getDateTime(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public double getDouble(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public float getFloat(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public int getInt(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public long getLong(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public abstract T getObject(int row);

  @Override
  public String getString(final int row) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public abstract Type getType();

  /**
   * Serializes this column as a protobuf message into the specified output stream.
   *
   * @return a ColumnMessage containing a serialized copy of this column.
   */
  public ColumnMessage serializeToProto() {
    return Column.defaultProto(this);
  }

  /**
   * Serializes this column as a protobuf message into the specified output stream.
   *
   * @param validIndices the rows of the column to serialize.
   * @return a ColumnMessage containing a serialized copy of this column.
   */
  public ColumnMessage serializeToProto(final ImmutableIntArray validIndices) {
    return Column.defaultProto(this, validIndices);
  }

  @Override
  public abstract int size();

  /**
   * Creates a new Column containing the contents of this column including only the specified rows.
   *
   * @param filter a BitSet indicating which rows should be kept.
   * @return a new Column containing the contents of this column including only the specified rows.
   */
  public Column<T> filter(final BitSet filter) {
    return new FilteredColumn<T>(this, filter);
  }

  /**
   * @param type the type of the column to be returned.
   * @return a new empty column of the specified type.
   */
  public static Column<?> emptyColumn(final Type type) {
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanColumn(new BitSet(0), 0);
      case DATETIME_TYPE:
        return new DateTimeColumn(new DateTime[] {}, 0);
      case DOUBLE_TYPE:
        return new DoubleColumn(new double[] {}, 0);
      case FLOAT_TYPE:
        return new FloatColumn(new float[] {}, 0);
      case INT_TYPE:
        return new IntArrayColumn(new int[] {}, 0);
      case LONG_TYPE:
        return new LongColumn(new long[] {}, 0);
      case STRING_TYPE:
        return new StringArrayColumn(new String[] {}, 0);
    }
    throw new UnsupportedOperationException("Allocating an empty column of type " + type);
  }

  /**
   * A default implementation to serialize any Boolean column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with a BooleanColumn member.
   */
  protected static final ColumnMessage defaultBooleanProto(final Column<?> column) {
    ByteString.Output bytes = ByteString.newOutput((column.size() + 7) / 8);
    int bitCnt = 0;
    int b = 0;
    for (int i = 0; i < column.size(); ++i) {
      if (column.getBoolean(i)) {
        b |= (1 << bitCnt);
      }
      bitCnt++;
      if (bitCnt == 8) {
        bytes.write(b);
        bitCnt = 0;
        b = 0;
      }
    }
    /* Note that we do *not* build the inner class. We pass its builder instead. */
    final BooleanColumnMessage.Builder inner =
        BooleanColumnMessage.newBuilder().setData(bytes.toByteString());
    return ColumnMessage.newBuilder()
        .setType(ColumnMessage.Type.BOOLEAN)
        .setBooleanColumn(inner)
        .build();
  }

  /**
   * A default implementation to serialize any DateTime column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with a DateColumn member.
   */
  protected static ColumnMessage defaultDateTimeProto(final Column<?> column) {
    ByteBuffer dataBytes = ByteBuffer.allocate(column.size() * Long.SIZE / Byte.SIZE);
    for (int i = 0; i < column.size(); i++) {
      dataBytes.putLong(column.getDateTime(i).getMillis());
    }
    dataBytes.flip();
    final DateTimeColumnMessage.Builder inner =
        DateTimeColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder()
        .setType(ColumnMessage.Type.DATETIME)
        .setDateColumn(inner)
        .build();
  }

  /**
   * A default implementation to serialize any Double column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with a DoubleColumn member.
   */
  protected static ColumnMessage defaultDoubleProto(final Column<?> column) {
    ByteBuffer dataBytes = ByteBuffer.allocate(column.size() * Double.SIZE / Byte.SIZE);
    for (int i = 0; i < column.size(); i++) {
      dataBytes.putDouble(column.getDouble(i));
    }
    dataBytes.flip();
    final DoubleColumnMessage.Builder inner =
        DoubleColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder()
        .setType(ColumnMessage.Type.DOUBLE)
        .setDoubleColumn(inner)
        .build();
  }

  /**
   * A default implementation to serialize any Float column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with a FloatColumn member.
   */
  protected static ColumnMessage defaultFloatProto(final Column<?> column) {
    ByteBuffer dataBytes = ByteBuffer.allocate(column.size() * Float.SIZE / Byte.SIZE);
    for (int i = 0; i < column.size(); i++) {
      dataBytes.putFloat(column.getFloat(i));
    }
    dataBytes.flip();
    final FloatColumnMessage.Builder inner =
        FloatColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder()
        .setType(ColumnMessage.Type.FLOAT)
        .setFloatColumn(inner)
        .build();
  }

  /**
   * A default implementation to serialize any Integer column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with an IntColumn member.
   */
  protected static ColumnMessage defaultIntProto(final Column<?> column) {
    ByteBuffer dataBytes = ByteBuffer.allocate(column.size() * Integer.SIZE / Byte.SIZE);
    for (int i = 0; i < column.size(); i++) {
      dataBytes.putInt(column.getInt(i));
    }
    dataBytes.flip();
    final IntColumnMessage.Builder inner =
        IntColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.INT).setIntColumn(inner).build();
  }

  /**
   * A default implementation to serialize any Long column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with a LongColumn member.
   */
  protected static ColumnMessage defaultLongProto(final Column<?> column) {
    ByteBuffer dataBytes = ByteBuffer.allocate(column.size() * Long.SIZE / Byte.SIZE);
    for (int i = 0; i < column.size(); i++) {
      dataBytes.putLong(column.getLong(i));
    }
    dataBytes.flip();
    final LongColumnMessage.Builder inner =
        LongColumnMessage.newBuilder().setData(ByteString.copyFrom(dataBytes));
    return ColumnMessage.newBuilder().setType(ColumnMessage.Type.LONG).setLongColumn(inner).build();
  }

  /**
   * A default implementation to serialize any column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with an appropriate member.
   */
  protected static ColumnMessage defaultProto(final Column<?> column) {
    switch (column.getType()) {
      case BOOLEAN_TYPE:
        return defaultBooleanProto(column);
      case DATETIME_TYPE:
        return defaultDateTimeProto(column);
      case DOUBLE_TYPE:
        return defaultDoubleProto(column);
      case FLOAT_TYPE:
        return defaultFloatProto(column);
      case INT_TYPE:
        return defaultIntProto(column);
      case LONG_TYPE:
        return defaultLongProto(column);
      case STRING_TYPE:
        return defaultStringProto(column);
    }
    throw new UnsupportedOperationException("Serializing a column of type " + column.getType());
  }

  /**
   * A default implementation to serialize any filtered column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @param validIndices the valid indices in the column.
   * @return a ColumnMessage with an appropriate member.
   */
  protected static ColumnMessage defaultProto(
      final Column<?> column, final ImmutableIntArray validIndices) {
    BitSet filter = new BitSet(column.size());
    for (int i = 0; i < column.size(); ++i) {
      filter.set(validIndices.get(i));
    }
    return defaultProto(new FilteredColumn<>(column, filter));
  }

  /**
   * A default implementation to serialize any String column to a proto. Full copy.
   *
   * @param column the column to be serialized.
   * @return a ColumnMessage with a StringColumn member.
   */
  protected static ColumnMessage defaultStringProto(final Column<?> column) {
    final StringColumnMessage.Builder inner = StringColumnMessage.newBuilder();
    StringBuilder sb = new StringBuilder();
    int startP = 0, endP = 0;
    for (int i = 0; i < column.size(); i++) {
      endP = startP + column.getString(i).length();
      inner.addStartIndices(startP);
      inner.addEndIndices(endP);
      sb.append(column.getString(i));
      startP = endP;
    }
    inner.setData(ByteString.copyFromUtf8(sb.toString()));
    return ColumnMessage.newBuilder()
        .setType(ColumnMessage.Type.STRING)
        .setStringColumn(inner)
        .build();
  }
}
