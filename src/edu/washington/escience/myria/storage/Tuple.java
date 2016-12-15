package edu.washington.escience.myria.storage;

import java.io.Serializable;
import java.util.List;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;
import java.nio.ByteBuffer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * A single row relation.
 */
public class Tuple implements Cloneable, AppendableTable, ReadableTable, Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The schema.
   */
  private final Schema schema;

  /**
   * The data of the tuple.
   */
  private final List<Field<?>> data;

  /**
   * @param schema the schema of the tuple
   */
  public Tuple(final Schema schema) {
    this.schema = schema;
    data = Lists.newArrayListWithCapacity(numColumns());
    for (int i = 0; i < numColumns(); i++) {
      data.add(new Field<>());
    }
  }

  /**
   * @return the schema
   */
  @Override
  public Schema getSchema() {
    return schema;
  }

  /**
   * Returns a value and checks arguments.
   *
   * @param column the column index.
   * @param row the row index.
   * @return the value at the desired position.
   */
  private Object getValue(final int column, final int row) {
    Preconditions.checkArgument(row == 0);
    Preconditions.checkElementIndex(column, numColumns());
    return data.get(column).getObject();
  }

  @Override
  public boolean getBoolean(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.BOOLEAN_TYPE);
    return (boolean) getValue(column, row);
  }

  @Override
  public double getDouble(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.DOUBLE_TYPE);
    return (double) getValue(column, row);
  }

  @Override
  public float getFloat(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.FLOAT_TYPE);
    return (float) getValue(column, row);
  }

  @Override
  public int getInt(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.INT_TYPE);
    return (int) getValue(column, row);
  }

  @Override
  public long getLong(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.LONG_TYPE);
    return (long) getValue(column, row);
  }

  @Override
  public String getString(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.STRING_TYPE);
    return (String) getValue(column, row);
  }

  @Override
  public DateTime getDateTime(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.DATETIME_TYPE);
    return (DateTime) getValue(column, row);
  }

  @Override
  public ByteBuffer getByteBuffer(final int column, final int row) {
    Preconditions.checkArgument(getSchema().getColumnType(column) == Type.BYTES_TYPE);
    return (ByteBuffer) getValue(column, row);
  }

  @Override
  public Object getObject(final int column, final int row) {
    return getValue(column, row);
  }

  @Override
  public int numColumns() {
    return getSchema().numColumns();
  }

  @Override
  public int numTuples() {
    return 1;
  }

  /**
   * @param columnIdx the column index
   * @return the field at the index
   */
  public Field<?> getColumn(final int columnIdx) {
    return data.get(columnIdx);
  }

  /**
   * Set value.
   *
   * @param columnIdx the column index
   * @param value the value to set
   */
  public void set(final int columnIdx, final Object value) {
    getColumn(columnIdx).appendObject(MyriaUtils.ensureObjectIsValidType(value));
  }

  @Override
  public ReadableColumn asColumn(final int column) {
    return new ReadableSubColumn(
        this, Preconditions.checkElementIndex(column, schema.numColumns()));
  }

  @Override
  public Tuple clone() {
    Tuple t = new Tuple(getSchema());
    for (int i = 0; i < numColumns(); ++i) {
      t.set(i, getObject(i, 0));
    }
    return t;
  }

  @Override
  public void putBoolean(final int column, final boolean value) {
    set(column, value);
  }

  @Override
  public void putDateTime(final int column, @Nonnull final DateTime value) {
    set(column, value);
  }

  @Override
  public void putDouble(final int column, final double value) {
    set(column, value);
  }

  @Override
  public void putFloat(final int column, final float value) {
    set(column, value);
  }

  @Override
  public void putInt(final int column, final int value) {
    set(column, value);
  }

  @Override
  public void putLong(final int column, final long value) {
    set(column, value);
  }

  @Override
  public void putString(final int column, final @Nonnull String value) {
    set(column, value);
  }

  @Override
  public void putObject(final int column, final @Nonnull Object value) {
    set(column, value);
  }

  @Override
  public void putByteBuffer(final int column, @Nonnull final ByteBuffer value) {
    set(column, value);
  }

  @Override
  public WritableColumn asWritableColumn(final int column) {
    return data.get(column);
  }
}
