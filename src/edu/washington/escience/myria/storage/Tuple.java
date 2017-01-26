package edu.washington.escience.myria.storage;

import java.io.Serializable;

import javax.annotation.Nonnull;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.builder.WritableColumn;
import edu.washington.escience.myria.util.MyriaArrayUtils;

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
  private final MutableTupleBuffer data;

  /**
   * @param schema the schema of the tuple
   */
  public Tuple(final Schema schema) {
    data = new MutableTupleBuffer(schema);
    this.schema = schema;
  }

  /**
   * @param data
   * @param row
   * @param cols
   */
  public Tuple(final ReadableTable data, final int row, final int[] cols) {
    this.schema = data.getSchema().getSubSchema(cols);
    this.data = new MutableTupleBuffer(schema);
    for (int i = 0; i < cols.length; ++i) {
      this.data.put(i, data.asColumn(cols[i]), row);
    }
  }

  /**
   * @return the schema
   */
  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean getBoolean(final int column, final int row) {
    return data.getBoolean(column, row);
  }

  @Override
  public double getDouble(final int column, final int row) {
    return data.getDouble(column, row);
  }

  @Override
  public float getFloat(final int column, final int row) {
    return data.getFloat(column, row);
  }

  @Override
  public int getInt(final int column, final int row) {
    return data.getInt(column, row);
  }

  @Override
  public long getLong(final int column, final int row) {
    return data.getLong(column, row);
  }

  @Override
  public String getString(final int column, final int row) {
    return data.getString(column, row);
  }

  @Override
  public DateTime getDateTime(final int column, final int row) {
    return data.getDateTime(column, row);
  }

  @Override
  public Object getObject(final int column, final int row) {
    return data.getObject(column, row);
  }

  @Override
  public int numColumns() {
    return getSchema().numColumns();
  }

  @Override
  public int numTuples() {
    return 1;
  }

  @Override
  public ReadableColumn asColumn(final int column) {
    return new ReadableSubColumn(
        this, Preconditions.checkElementIndex(column, schema.numColumns()));
  }

  @Override
  public Tuple clone() {
    return new Tuple(data.clone(), 0, MyriaArrayUtils.range(0, numColumns()));
  }

  @Override
  public void putBoolean(final int column, final boolean value) {
    data.putBoolean(column, value);
  }

  @Override
  public void putDateTime(final int column, @Nonnull final DateTime value) {
    data.putDateTime(column, value);
  }

  @Override
  public void putDouble(final int column, final double value) {
    data.putDouble(column, value);
  }

  @Override
  public void putFloat(final int column, final float value) {
    data.putFloat(column, value);
  }

  @Override
  public void putInt(final int column, final int value) {
    data.putInt(column, value);
  }

  @Override
  public void putLong(final int column, final long value) {
    data.putLong(column, value);
  }

  @Override
  public void putString(final int column, final @Nonnull String value) {
    data.putString(column, value);
  }

  @Override
  @Deprecated
  public void putObject(final int column, final @Nonnull Object value) {
    data.putObject(column, value);
  }

  @Override
  public WritableColumn asWritableColumn(final int column) {
    return data.asWritableColumn(column);
  }

  @Override
  public final String toString() {
    return data.getAll().get(0).toString();
  }
}
