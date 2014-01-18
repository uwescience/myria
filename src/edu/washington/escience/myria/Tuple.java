package edu.washington.escience.myria;

import java.io.Serializable;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A single row relation. Behaves like a tuple batch.
 */
public class Tuple implements Serializable, Relation {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The schema.
   */
  private final Schema schema;

  /**
   * The data of the tuple.
   */
  private final List<Object> data;

  /**
   * @param schema the schema of the tuple
   */
  public Tuple(final Schema schema) {
    this.schema = schema;
    data = Lists.newArrayListWithCapacity(numColumns());
    for (int i = 0; i < numColumns(); i++) {
      data.add(null);
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
    return data.get(column);
  }

  /**
   * Set a value in the tuple.
   * 
   * @param column the column index.
   * @param value the value to set.
   * @return
   */
  public void set(final int column, final Object value) {
    data.set(column, value);
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
  public Object getObject(final int column, final int row) {
    return getValue(column, row);
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
  public int numColumns() {
    return getSchema().numColumns();
  }

  @Override
  public int numTuples() {
    return 1;
  }
}
