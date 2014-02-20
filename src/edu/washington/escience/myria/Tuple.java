package edu.washington.escience.myria;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * A single row relation.
 */
public class Tuple implements Serializable, ReadableTable {
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
    getColumn(columnIdx).appendObject(value);
  }

  /**
   * Insert tuple into JDBC database.
   *
   * @param statement the JDBC statement
   * @throws SQLException when setting values in the statement fails
   */
  public void getIntoJdbc(final PreparedStatement statement) throws SQLException {
    for (int column = 0; column < numColumns(); column++) {
      switch (getSchema().getColumnType(column)) {
        case BOOLEAN_TYPE:
          statement.setBoolean(column + 1, getBoolean(column, 0));
          break;
        case INT_TYPE:
          statement.setInt(column + 1, getInt(column, 0));
          break;
        case LONG_TYPE:
          statement.setLong(column + 1, getLong(column, 0));
          break;
        case FLOAT_TYPE:
          statement.setFloat(column + 1, getFloat(column, 0));
          break;
        case DOUBLE_TYPE:
          statement.setDouble(column + 1, getDouble(column, 0));
          break;
        case DATETIME_TYPE:
          statement.setTimestamp(column + 1, new Timestamp(getDateTime(column, 0).getMillis()));
          break;
        case STRING_TYPE:
          statement.setString(column + 1, getString(column, 0));
          break;
      }
    }
  }
}
