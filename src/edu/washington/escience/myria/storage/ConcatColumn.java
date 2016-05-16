package edu.washington.escience.myria.storage;

import java.util.Map;
import java.util.TreeMap;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;

/**
 * A {@link Column} formed by the concatenation of one or more columns.
 *
 * @param <T> the type of the values in this column.
 */
public class ConcatColumn<T extends Comparable<?>> extends Column<T> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The type of the values in this column. */
  private final Type type;
  /** The number of values in this column. */
  private int numTuples;
  /** Used to figure out which of the concatenated columns is the desired one. */
  private final TreeMap<Integer, Column<?>> columnIds;
  /** Whether this concatenated column is read only and can no longer be concatenated to. */
  private boolean readOnly;

  /**
   * A wrapper for one or more columns of a specified type.
   *
   * @param type type of the columns that this object exposes.
   */
  public ConcatColumn(final Type type) {
    this.type = type;
    numTuples = 0;
    columnIds = Maps.newTreeMap();
    readOnly = false;
  }

  /**
   * Add the specified column to this column.
   *
   * @param column the column to be added.
   */
  public void addColumn(final Column<?> column) {
    Preconditions.checkState(!readOnly, "Cannot add more data to a read only concat column");
    Preconditions.checkArgument(
        column.getType() == getType(),
        "cannot append a type %s to a column of type %s",
        column.getType(),
        getType());
    if (column instanceof ConcatColumn) {
      for (Column<?> c : ((ConcatColumn<?>) column).columnIds.values()) {
        addColumn(c);
      }
    } else {
      columnIds.put(numTuples, column);
      numTuples += column.size();
    }
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public int size() {
    return numTuples;
  }

  /**
   * Validates the index of the requested row and returns the necessary information to calculate get the correct row out
   * of it.
   *
   * @param row the row of the requested value.
   * @return the necessary information to calculate get the correct row and value out of the concatenated columns.
   */
  private Map.Entry<Integer, Column<?>> getColumnEntry(final int row) {
    Preconditions.checkElementIndex(row, numTuples);
    readOnly = true;
    return columnIds.floorEntry(row);
  }

  @Override
  public boolean getBoolean(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getBoolean(row - entry.getKey());
  }

  @Override
  public DateTime getDateTime(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getDateTime(row - entry.getKey());
  }

  @Override
  public double getDouble(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getDouble(row - entry.getKey());
  }

  @Override
  public float getFloat(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getFloat(row - entry.getKey());
  }

  @Override
  public int getInt(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getInt(row - entry.getKey());
  }

  @Override
  public long getLong(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getLong(row - entry.getKey());
  }

  @SuppressWarnings("unchecked")
  @Deprecated
  @Override
  public T getObject(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return (T) entry.getValue().getObject(row - entry.getKey());
  }

  @Override
  public String getString(final int row) {
    Map.Entry<Integer, Column<?>> entry = getColumnEntry(row);
    return entry.getValue().getString(row - entry.getKey());
  }
}
