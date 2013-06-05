package edu.washington.escience.myriad;

import java.util.BitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.util.ImmutableBitSet;

/**
 * A predicate for filtering x != y. x and y must have the same type.
 * 
 * @author leelee
 * 
 */
public class NotEqualsPredicate implements Predicate {

  /** For serialization. */
  private static final long serialVersionUID = 1L;
  /** The index of the column. */
  private final int compareIndex;
  /** The value to be compared with. */
  private final String compareValue;

  /**
   * Construct a new EqualsPredicate that filter the value at the given compareIndex = compareValue.
   * 
   * @param compareIndex The index of the column.
   * @param compareValue The value to be compared with.
   */
  public NotEqualsPredicate(final int compareIndex, final String compareValue) {
    this.compareIndex = compareIndex;
    this.compareValue = compareValue;
  }

  @Override
  public final ImmutableBitSet filter(final TupleBatch tb) {
    Preconditions.checkNotNull(tb);
    ImmutableList<Column<?>> columns = tb.getDataColumns();
    ImmutableList<Integer> validIndices = tb.getValidIndices();
    Schema schema = tb.getSchema();
    BitSet result = new BitSet();

    Type type = schema.getColumnType(compareIndex);
    if (type == Type.INT_TYPE) {
      // the column is an int type
      IntColumn compareColumn = (IntColumn) columns.get(compareIndex);
      for (int idx : validIndices) {
        if (compareColumn.getInt(idx) != Integer.valueOf(compareValue)) {
          result.set(idx);
        }
      }
    } else if (type == Type.DOUBLE_TYPE) {
      // the column is a double type
      DoubleColumn compareColumn = (DoubleColumn) columns.get(compareIndex);
      for (int idx : validIndices) {
        if (Double.compare(Double.valueOf(compareValue), compareColumn.getDouble(idx)) != 0) {
          result.set(idx);
        }
      }
    } else if (type == Type.FLOAT_TYPE) {
      // the column is a float type
      FloatColumn compareColumn = (FloatColumn) columns.get(compareIndex);
      for (int idx : validIndices) {
        if (Float.compare(Float.valueOf(compareValue), compareColumn.getFloat(idx)) != 0) {
          result.set(idx);
        }
      }
    } else if (type == Type.LONG_TYPE) {
      // the column is a long type
      LongColumn compareColumn = (LongColumn) columns.get(compareIndex);
      for (int idx : validIndices) {
        if (compareColumn.getLong(idx) != Long.valueOf(compareValue)) {
          result.set(idx);
        }
      }
    } else if (type == Type.STRING_TYPE) {
      // the column is a string type
      StringColumn compareColumn = (StringColumn) columns.get(compareIndex);
      for (int idx : validIndices) {
        if (!compareColumn.getString(idx).equals(compareValue)) {
          result.set(idx);
        }
      }
    } else {
      throw new IllegalArgumentException("Not supported type: " + type);
    }
    return new ImmutableBitSet(result);
  }

}
