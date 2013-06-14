package edu.washington.escience.myriad;

import java.util.BitSet;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.column.Column;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.util.ImmutableBitSet;

/**
 * A predicate for filtering x - y < target < x + y. Assuming both the columns have the same type
 */
public class WithinSumRangePredicate implements Predicate {

  /** required by java. */
  private static final long serialVersionUID = 1L;

  /** stores the indices for the operand. */
  private final List<Integer> indices;

  /** The target column index. */
  private final int compareIndex;

  /**
   * Constructs the predicate. Uses only 2 operand indices for x and y.
   * 
   * @param compareIndex the target index
   * @param operandIndices the operand indices for x, y, where x is at index 0, and y is at index 1
   */
  public WithinSumRangePredicate(final int compareIndex, final List<Integer> operandIndices) {
    indices = operandIndices;
    this.compareIndex = compareIndex;
  }

  @Override
  public final ImmutableBitSet filter(final TupleBatch tb) {
    Preconditions.checkNotNull(tb);
    ImmutableList<Column<?>> columns = tb.getDataColumns();
    final int[] validIndices = tb.getValidIndices();
    Schema schema = tb.getSchema();
    BitSet result = new BitSet();
    if (schema.getColumnType(compareIndex) == Type.INT_TYPE) {
      // the column is an int type
      IntColumn c1 = (IntColumn) columns.get(indices.get(0));
      IntColumn c2 = (IntColumn) columns.get(indices.get(1));
      IntColumn compareColumn = (IntColumn) columns.get(compareIndex);
      for (Integer idx : validIndices) {
        if (c1.getInt(idx) + c2.getInt(idx) > compareColumn.getInt(idx)
            && c1.getInt(idx) - c2.getInt(idx) < compareColumn.getInt(idx)) {
          result.set(idx);
        }
      }
    } else if (schema.getColumnType(compareIndex) == Type.DOUBLE_TYPE) {
      // the column is a double type
      DoubleColumn c1 = (DoubleColumn) columns.get(indices.get(0));
      DoubleColumn c2 = (DoubleColumn) columns.get(indices.get(1));
      DoubleColumn compareColumn = (DoubleColumn) columns.get(compareIndex);
      for (Integer idx : validIndices) {
        if (Double.compare(c1.getDouble(idx) + c2.getDouble(idx), compareColumn.getDouble(idx)) > 0
            && Double.compare(c1.getDouble(idx) - c2.getDouble(idx), compareColumn.getDouble(idx)) < 0) {
          result.set(idx);
        }
      }
    } else if (schema.getColumnType(compareIndex) == Type.FLOAT_TYPE) {
      // the column is a float type
      FloatColumn c1 = (FloatColumn) columns.get(indices.get(0));
      FloatColumn c2 = (FloatColumn) columns.get(indices.get(1));
      FloatColumn compareColumn = (FloatColumn) columns.get(compareIndex);
      for (Integer idx : validIndices) {
        if (Float.compare(c1.getFloat(idx) + c2.getFloat(idx), compareColumn.getFloat(idx)) > 0
            && Float.compare(c1.getFloat(idx) - c2.getFloat(idx), compareColumn.getFloat(idx)) < 0) {
          result.set(idx);
        }
      }
    } else if (schema.getColumnType(compareIndex) == Type.LONG_TYPE) {
      // the column is a long type
      LongColumn c1 = (LongColumn) columns.get(indices.get(0));
      LongColumn c2 = (LongColumn) columns.get(indices.get(1));
      LongColumn compareColumn = (LongColumn) columns.get(compareIndex);
      for (Integer idx : validIndices) {
        if (c1.getLong(idx) + c2.getLong(idx) > compareColumn.getLong(idx)
            && c1.getLong(idx) - c2.getLong(idx) < compareColumn.getLong(idx)) {
          result.set(idx);
        }
      }
    }
    return new ImmutableBitSet(result);
  }
}
