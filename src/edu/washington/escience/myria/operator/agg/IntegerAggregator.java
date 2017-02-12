package edu.washington.escience.myria.operator.agg;

import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReplaceableColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public final class IntegerAggregator extends PrimitiveAggregator {

  protected IntegerAggregator(final String inputName, final int column, final AggregationOp aggOp) {
    super(inputName, column, aggOp);
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public void addRow(
      final TupleBatch from,
      final int fromRow,
      final MutableTupleBuffer to,
      final int toRow,
      final int offset) {
    ReadableColumn fromCol = from.asColumn(column);
    ReplaceableColumn toCol = to.getColumn(offset, toRow);
    final int inColumRow = to.getInColumnIndex(toRow);
    switch (aggOp) {
      case COUNT:
        toCol.replaceLong(toCol.getLong(inColumRow) + 1, inColumRow);
        break;
      case MAX:
        toCol.replaceInt(Math.max(fromCol.getInt(fromRow), toCol.getInt(inColumRow)), inColumRow);
        break;
      case MIN:
        toCol.replaceInt(Math.min(fromCol.getInt(fromRow), toCol.getInt(inColumRow)), inColumRow);
        break;
      case SUM:
        toCol.replaceLong(
            LongMath.checkedAdd((long) fromCol.getInt(fromRow), toCol.getLong(inColumRow)),
            inColumRow);
        break;
      case SUM_SQUARED:
        toCol.replaceLong(
            LongMath.checkedAdd(
                LongMath.checkedMultiply((long) fromCol.getInt(fromRow), fromCol.getInt(fromRow)),
                toCol.getLong(inColumRow)),
            inColumRow);
        break;
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  protected boolean isSupported(final AggregationOp aggOp) {
    return ImmutableSet.of(
            AggregationOp.COUNT,
            AggregationOp.MIN,
            AggregationOp.MAX,
            AggregationOp.SUM,
            AggregationOp.AVG,
            AggregationOp.STDEV,
            AggregationOp.SUM_SQUARED)
        .contains(aggOp);
  }

  @Override
  protected Type getOutputType() {
    switch (aggOp) {
      case COUNT:
      case SUM:
        return Type.LONG_TYPE;
      case MAX:
      case MIN:
        return Type.INT_TYPE;
      case AVG:
      case STDEV:
        return Type.DOUBLE_TYPE;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  };

  @Override
  public void appendInitValue(AppendableTable data, final int column) {
    switch (aggOp) {
      case COUNT:
      case SUM:
      case SUM_SQUARED:
        data.putLong(column, 0);
        break;
      case MAX:
        data.putInt(column, Integer.MIN_VALUE);
        break;
      case MIN:
        data.putInt(column, Integer.MAX_VALUE);
        break;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  }
}
