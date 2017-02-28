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
 * Knows how to compute some aggregates over a LongColumn.
 */
public final class LongAggregator extends PrimitiveAggregator {

  protected LongAggregator(final String inputName, final int column, final AggregationOp aggOp) {
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
    final int inColumnRow = to.getInColumnIndex(toRow);
    switch (aggOp) {
      case COUNT:
        toCol.replaceLong(toCol.getLong(inColumnRow) + 1, inColumnRow);
        break;
      case MAX:
        toCol.replaceLong(
            Math.max(fromCol.getLong(fromRow), toCol.getLong(inColumnRow)), inColumnRow);
        break;
      case MIN:
        toCol.replaceLong(
            Math.min(fromCol.getLong(fromRow), toCol.getLong(inColumnRow)), inColumnRow);
        break;
      case SUM:
        toCol.replaceLong(
            LongMath.checkedAdd(fromCol.getLong(fromRow), toCol.getLong(inColumnRow)), inColumnRow);
        break;
      case SUM_SQUARED:
        toCol.replaceLong(
            LongMath.checkedAdd(
                LongMath.checkedMultiply(fromCol.getLong(fromRow), fromCol.getLong(fromRow)),
                toCol.getLong(inColumnRow)),
            inColumnRow);
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
      case MAX:
      case MIN:
        return Type.LONG_TYPE;
      case AVG:
      case STDEV:
        return Type.DOUBLE_TYPE;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  };

  public void appendInitValue(AppendableTable data, final int column) {
    switch (aggOp) {
      case COUNT:
      case SUM:
      case SUM_SQUARED:
        data.putLong(column, 0);
        break;
      case MAX:
        data.putLong(column, Long.MIN_VALUE);
        break;
      case MIN:
        data.putLong(column, Long.MAX_VALUE);
        break;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  }
}
