package edu.washington.escience.myria.operator.agg;

import java.util.List;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.DoubleColumn;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.ReplaceableColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Knows how to compute some aggregates over a FloatColumn.
 */
public final class FloatAggregator extends PrimitiveAggregator {

  protected FloatAggregator(
      final String inputName, final int column, final AggregationOp aggOp, final int[] stateCols) {
    super(inputName, column, aggOp, stateCols);
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public void addRow(
      final ReadableTable from, final int fromRow, final MutableTupleBuffer to, final int toRow) {
    ReadableColumn fromCol = from.asColumn(column);
    ReplaceableColumn toCol = to.getColumn(stateCols[0], toRow);
    switch (aggOp) {
      case COUNT:
        toCol.replaceLong(toCol.getLong(toRow) + 1, toRow);
        break;
      case MAX:
        toCol.replaceFloat(Math.max(fromCol.getFloat(fromRow), toCol.getFloat(toRow)), toRow);
        break;
      case MIN:
        toCol.replaceFloat(Math.min(fromCol.getFloat(fromRow), toCol.getFloat(toRow)), toRow);
        break;
      case SUM:
        toCol.replaceDouble(fromCol.getFloat(fromRow) + toCol.getDouble(toRow), toRow);
        break;
      case SUM_SQUARED:
        toCol.replaceDouble(
            (double) fromCol.getFloat(fromRow) * fromCol.getFloat(fromRow) + toCol.getDouble(toRow),
            toRow);
        break;
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  public List<Column<?>> emitOutput(final TupleBatch tb) {
    switch (aggOp) {
      case COUNT:
      case SUM:
      case MAX:
      case MIN:
        return ImmutableList.of(tb.getDataColumns().get(stateCols[0]));
      case AVG:
        {
          ReadableColumn sumCol = tb.asColumn(stateCols[0]);
          ReadableColumn countCol = tb.asColumn(stateCols[1]);
          double[] ret = new double[sumCol.size()];
          for (int i = 0; i < sumCol.size(); ++i) {
            ret[i] = (sumCol.getDouble(i)) / countCol.getLong(i);
          }
          return ImmutableList.of(new DoubleColumn(ret, ret.length));
        }
      case STDEV:
        {
          ReadableColumn sumCol = tb.asColumn(stateCols[0]);
          ReadableColumn sumSquaredCol = tb.asColumn(stateCols[1]);
          ReadableColumn countCol = tb.asColumn(stateCols[2]);
          double[] ret = new double[sumCol.size()];
          for (int i = 0; i < sumCol.size(); ++i) {
            double first = (sumSquaredCol.getDouble(i)) / countCol.getLong(i);
            double second = (sumCol.getDouble(i)) / countCol.getLong(i);
            ret[i] = Math.sqrt(first - second * second);
          }
          return ImmutableList.of(new DoubleColumn(ret, ret.length));
        }
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  protected boolean isSupported(final AggregationOp aggOp) {
    return true;
  }

  @Override
  protected Type getOutputType() {
    switch (aggOp) {
      case COUNT:
        return Type.LONG_TYPE;
      case MAX:
      case MIN:
        return Type.FLOAT_TYPE;
      case SUM:
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
        data.putLong(column, 0);
        break;
      case SUM:
      case SUM_SQUARED:
        data.putDouble(column, 0);
        break;
      case MAX:
        data.putFloat(column, Float.MIN_VALUE);
        break;
      case MIN:
        data.putFloat(column, Float.MAX_VALUE);
        break;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  }
}
