package edu.washington.escience.myria.operator.agg;

import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReplaceableColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Knows how to compute some aggregate over a StringColumn.
 */
public final class StringAggregator extends PrimitiveAggregator {

  protected StringAggregator(final String inputName, final int column, final AggregationOp aggOp) {
    super(inputName, column, aggOp);
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Placeholder as MIN/MAX value of String. */
  private static final String STRING_INIT_VALUE = "NULL";

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
        {
          String value = toCol.getString(inColumRow);
          if (value.equals(STRING_INIT_VALUE) || value.compareTo(fromCol.getString(fromRow)) < 0) {
            toCol.replaceString(fromCol.getString(fromRow), inColumRow);
          }
          break;
        }
      case MIN:
        {
          String value = toCol.getString(inColumRow);
          if (value.equals(STRING_INIT_VALUE) || value.compareTo(fromCol.getString(fromRow)) > 0) {
            toCol.replaceString(fromCol.getString(fromRow), inColumRow);
          }
          break;
        }
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  protected boolean isSupported(final AggregationOp aggOp) {
    return ImmutableSet.of(AggregationOp.COUNT, AggregationOp.MIN, AggregationOp.MAX)
        .contains(aggOp);
  }

  @Override
  protected Type getOutputType() {
    switch (aggOp) {
      case COUNT:
        return Type.LONG_TYPE;
      case MAX:
      case MIN:
        return Type.STRING_TYPE;
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
      case MIN:
      case MAX:
        data.putString(column, STRING_INIT_VALUE);
        break;
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  }
}
