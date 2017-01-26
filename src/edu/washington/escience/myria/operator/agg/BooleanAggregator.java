package edu.washington.escience.myria.operator.agg;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReplaceableColumn;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Knows how to compute some aggregates over a BooleanColumn.
 */
public final class BooleanAggregator extends PrimitiveAggregator {

  protected BooleanAggregator(
      final String inputName, final int column, final AggregationOp aggOp, final int[] stateCols) {
    super(inputName, column, aggOp, stateCols);
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public void addRow(
      final TupleBatch from, final int fromRow, final MutableTupleBuffer to, final int toRow) {
    Objects.requireNonNull(from, "from");
    ReplaceableColumn toCol = to.getColumn(stateCols[0], toRow);
    final int inColumRow = to.getInColumnIndex(toRow);
    switch (aggOp) {
      case COUNT:
        toCol.replaceLong(toCol.getLong(inColumRow) + 1, inColumRow);
        break;
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  public List<Column<?>> emitOutput(final TupleBatch tb) {
    switch (aggOp) {
      case COUNT:
        return ImmutableList.of(tb.getDataColumns().get(stateCols[0]));
      default:
        throw new IllegalArgumentException(aggOp + " is invalid");
    }
  }

  @Override
  protected boolean isSupported(final AggregationOp aggOp) {
    return aggOp.equals(AggregationOp.COUNT);
  }

  @Override
  protected Type getOutputType() {
    switch (aggOp) {
      case COUNT:
        return Type.LONG_TYPE;
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
      default:
        throw new IllegalArgumentException("Type " + aggOp + " is invalid");
    }
  }
}
