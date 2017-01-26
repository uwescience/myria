package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.gs.collections.api.iterator.IntIterator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * This aggregate operator computes the aggregation in streaming manner (requires input sorted on grouping column(s)).
 * Intend to substitute Aggregate when input is known to be sorted.
 *
 * @see Aggregate
 */
public class StreamingAggregate extends Aggregate {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Groups the input tuples according to the specified grouping fields, then produces the specified aggregates.
   *
   * @param child The Operator that is feeding us tuples.
   * @param gfields The columns over which we are grouping the result.
   * @param factories The factories that will produce the {@link Aggregator}s for each group.
   */
  public StreamingAggregate(
      @Nullable final Operator child,
      @Nonnull final int[] gfields,
      @Nonnull final AggregatorFactory... factories) {
    super(child, gfields, factories);
  }

  /**
   * Returns the next tuple batch containing the result of this aggregate. Grouping field(s) followed by aggregate
   * field(s).
   *
   * @throws DbException if any error occurs.
   * @return result tuple batch
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = child.nextReady();
    while (tb != null) {
      for (int row = 0; row < tb.numTuples(); ++row) {
        IntIterator iter = groupStates.getIndices(tb, gfields, row).intIterator();
        int indice;
        if (!iter.hasNext()) {
          addToResult();
          groupStates.addTuple(tb, gfields, row, true);
          for (int i = 0; i < internalAggs.size(); ++i) {
            internalAggs.get(i).initState(groupStates.getData());
          }
          indice = groupStates.getData().numTuples() - 1;
        } else {
          indice = iter.next();
        }
        for (int i = 0; i < internalAggs.size(); ++i) {
          internalAggs.get(i).addRow(tb, row, groupStates.getData(), indice);
        }
      }
      if (resultBuffer.hasFilledTB()) {
        return resultBuffer.popFilled();
      }
      tb = child.nextReady();
    }
    if (child.eos()) {
      addToResult();
      return resultBuffer.popAny();
    }
    return null;
  }

  /**
   * Add aggregate results with previous grouping key to result buffer.
   */
  private void addToResult() {
    for (TupleBatch tb : groupStates.getData().getAll()) {
      List<Column<?>> columns = new ArrayList<Column<?>>();
      for (int i = 0; i < gfields.length; ++i) {
        columns.add(tb.getDataColumns().get(i));
      }
      for (Aggregator agg : emitAggs) {
        columns.addAll(agg.emitOutput(tb));
      }
      resultBuffer.absorb(new TupleBatch(getSchema(), columns), false);
    }
    groupStates.cleanup();
  }
}
