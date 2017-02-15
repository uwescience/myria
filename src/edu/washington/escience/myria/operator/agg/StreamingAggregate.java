package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.gs.collections.api.iterator.IntIterator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * This aggregate operator computes the aggregation in streaming manner (requires input sorted on grouping column(s)).
 * Intended to substitute for Aggregate when input is known to be sorted.
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
   * @throws IOException
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = child.nextReady();
    while (tb != null) {
      for (int row = 0; row < tb.numTuples(); ++row) {
        IntIterator iter = groupStates.getIndices(tb, gfields, row).intIterator();
        int index;
        if (!iter.hasNext()) {
          /* A new group is encountered. Since input tuples are sorted on the grouping key, the previous group must be
           * finished so we can add its state to the result. */
          generateResult();
          groupStates.addTuple(tb, gfields, row, true);
          int offset = gfields.length;
          for (Aggregator agg : internalAggs) {
            agg.initState(groupStates.getData(), offset);
            offset += agg.getStateSize();
          }
          index = groupStates.getData().numTuples() - 1;
        } else {
          index = iter.next();
        }
        int offset = gfields.length;
        for (Aggregator agg : internalAggs) {
          agg.addRow(tb, row, groupStates.getData(), index, offset);
          offset += agg.getStateSize();
        }
      }
      if (resultBuffer.hasFilledTB()) {
        return resultBuffer.popFilled();
      }
      tb = child.nextReady();
    }
    if (child.eos()) {
      generateResult();
      return resultBuffer.popAny();
    }
    return null;
  }
}
