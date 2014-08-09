package edu.washington.escience.myria.operator.agg;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * The Aggregation operator that computes an aggregate.
 * 
 * This class does not do group by.
 */
public final class Aggregate extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The actual aggregators. */
  private final Aggregator[] aggregators;
  /**
   * buffer for holding results.
   * */
  private transient TupleBatchBuffer aggBuffer;

  /**
   * Computes the value of one or more aggregates over the entire input relation.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param aggregators The {@link Aggregator}s that actually compute the aggregates.
   */
  public Aggregate(@Nullable final Operator child, @Nonnull final Aggregator[] aggregators) {
    super(child);
    Preconditions.checkNotNull(aggregators, "aggregators");
    int i = 0;
    for (Aggregator agg : aggregators) {
      Preconditions.checkNotNull(agg, "aggregators[%s]", i);
      ++i;
    }
    this.aggregators = aggregators;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;
    final Operator child = getChild();

    if (child.eos()) {
      return aggBuffer.popAny();
    }

    while ((tb = child.nextReady()) != null) {
      for (Aggregator agg : aggregators) {
        agg.add(tb);
      }
    }

    if (child.eos()) {
      int fromIndex = 0;
      for (Aggregator agg : aggregators) {
        agg.getResult(aggBuffer, fromIndex);
        fromIndex += agg.getResultSchema(child.getSchema()).numColumns();
      }
      return aggBuffer.popAny();
    }
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    aggBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  protected Schema generateSchema() {
    if (getChild() == null) {
      return null;
    }
    final Schema childSchema = getChild().getSchema();
    if (childSchema == null) {
      return null;
    }

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    for (Aggregator agg : aggregators) {
      gTypes.addAll(agg.getResultSchema(childSchema).getColumnTypes());
      gNames.addAll(agg.getResultSchema(childSchema).getColumnNames());
    }
    return new Schema(gTypes, gNames);
  }
}
