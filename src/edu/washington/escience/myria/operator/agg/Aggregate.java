package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

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
  private Aggregator[] aggregators;
  /** Use to create the aggregators. */
  private final AggregatorFactory[] factories;
  /**
   * buffer for holding results.
   * */
  private transient TupleBatchBuffer aggBuffer;

  /**
   * Computes the value of one or more aggregates over the entire input relation.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param aggregators The {@link AggregatorFactory}s that creators the {@link Aggregator}s.
   */
  public Aggregate(@Nullable final Operator child, @Nonnull final AggregatorFactory... aggregators) {
    super(child);
    Preconditions.checkNotNull(aggregators, "aggregators");
    int i = 0;
    for (AggregatorFactory agg : aggregators) {
      Preconditions.checkNotNull(agg, "aggregators[%s]", i);
      ++i;
    }
    factories = aggregators;
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
        fromIndex += agg.getResultSchema().numColumns();
      }
      return aggBuffer.popAny();
    }
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    aggBuffer = new TupleBatchBuffer(getSchema());
    aggregators = getOrInitAggregators(getChild().getSchema());
  }

  /**
   * Utility class to initialize the aggregators from the given input schema.
   * 
   * @param childSchema the schema of the input tuples to this operator.
   * @return the aggregators.
   */
  private Aggregator[] getOrInitAggregators(final Schema childSchema) {
    if (aggregators != null) {
      return aggregators;
    }

    Objects.requireNonNull(childSchema, "childSchema");
    aggregators = new Aggregator[factories.length];
    for (int i = 0; i < factories.length; ++i) {
      aggregators[i] = factories[i].get(childSchema);
    }
    return aggregators;
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

    for (Aggregator agg : getOrInitAggregators(childSchema)) {
      gTypes.addAll(agg.getResultSchema().getColumnTypes());
      gNames.addAll(agg.getResultSchema().getColumnNames());
    }
    return new Schema(gTypes, gNames);
  }
}
