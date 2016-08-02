package edu.washington.escience.myria.operator.agg;

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

  /** Use to create the aggregators. */
  private final AggregatorFactory[] factories;
  /** The actual aggregators. */
  private Aggregator[] aggregators;
  /** The state of the aggregators. */
  private Object[] aggregatorStates;
  /**
   * buffer for holding results.
   */
  private transient TupleBatchBuffer aggBuffer;

  /**
   * Computes the value of one or more aggregates over the entire input relation.
   *
   * @param child The Operator that is feeding us tuples.
   * @param aggregators The {@link AggregatorFactory}s that creators the {@link Aggregator}s.
   */
  public Aggregate(@Nullable final Operator child, final AggregatorFactory... aggregators) {
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
      for (int agg = 0; agg < aggregators.length; ++agg) {
        aggregators[agg].add(tb, aggregatorStates[agg]);
      }
    }

    if (child.eos()) {
      int fromIndex = 0;
      for (int agg = 0; agg < aggregators.length; ++agg) {
        aggregators[agg].getResult(aggBuffer, fromIndex, aggregatorStates[agg]);
        fromIndex += aggregators[agg].getResultSchema().numColumns();
      }
      return aggBuffer.popAny();
    }
    return null;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkState(getSchema() != null, "unable to determine schema in init");

    aggregators =
        AggUtils.allocateAggs(factories, getChild().getSchema(), getPythonFunctionRegistrar());
    aggregatorStates = AggUtils.allocateAggStates(aggregators);
    aggBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  protected Schema generateSchema() {
    if (getChild() == null) {
      return null;
    }
    final Schema inputSchema = getChild().getSchema();
    if (inputSchema == null) {
      return null;
    }

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    try {
      for (Aggregator agg :
          AggUtils.allocateAggs(factories, inputSchema, getPythonFunctionRegistrar())) {
        Schema s = agg.getResultSchema();
        gTypes.addAll(s.getColumnTypes());
        gNames.addAll(s.getColumnNames());
      }
    } catch (DbException e) {
      throw new RuntimeException("unable to allocate aggregators", e);
    }
    return new Schema(gTypes, gNames);
  }
}
