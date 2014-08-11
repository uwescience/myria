package edu.washington.escience.myria.operator.agg;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;

/**
 * Utility functions for aggregation.
 */
public final class AggUtils {
  /** Utility classes do not have a public constructor. */
  private AggUtils() {
  }

  /** Which aggregation ops require COUNT to be computed. */
  private static final Set<AggregationOp> COUNT_OPS = ImmutableSet.of(AggregationOp.COUNT, AggregationOp.AVG,
      AggregationOp.STDEV);
  /** Which aggregation ops require SUM to be computed. */
  private static final Set<AggregationOp> SUM_OPS = ImmutableSet.of(AggregationOp.SUM, AggregationOp.AVG,
      AggregationOp.STDEV);
  /** Which aggregation ops require any tuple-level stats to be computed. */
  private static final Set<AggregationOp> STATS_OPS = ImmutableSet.of(AggregationOp.MIN, AggregationOp.MAX,
      AggregationOp.SUM, AggregationOp.AVG, AggregationOp.STDEV);

  /**
   * @param aggOps the aggregate operations
   * @return true if count must be computed.
   */
  public static boolean needsCount(final Set<AggregationOp> aggOps) {
    return !Sets.intersection(COUNT_OPS, aggOps).isEmpty();
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if sum must be computed.
   */
  public static boolean needsSum(final Set<AggregationOp> aggOps) {
    return !Sets.intersection(SUM_OPS, aggOps).isEmpty();
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if sumSq must be computed.
   */
  public static boolean needsSumSq(final Set<AggregationOp> aggOps) {
    return aggOps.contains(AggregationOp.STDEV);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if min must be computed.
   */
  public static boolean needsMin(final Set<AggregationOp> aggOps) {
    return aggOps.contains(AggregationOp.MIN);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if max must be computed.
   */
  public static boolean needsMax(final Set<AggregationOp> aggOps) {
    return aggOps.contains(AggregationOp.MAX);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if tuple-level stats must be computed.
   */
  public static boolean needsStats(final Set<AggregationOp> aggOps) {
    return !Sets.intersection(STATS_OPS, aggOps).isEmpty();
  }

  /**
   * @param type the type of the aggregator.
   * @param inputName the name of the column in the child schema
   * @param aggOps the aggregate operations
   * @return an {@link PrimitiveAggregator} for the specified type, column name, and operations.
   */
  public static PrimitiveAggregator allocate(final Type type, final String inputName, final AggregationOp[] aggOps) {
    switch (type) {
      case BOOLEAN_TYPE:
        return new BooleanAggregator(inputName, aggOps);
      case DATETIME_TYPE:
        return new DateTimeAggregator(inputName, aggOps);
      case DOUBLE_TYPE:
        return new DoubleAggregator(inputName, aggOps);
      case FLOAT_TYPE:
        return new FloatAggregator(inputName, aggOps);
      case INT_TYPE:
        return new IntegerAggregator(inputName, aggOps);
      case LONG_TYPE:
        return new LongAggregator(inputName, aggOps);
      case STRING_TYPE:
        return new StringAggregator(inputName, aggOps);
    }
    throw new IllegalArgumentException("Unknown column type: " + type);
  }

  /**
   * Utility class to allocate a set of aggregators from the factories.
   * 
   * @param factories The factories that will produce the aggregators.
   * @param inputSchema The schema of the input tuples.
   * @return the aggregators for this operator.
   */
  public static Aggregator[] allocateAggs(final AggregatorFactory[] factories, final Schema inputSchema) {
    Aggregator[] aggregators = new Aggregator[factories.length];
    for (int j = 0; j < factories.length; ++j) {
      aggregators[j] = factories[j].get(inputSchema);
    }
    return aggregators;
  }
}
