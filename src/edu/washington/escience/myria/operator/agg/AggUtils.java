package edu.washington.escience.myria.operator.agg;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.functions.PythonFunctionRegistrar;
import edu.washington.escience.myria.operator.agg.PrimitiveAggregator.AggregationOp;

/**
 * Utility functions for aggregation.
 */
public final class AggUtils {
  /** Utility classes do not have a public constructor. */
  private AggUtils() {}

  /** logger for this class. */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(AggUtils.class);

  /** Which aggregation ops require COUNT to be computed. */
  private static final Set<AggregationOp> COUNT_OPS =
      ImmutableSet.of(AggregationOp.COUNT, AggregationOp.AVG, AggregationOp.STDEV);
  /** Which aggregation ops require SUM to be computed. */
  private static final Set<AggregationOp> SUM_OPS =
      ImmutableSet.of(AggregationOp.SUM, AggregationOp.AVG, AggregationOp.STDEV);
  /** Which aggregation ops require any tuple-level stats to be computed. */
  private static final Set<AggregationOp> STATS_OPS =
      ImmutableSet.of(
          AggregationOp.MIN,
          AggregationOp.MAX,
          AggregationOp.SUM,
          AggregationOp.AVG,
          AggregationOp.STDEV);

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
   * Utility class to allocate a set of aggregators from the factories.
   *
   * @param factories The factories that will produce the aggregators.
   * @param inputSchema The schema of the input tuples.
   * @return the aggregators for this operator.
   * @throws DbException if there is an error.
   */
  public static Aggregator[] allocateAggs(
      final AggregatorFactory[] factories,
      final Schema inputSchema,
      final PythonFunctionRegistrar pyFuncReg)
      throws DbException {
    Aggregator[] aggregators = new Aggregator[factories.length];
    for (int j = 0; j < factories.length; ++j) {

      aggregators[j] = factories[j].get(inputSchema, pyFuncReg);
    }
    return aggregators;
  }

  /**
   * Utility class to allocate the initial aggregation states from a set of {@link Aggregator}s.
   *
   * @param aggregators the {@link Aggregator}s that will update the states.
   * @return the initial aggregation states for the specified {@link Aggregator}s.
   */
  public static Object[] allocateAggStates(final Aggregator[] aggregators) {
    Object[] states = new Object[aggregators.length];
    for (int j = 0; j < aggregators.length; ++j) {
      states[j] = aggregators[j].getInitialState();
    }
    return states;
  }
}
