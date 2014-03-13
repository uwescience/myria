package edu.washington.escience.myria.operator.agg;

/**
 * Utility functions for aggregation.
 */
public final class AggUtils {
  /** Utility classes do not have a public constructor. */
  private AggUtils() {
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if count must be computed.
   */
  public static boolean needsCount(final int aggOps) {
    int countMask = Aggregator.AGG_OP_COUNT | Aggregator.AGG_OP_AVG | Aggregator.AGG_OP_STDEV;
    return 0 != (aggOps & countMask);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if sum must be computed.
   */
  public static boolean needsSum(final int aggOps) {
    int sumMask = Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_AVG | Aggregator.AGG_OP_STDEV;
    return 0 != (aggOps & sumMask);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if sumSq must be computed.
   */
  public static boolean needsSumSq(final int aggOps) {
    int sumSqMask = Aggregator.AGG_OP_STDEV;
    return 0 != (aggOps & sumSqMask);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if min must be computed.
   */
  public static boolean needsMin(final int aggOps) {
    return 0 != (aggOps & Aggregator.AGG_OP_MIN);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if max must be computed.
   */
  public static boolean needsMax(final int aggOps) {
    return 0 != (aggOps & Aggregator.AGG_OP_MAX);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if tuple-level stats must be computed.
   */
  public static boolean needsStats(final int aggOps) {
    int statsMask =
        Aggregator.AGG_OP_MIN | Aggregator.AGG_OP_MAX | Aggregator.AGG_OP_SUM | Aggregator.AGG_OP_AVG
            | Aggregator.AGG_OP_STDEV;
    return 0 != (aggOps & statsMask);
  }
}
