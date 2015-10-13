package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Single column aggregator.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public abstract class PrimitiveAggregator implements Aggregator, Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The different aggregations that can be used when aggregating built-in types.
   */
  public enum AggregationOp {
    /** COUNT. Applies to all types. Result is always of type {@link Type#LONG_TYPE}. */
    COUNT,
    /** MIN. Applies to all types. Result is same as input type. */
    MIN,
    /** MAX. Applies to all types. Result is same as input type. */
    MAX,
    /**
     * SUM. Applies to numeric types. Result is the bigger numeric type, i.e., {@link Type#INT_TYPE}
     * -> {@link Type#LONG_TYPE} and . {@link Type#FLOAT_TYPE} -> {@link Type#DOUBLE_TYPE}.
     */
    SUM,
    /** AVG. Applies to numeric types. Result is always {@link Type#DOUBLE_TYPE}. */
    AVG,
    /** STDEV. Applies to numeric types. Result is always {@link Type#DOUBLE_TYPE}. */
    STDEV
  };

  /** Does this aggregator need to compute the count? */
  protected final boolean needsCount;
  /** Does this aggregator need to compute the sum? */
  protected final boolean needsSum;
  /** Does this aggregator need to compute the sum squared? */
  protected final boolean needsSumSq;
  /** Does this aggregator need to compute the max? */
  protected final boolean needsMax;
  /** Does this aggregator need to compute the min? */
  protected final boolean needsMin;
  /** Does this aggregator need to compute tuple-level stats? */
  protected final boolean needsStats;
  /**
   * Aggregate operations. A set of all valid aggregation operations, i.e. those in
   * {@link LongAggregator#AVAILABLE_AGG} .
   * 
   * Note that we use a {@link LinkedHashSet} to ensure that the iteration order is consistent!
   */
  protected final LinkedHashSet<AggregationOp> aggOps;

  /**
   * Result schema. It's automatically generated according to the {@link #aggOps}.
   */
  private final Schema resultSchema;

  /**
   * Instantiate a PrimitiveAggregator that computes the specified aggregates.
   * 
   * @param fieldName the name of the field being aggregated, for naming output columns.
   * @param aggOps the set of aggregate operations to be computed.
   */
  protected PrimitiveAggregator(final String fieldName, final AggregationOp[] aggOps) {
    Objects.requireNonNull(aggOps, "aggOps");
    Objects.requireNonNull(fieldName, "fieldName");

    this.aggOps = new LinkedHashSet<>(Arrays.asList(aggOps));

    if (!getAvailableAgg().containsAll(this.aggOps)) {
      throw new IllegalArgumentException("Unsupported aggregation(s): "
          + Sets.difference(this.aggOps, getAvailableAgg()));
    }

    if (aggOps.length == 0) {
      throw new IllegalArgumentException("No aggregation operations are selected");
    }

    needsCount = AggUtils.needsCount(this.aggOps);
    needsSum = AggUtils.needsSum(this.aggOps);
    needsSumSq = AggUtils.needsSumSq(this.aggOps);
    needsMin = AggUtils.needsMin(this.aggOps);
    needsMax = AggUtils.needsMax(this.aggOps);
    needsStats = AggUtils.needsStats(this.aggOps);

    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    for (AggregationOp op : this.aggOps) {
      switch (op) {
        case COUNT:
          types.add(Type.LONG_TYPE);
          names.add("count_" + fieldName);
          break;
        case MAX:
          types.add(getType());
          names.add("max_" + fieldName);
          break;
        case MIN:
          types.add(getType());
          names.add("min_" + fieldName);
          break;
        case AVG:
          types.add(Type.DOUBLE_TYPE);
          names.add("avg_" + fieldName);
          break;
        case STDEV:
          types.add(Type.DOUBLE_TYPE);
          names.add("stdev_" + fieldName);
          break;
        case SUM:
          types.add(getSumType());
          names.add("sum_" + fieldName);
          break;
      }
    }
    resultSchema = new Schema(types, names);
  }

  /**
   * Returns the Type of the SUM aggregate.
   * 
   * @return the Type of the SUM aggregate.
   */
  protected abstract Type getSumType();

  /**
   * Returns the set of aggregation operations that are supported by this aggregator.
   * 
   * @return the set of aggregation operations that are supported by this aggregator.
   */
  protected abstract Set<AggregationOp> getAvailableAgg();

  /**
   * @return The {@link Type} of the values this aggregator handles.
   */
  public abstract Type getType();

  @Override
  public final Schema getResultSchema() {
    return resultSchema;
  }
}
