package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.ReadableTable;

/**
 * Single column aggregator.
 */
public abstract class PrimitiveAggregator implements Serializable {

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
     * SUM. Applies to numeric types. Result is the bigger numeric type, i.e., {@link Type#INT_TYPE} ->
     * {@link Type#LONG_TYPE} and . {@link Type#FLOAT_TYPE} -> {@link Type#DOUBLE_TYPE}.
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
   * Aggregate operations. A set of all valid aggregation operations, i.e. those in {@link LongAggregator#AVAILABLE_AGG}
   * .
   * 
   * Note that we use a {@link LinkedHashSet} to ensure that the iteration order is consistent!
   */
  protected final LinkedHashSet<AggregationOp> aggOps;

  /**
   * Instantiate a PrimitiveAggregator that computes the specified aggregates.
   * 
   * @param aggOps the set, i
   */
  protected PrimitiveAggregator(final AggregationOp[] aggOps) {
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
  }

  /**
   * Returns the set of aggregation operations that are supported by this aggregator.
   * 
   * @return the set of aggregation operations that are supported by this aggregator.
   */
  protected abstract Set<AggregationOp> getAvailableAgg();

  /**
   * Add the entire contents of {@link ReadableColumn} into the aggregate.
   * 
   * @param from the source {@link ReadableColumn}
   */
  abstract void add(ReadableColumn from);

  /**
   * Add the entire contents of the specified column from the {@link ReadableTable} into the aggregate.
   * 
   * @param from the source {@link ReadableTable}
   * @param fromColumn the column in the table to add values from
   */
  abstract void add(ReadableTable from, int fromColumn);

  /**
   * Add the value in the specified <code>column</code> and <code>row</code> in the given {@link ReadableTable} into the
   * aggregate.
   * 
   * @param table the source {@link ReadableTable}
   * @param column the column in <code>t</code> containing the value
   * @param row the row in <code>t</code> containing the value
   */
  abstract void add(ReadableTable table, int column, int row);

  /**
   * Output the aggregate result. Store the output to buffer.
   * 
   * @param dest the buffer to store the aggregate result.
   * @param destColumn from the fromIndex to put the result columns
   */
  abstract void getResult(AppendableTable dest, int destColumn);

  /**
   * All the count aggregates are of type Long. All the avg aggregates are of type Double. And each of the max/min/sum
   * aggregate has the same type as the column on which the aggregate is computed.
   * 
   * @return Result schema of this Aggregator.
   */
  abstract Schema getResultSchema();

  /**
   * @return The {@link Type} of the values this aggreagtor handles.
   */
  abstract Type getType();
}
