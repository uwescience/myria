package edu.washington.escience.myria.operator.agg;

import java.io.Serializable;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.AppendableTable;

/**
 * Single column aggregator.
 */
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
     * SUM. Applies to numeric types. Result is the bigger numeric type, i.e., {@link Type#INT_TYPE} ->
     * {@link Type#LONG_TYPE} and . {@link Type#FLOAT_TYPE} -> {@link Type#DOUBLE_TYPE}.
     */
    SUM,
    /** AVG. Applies to numeric types. Result is always {@link Type#DOUBLE_TYPE}. */
    AVG,
    /** STDEV. Applies to numeric types. Result is always {@link Type#DOUBLE_TYPE}. */
    STDEV,
    /** SUM_SQUARED. Applies to numeric types. Result is the bigger numeric type. */
    SUM_SQUARED
  };

  /** The aggregate operation. */
  protected final AggregationOp aggOp;
  /** The column to aggregate on. */
  protected final int column;
  /** Column indices of this aggregator of the state hash table. */
  protected final int[] stateCols;
  /** The output name of the aggregate. */
  private final String outputName;

  /**
   * Instantiate a PrimitiveAggregator that computes the specified aggregates.
   *
   * @param fieldName the name of the field being aggregated, for naming output columns.
   * @param aggOps the set of aggregate operations to be computed.
   */
  protected PrimitiveAggregator(
      final String inputName, final int column, final AggregationOp aggOp, final int[] stateCols) {
    if (!isSupported(aggOp)) {
      throw new IllegalArgumentException("Unsupported aggregation " + aggOp);
    }
    this.aggOp = aggOp;
    this.column = column;
    this.stateCols = stateCols;
    this.outputName = aggOp.toString().toLowerCase() + "_" + inputName;
  }

  @Override
  public Schema getOutputSchema() {
    return Schema.ofFields(outputName, getOutputType());
  }

  /** @return The {@link Type} of the values this aggregator handles. */
  protected abstract Type getOutputType();

  /**
   * Initialize a state by appending an initial value to a column.
   *
   * @param data the table to append to
   * @param column the column to append to
   */
  protected abstract void appendInitValue(AppendableTable data, final int column);

  /**
   * @param aggOp
   * @return if aggOp is supported by this aggregator.
   */
  protected abstract boolean isSupported(AggregationOp aggOp);

  @Override
  public void initState(AppendableTable data) {
    for (int i = 0; i < stateCols.length; ++i) {
      appendInitValue(data, stateCols[i]);
    }
  }
}
