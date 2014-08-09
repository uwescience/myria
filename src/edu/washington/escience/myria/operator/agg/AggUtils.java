package edu.washington.escience.myria.operator.agg;

import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.ReadableTable;

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
    int countMask =
        PrimitiveAggregator.AGG_OP_COUNT | PrimitiveAggregator.AGG_OP_AVG | PrimitiveAggregator.AGG_OP_STDEV;
    return 0 != (aggOps & countMask);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if sum must be computed.
   */
  public static boolean needsSum(final int aggOps) {
    int sumMask = PrimitiveAggregator.AGG_OP_SUM | PrimitiveAggregator.AGG_OP_AVG | PrimitiveAggregator.AGG_OP_STDEV;
    return 0 != (aggOps & sumMask);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if sumSq must be computed.
   */
  public static boolean needsSumSq(final int aggOps) {
    int sumSqMask = PrimitiveAggregator.AGG_OP_STDEV;
    return 0 != (aggOps & sumSqMask);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if min must be computed.
   */
  public static boolean needsMin(final int aggOps) {
    return 0 != (aggOps & PrimitiveAggregator.AGG_OP_MIN);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if max must be computed.
   */
  public static boolean needsMax(final int aggOps) {
    return 0 != (aggOps & PrimitiveAggregator.AGG_OP_MAX);
  }

  /**
   * @param aggOps the aggregate operations
   * @return true if tuple-level stats must be computed.
   */
  public static boolean needsStats(final int aggOps) {
    int statsMask =
        PrimitiveAggregator.AGG_OP_MIN | PrimitiveAggregator.AGG_OP_MAX | PrimitiveAggregator.AGG_OP_SUM
            | PrimitiveAggregator.AGG_OP_AVG | PrimitiveAggregator.AGG_OP_STDEV;
    return 0 != (aggOps & statsMask);
  }

  /**
   * Add a value to an aggregator from a {@link ReadableTable}.
   * 
   * @param from the source of the data.
   * @param fromRow which row.
   * @param fromColumn which column.
   * @param agg the aggregator.
   */
  public static void addValue2Group(final ReadableTable from, final int fromRow, final int fromColumn,
      final PrimitiveAggregator agg) {
    switch (agg.getType()) {
      case BOOLEAN_TYPE:
        ((BooleanAggregator) agg).addBoolean(from.getBoolean(fromColumn, fromRow));
        break;
      case DATETIME_TYPE:
        ((DateTimeAggregator) agg).addDateTime(from.getDateTime(fromColumn, fromRow));
        break;
      case DOUBLE_TYPE:
        ((DoubleAggregator) agg).addDouble(from.getDouble(fromColumn, fromRow));
        break;
      case FLOAT_TYPE:
        ((FloatAggregator) agg).addFloat(from.getFloat(fromColumn, fromRow));
        break;
      case INT_TYPE:
        ((IntegerAggregator) agg).addInt(from.getInt(fromColumn, fromRow));
        break;
      case LONG_TYPE:
        ((LongAggregator) agg).addLong(from.getLong(fromColumn, fromRow));
        break;
      case STRING_TYPE:
        ((StringAggregator) agg).addString(from.getString(fromColumn, fromRow));
        break;
    }
  }

  /**
   * @param type the type of the aggregator.
   * @param inputName the name of the column in the child schema
   * @param aggOps the aggregate operations
   * @return an {@link PrimitiveAggregator} for the specified type, column name, and operations.
   */
  public static PrimitiveAggregator allocate(final Type type, final String inputName, final int aggOps) {
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
   * @param childSchema the schema of the child of the aggregate operator
   * @param aggColumns which columns are aggregated over
   * @param aggOps the aggregate operations corresponding to each column
   * @return an array of {@link PrimitiveAggregator}s, one for each column and corresponding aggregate operations
   */
  public static PrimitiveAggregator[] allocate(final Schema childSchema, final int[] aggColumns, final int[] aggOps) {
    Preconditions.checkArgument(aggColumns.length == aggOps.length, "mismatched agg lengths");
    PrimitiveAggregator[] ret = new PrimitiveAggregator[aggColumns.length];
    for (int i = 0; i < aggColumns.length; ++i) {
      int column = aggColumns[i];
      ret[i] = AggUtils.allocate(childSchema.getColumnType(column), childSchema.getColumnName(column), aggOps[i]);
    }
    return ret;
  }
}
