package edu.washington.escience.myriad.operator.agg;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import gnu.trove.iterator.TDoubleObjectIterator;
import gnu.trove.iterator.TFloatObjectIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TDoubleObjectMap;
import gnu.trove.map.TFloatObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TDoubleObjectHashMap;
import gnu.trove.map.hash.TFloatObjectHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min) with a single group by column.
 */
public class SingleGroupByAggregateNoBuffer extends Operator {

  /**
   * The Logger.
   * */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory
      .getLogger(SingleGroupByAggregateNoBuffer.class);

  /**
   * default serialization ID.
   * */
  private static final long serialVersionUID = 1L;

  /**
   * result schema.
   * */
  private final Schema schema;
  /**
   * the child.
   * */
  private Operator child;

  /**
   * compute multiple aggregates in the same time. The columns to compute the aggregates are
   * {@link SingleGroupByAggregateNoIntermediateBuffer#afields}.
   * */
  private final Aggregator<?>[] agg;

  /**
   * Compute aggregate on each of the {@link SingleGroupByAggregateNoIntermediateBuffer#afields}, with the corresponding
   * {@link Aggregator} in @link SingleGroupByAggregate#agg}.
   * */
  private final int[] afields;

  /**
   * The group by column.
   * */
  private final int gColumn;

  /**
   * The group-by column type.
   * */
  private final Type gColumnType;

  /**
   * aggregate column types.
   * */
  private final Type[] aColumnTypes;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * String
   * */
  private transient HashMap<String, Aggregator<?>[]> groupAggs;

  /**
   * he buffer stroing in-progress group by results when the group key is int.
   * */
  private transient TIntObjectMap<Aggregator<?>[]> groupAggsInt;
  /**
   * he buffer stroing in-progress group by results when the group key is boolean.
   * */
  private transient Aggregator<?>[][] groupAggsBoolean;
  /**
   * he buffer stroing in-progress group by results when the group key is long.
   * */
  private transient TLongObjectMap<Aggregator<?>[]> groupAggsLong;
  /**
   * he buffer stroing in-progress group by results when the group key is float.
   * */
  private transient TFloatObjectMap<Aggregator<?>[]> groupAggsFloat;
  /**
   * he buffer stroing in-progress group by results when the group key is double.
   * */
  private transient TDoubleObjectMap<Aggregator<?>[]> groupAggsDouble;

  /**
   * storing results after group by is done.
   * */
  private transient TupleBatchBuffer resultBuffer;

  /**
   * Constructor.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param afields The columns over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result
   * @param aggOps The aggregation operator to use
   */
  public SingleGroupByAggregateNoBuffer(final Operator child, final int[] afields, final int gfield, final int[] aggOps) {
    Objects.requireNonNull(afields);
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }

    final Schema childSchema = child.getSchema();
    if (gfield < 0 || gfield >= childSchema.numColumns()) {
      throw new IllegalArgumentException("Invalid group field");
    }

    Schema outputSchema = null;

    outputSchema =
        new Schema(ImmutableList.of(childSchema.getColumnType(gfield)), ImmutableList.of(childSchema
            .getColumnName(gfield)));

    this.child = child;
    this.afields = afields;
    gColumn = gfield;
    gColumnType = childSchema.getColumnType(gColumn);
    aColumnTypes = new Type[this.afields.length];

    agg = new Aggregator<?>[aggOps.length];

    int idx = 0;
    for (final int afield : afields) {
      aColumnTypes[idx] = childSchema.getColumnType(afield);
      switch (aColumnTypes[idx]) {
        case BOOLEAN_TYPE:
          agg[idx] = new BooleanAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case INT_TYPE:
          agg[idx] = new IntegerAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case LONG_TYPE:
          agg[idx] = new LongAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case FLOAT_TYPE:
          agg[idx] = new FloatAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case DOUBLE_TYPE:
          agg[idx] = new DoubleAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case STRING_TYPE:
          agg[idx] = new StringAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
      }
      idx++;
    }
    schema = outputSchema;
  }

  /**
   * @return the aggregate field
   * */
  public final int[] aggregateFields() {
    return afields;
  }

  @Override
  protected final void cleanup() throws DbException {

    groupAggs = null;
    groupAggsDouble = null;
    groupAggsFloat = null;
    groupAggsInt = null;
    groupAggsLong = null;
    resultBuffer = null;
  }

  /**
   * add a value to group by.
   * 
   * @param tb source TB.
   * @param row which row.
   * @param aggFieldIdx which column.
   * @param aggFieldType the type of the value
   * @param agg the aggregator.
   * */
  public static void addValue2Group(final TupleBatch tb, final int row, final int aggFieldIdx, final Type aggFieldType,
      final Aggregator<?> agg) {
    switch (aggFieldType) {
      case BOOLEAN_TYPE:
        ((BooleanAggregator) agg).add(tb.getBoolean(aggFieldIdx, row));
        break;
      case INT_TYPE:
        ((IntegerAggregator) agg).add(tb.getInt(aggFieldIdx, row));
        break;
      case LONG_TYPE:
        ((LongAggregator) agg).add(tb.getLong(aggFieldIdx, row));
        break;
      case FLOAT_TYPE:
        ((FloatAggregator) agg).add(tb.getFloat(aggFieldIdx, row));
        break;
      case DOUBLE_TYPE:
        ((DoubleAggregator) agg).add(tb.getDouble(aggFieldIdx, row));
        break;
      case STRING_TYPE:
        ((StringAggregator) agg).add(tb.getString(aggFieldIdx, row));
        break;
    }
  }

  /**
   * @param tb the TupleBatch to be processed.
   * */
  private void processTupleBatch(final TupleBatch tb) {
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          boolean groupByKey = tb.getBoolean(gColumn, i);
          Aggregator<?>[] groupAgg = null;
          if (groupByKey) {
            groupAgg = groupAggsBoolean[0];
          } else {
            groupAgg = groupAggsBoolean[1];
          }
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            if (groupByKey) {
              groupAggsBoolean[0] = groupAgg;
            } else {
              groupAggsBoolean[1] = groupAgg;
            }
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
      case STRING_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          String groupByKey = tb.getString(gColumn, i);
          Aggregator<?>[] groupAgg = groupAggs.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggs.put(groupByKey, groupAgg);
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
      case INT_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          int groupByKey = tb.getInt(gColumn, i);
          Aggregator<?>[] groupAgg = groupAggsInt.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggsInt.put(groupByKey, groupAgg);
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
      case LONG_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          long groupByKey = tb.getLong(gColumn, i);
          Aggregator<?>[] groupAgg = groupAggsLong.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggsLong.put(groupByKey, groupAgg);
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
      case FLOAT_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          float groupByKey = tb.getFloat(gColumn, i);
          Aggregator<?>[] groupAgg = groupAggsFloat.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggsFloat.put(groupByKey, groupAgg);
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
      case DOUBLE_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          double groupByKey = tb.getDouble(gColumn, i);
          Aggregator<?>[] groupAgg = groupAggsDouble.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggsDouble.put(groupByKey, groupAgg);
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
    }

  }

  /**
   * @param resultBuffer where the results are stored.
   * */
  private void generateResult(final TupleBatchBuffer resultBuffer) {

    switch (gColumnType) {
      case BOOLEAN_TYPE:
        Aggregator<?>[] t = groupAggsBoolean[0];
        if (t != null) {
          resultBuffer.put(0, true);
          int fromIndex = 1;
          for (final Aggregator<?> element : t) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        Aggregator<?>[] f = groupAggsBoolean[1];
        if (f != null) {
          resultBuffer.put(1, true);
          int fromIndex = 1;
          for (final Aggregator<?> element : f) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case STRING_TYPE:
        for (final Map.Entry<String, Aggregator<?>[]> e : groupAggs.entrySet()) {
          final String groupByValue = e.getKey();
          final Aggregator<?>[] aggLocal = e.getValue();
          resultBuffer.put(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator<?> element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case INT_TYPE:
        TIntObjectIterator<Aggregator<?>[]> itInt = groupAggsInt.iterator();
        while (itInt.hasNext()) {
          itInt.advance();
          int groupByValue = itInt.key();
          final Aggregator<?>[] aggLocal = itInt.value();
          resultBuffer.put(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator<?> element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case LONG_TYPE:
        TLongObjectIterator<Aggregator<?>[]> itLong = groupAggsLong.iterator();
        while (itLong.hasNext()) {
          itLong.advance();
          long groupByValue = itLong.key();
          final Aggregator<?>[] aggLocal = itLong.value();
          resultBuffer.put(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator<?> element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        groupAggsLong = new TLongObjectHashMap<Aggregator<?>[]>();
        break;
      case FLOAT_TYPE:
        groupAggsFloat = new TFloatObjectHashMap<Aggregator<?>[]>();
        TFloatObjectIterator<Aggregator<?>[]> itFloat = groupAggsFloat.iterator();
        while (itFloat.hasNext()) {
          itFloat.advance();
          float groupByValue = itFloat.key();
          final Aggregator<?>[] aggLocal = itFloat.value();
          resultBuffer.put(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator<?> element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case DOUBLE_TYPE:
        TDoubleObjectIterator<Aggregator<?>[]> itDouble = groupAggsDouble.iterator();
        while (itDouble.hasNext()) {
          itDouble.advance();
          double groupByValue = itDouble.key();
          final Aggregator<?>[] aggLocal = itDouble.value();
          resultBuffer.put(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator<?> element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
    }

  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;

    if (resultBuffer.numTuples() > 0) {
      return resultBuffer.popAny();
    }

    if (child.eos() || child.eoi()) {
      return null;
    }

    while ((tb = child.nextReady()) != null) {

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("get a TB from child");
      }
      processTupleBatch(tb);
    }

    if (child.eos() || child.eoi()) {
      generateResult(resultBuffer);
    }
    return resultBuffer.popAny();
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  /**
   * @return the group by column.
   * */
  public final int getGroupByColumn() {
    return gColumn;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        groupAggsBoolean = new Aggregator<?>[2][];
        break;
      case INT_TYPE:
        groupAggsInt = new TIntObjectHashMap<Aggregator<?>[]>();
        break;
      case LONG_TYPE:
        groupAggsLong = new TLongObjectHashMap<Aggregator<?>[]>();
        break;
      case FLOAT_TYPE:
        groupAggsFloat = new TFloatObjectHashMap<Aggregator<?>[]>();
        break;
      case DOUBLE_TYPE:
        groupAggsDouble = new TDoubleObjectHashMap<Aggregator<?>[]>();
        break;
      case STRING_TYPE:
        groupAggs = new HashMap<String, Aggregator<?>[]>();
        break;
    }
    resultBuffer = new TupleBatchBuffer(schema);

  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

}
