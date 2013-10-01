package edu.washington.escience.myria.operator.agg;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
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
public class SingleGroupByAggregateNoBuffer extends UnaryOperator {

  /**
   * The Logger.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory
      .getLogger(SingleGroupByAggregateNoBuffer.class);

  /**
   * default serialization ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * compute multiple aggregates in the same time. The columns to compute the aggregates are
   * {@link SingleGroupByAggregateNoIntermediateBuffer#afields}.
   */
  private Aggregator<?>[] agg;

  /**
   * Compute aggregate on each of the {@link SingleGroupByAggregateNoIntermediateBuffer#afields}, with the corresponding
   * {@link Aggregator} in @link SingleGroupByAggregate#agg}.
   */
  private final int[] afields;

  /**
   * The aggregate operations that will be computed.
   */
  private final int[] aggOps;

  /**
   * The group by column.
   */
  private final int gColumn;

  /**
   * The group-by column type.
   */
  private Type gColumnType;

  /**
   * aggregate column types.
   */
  private Type[] aColumnTypes;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * String
   */
  private transient HashMap<String, Aggregator<?>[]> groupAggsString;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * DateTime.
   */
  private transient HashMap<DateTime, Aggregator<?>[]> groupAggsDatetime;

  /**
   * The buffer storing in-progress group by results when the group key is int.
   */
  private transient TIntObjectMap<Aggregator<?>[]> groupAggsInt;
  /**
   * The buffer storing in-progress group by results when the group key is boolean.
   */
  private transient Aggregator<?>[][] groupAggsBoolean;
  /**
   * The buffer storing in-progress group by results when the group key is long.
   */
  private transient TLongObjectMap<Aggregator<?>[]> groupAggsLong;
  /**
   * The buffer storing in-progress group by results when the group key is float.
   */
  private transient TFloatObjectMap<Aggregator<?>[]> groupAggsFloat;
  /**
   * The buffer storing in-progress group by results when the group key is double.
   */
  private transient TDoubleObjectMap<Aggregator<?>[]> groupAggsDouble;

  /**
   * The buffer storing results after group by is done.
   */
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
    super(child);
    Objects.requireNonNull(afields);
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }
    gColumn = gfield;
    this.afields = afields;
    this.aggOps = aggOps;
  }

  /**
   * @return the aggregate field
   */
  public final int[] aggregateFields() {
    return afields;
  }

  @Override
  protected final void cleanup() throws DbException {

    groupAggsString = null;
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
   */
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
      case DATETIME_TYPE:
        ((DateTimeAggregator) agg).add(tb.getDateTime(aggFieldIdx, row));
        break;

    }
  }

  /**
   * @param tb the TupleBatch to be processed.
   */
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
          Aggregator<?>[] groupAgg = groupAggsString.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggsString.put(groupByKey, groupAgg);
          }
          for (int j = 0; j < afields.length; j++) {
            addValue2Group(tb, i, afields[j], aColumnTypes[j], groupAgg[j]);
          }
        }
        break;
      case DATETIME_TYPE:
        for (int i = 0; i < tb.numTuples(); i++) {
          DateTime groupByKey = tb.getDateTime(gColumn, i);
          Aggregator<?>[] groupAgg = groupAggsDatetime.get(groupByKey);
          if (groupAgg == null) {
            groupAgg = new Aggregator<?>[agg.length];
            for (int j = 0; j < agg.length; j++) {
              groupAgg[j] = agg[j].freshCopyYourself();
            }
            groupAggsDatetime.put(groupByKey, groupAgg);
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
   */
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
        for (final Map.Entry<String, Aggregator<?>[]> e : groupAggsString.entrySet()) {
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
      case DATETIME_TYPE:
        for (final Map.Entry<DateTime, Aggregator<?>[]> e : groupAggsDatetime.entrySet()) {
          final DateTime groupByValue = e.getKey();
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
    final Operator child = getChild();

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

  /**
   * @return the group by column.
   */
  public final int getGroupByColumn() {
    return gColumn;
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    generateSchema();
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
        groupAggsString = new HashMap<String, Aggregator<?>[]>();
        break;
      case DATETIME_TYPE:
        groupAggsDatetime = new HashMap<DateTime, Aggregator<?>[]>();
        break;
    }
    resultBuffer = new TupleBatchBuffer(getSchema());

  }

  @Override
  protected Schema generateSchema() {
    final Schema childSchema = getChild().getSchema();
    if (gColumn < 0 || gColumn >= childSchema.numColumns()) {
      throw new IllegalArgumentException("Invalid group field");
    }

    Schema outputSchema = null;

    outputSchema =
        new Schema(ImmutableList.of(childSchema.getColumnType(gColumn)), ImmutableList.of(childSchema
            .getColumnName(gColumn)));

    gColumnType = childSchema.getColumnType(gColumn);
    aColumnTypes = new Type[afields.length];

    agg = new Aggregator<?>[aggOps.length];

    int idx = 0;
    for (final int afield : afields) {
      aColumnTypes[idx] = childSchema.getColumnType(afield);
      switch (aColumnTypes[idx]) {
        case BOOLEAN_TYPE:
          agg[idx] = new BooleanAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case INT_TYPE:
          agg[idx] = new IntegerAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case LONG_TYPE:
          agg[idx] = new LongAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case FLOAT_TYPE:
          agg[idx] = new FloatAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case DOUBLE_TYPE:
          agg[idx] = new DoubleAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case STRING_TYPE:
          agg[idx] = new StringAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        default:
          throw new IllegalArgumentException("Unknown column type: " + aColumnTypes[idx]);
      }

      outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
      idx++;
    }
    return outputSchema;
  }
}
