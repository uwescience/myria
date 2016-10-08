package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.gs.collections.impl.map.mutable.primitive.DoubleObjectHashMap;
import com.gs.collections.impl.map.mutable.primitive.FloatObjectHashMap;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;
import com.gs.collections.impl.map.mutable.primitive.LongObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min) with a single group by column.
 */
public class SingleGroupByAggregate extends UnaryOperator {

  /**
   * The Logger.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SingleGroupByAggregate.class);

  /**
   * default serialization ID.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Factories to create the {@link Aggregator}s.
   */
  private final AggregatorFactory[] factories;

  /**
   * The group by column.
   */
  private final int gColumn;

  /**
   * A cache of the group-by column type.
   */
  private Type gColumnType;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * String
   */
  private transient HashMap<String, Object[]> stringAggState;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * DateTime.
   */
  private transient HashMap<DateTime, Object[]> datetimeAggState;

  /**
   * The buffer storing in-progress group by results when the group key is int.
   */
  private transient IntObjectHashMap<Object[]> intAggState;
  /**
   * The buffer storing in-progress group by results when the group key is boolean.
   */
  private transient Object[][] booleanAggState;
  /**
   * The buffer storing in-progress group by results when the group key is long.
   */
  private transient LongObjectHashMap<Object[]> longAggState;
  /**
   * The buffer storing in-progress group by results when the group key is float.
   */
  private transient FloatObjectHashMap<Object[]> floatAggState;
  /**
   * The buffer storing in-progress group by results when the group key is double.
   */
  private transient DoubleObjectHashMap<Object[]> doubleAggState;
  /**
   * The aggregators that will initialize and update the state.
   */
  private transient HashMap<Object, List<TupleBatch>> ltb;
  private transient HashMap<Object, BitSet> tbbs;
  private Aggregator[] aggregators;

  /**
   * The buffer storing results after group by is done.
   */
  private transient TupleBatchBuffer resultBuffer;

  /**
   * Constructor.
   *
   * @param child The Operator that is feeding us tuples.
   * @param gfield The column over which we are grouping the result.
   * @param factories Factories for the aggregation operators to use.
   */
  public SingleGroupByAggregate(@Nullable final Operator child, final int gfield,
      final AggregatorFactory... factories) {
    super(child);
    gColumn = Objects.requireNonNull(gfield, "gfield");
    this.factories = Objects.requireNonNull(factories, "factories");
    if (child == null) {
      LOGGER.info("child is null ...");
    }

  }

  @Override
  protected final void cleanup() throws DbException {
    stringAggState = null;
    datetimeAggState = null;
    doubleAggState = null;
    booleanAggState = null;
    floatAggState = null;
    intAggState = null;
    longAggState = null;
    resultBuffer = null;
  }

  @Override
  public void sendEos() throws DbException {
    for (Aggregator aggregator : aggregators) {
      aggregator.sendEos();
    }

  }

  /**
   * Utility function to fetch or create/initialize the aggregation state for the group corresponding to the data in the
   * specified table and row.
   *
   * @param table the data to be aggregated.
   * @param row which row of the table is to be aggregated.
   * @return the aggregation state for that row.
   * @throws DbException if there is an error.
   */
  private Object[] getAggState(final ReadableTable table, final int row) throws DbException {
    Object[] aggState = null;
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        boolean groupByBool = table.getBoolean(gColumn, row);
        if (groupByBool) {
          aggState = booleanAggState[0];
        } else {
          aggState = booleanAggState[1];
        }
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          if (groupByBool) {
            booleanAggState[0] = aggState;
          } else {
            booleanAggState[1] = aggState;
          }
        }
        break;
      case STRING_TYPE:
        String groupByString = table.getString(gColumn, row);
        aggState = stringAggState.get(groupByString);
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          stringAggState.put(groupByString, aggState);
        }
        break;
      case DATETIME_TYPE:
        DateTime groupByDateTime = table.getDateTime(gColumn, row);
        aggState = datetimeAggState.get(groupByDateTime);
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          datetimeAggState.put(groupByDateTime, aggState);
        }
        break;
      case INT_TYPE:
        int groupByInt = table.getInt(gColumn, row);
        aggState = intAggState.get(groupByInt);
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          intAggState.put(groupByInt, aggState);
        }
        break;
      case LONG_TYPE:
        long groupByLong = table.getLong(gColumn, row);
        aggState = longAggState.get(groupByLong);
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          longAggState.put(groupByLong, aggState);
        }
        break;
      case FLOAT_TYPE:
        float groupByFloat = table.getFloat(gColumn, row);
        aggState = floatAggState.get(groupByFloat);
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          floatAggState.put(groupByFloat, aggState);
        }
        break;
      case DOUBLE_TYPE:
        double groupByDouble = table.getDouble(gColumn, row);
        aggState = doubleAggState.get(groupByDouble);
        if (aggState == null) {
          aggState = AggUtils.allocateAggStates(aggregators);
          doubleAggState.put(groupByDouble, aggState);
        }
        break;
    }
    if (aggState == null) {
      throw new IllegalStateException("Aggregating values of unknown type.");
    }
    return aggState;
  }

  /**
   * @param tb the TupleBatch to be processed.
   * @throws DbException if there is an error.
   */
  private void processTupleBatch(final TupleBatch tb) throws DbException {
    LOGGER.info("processing tuple batch");
    LOGGER.info("input schema for tuplebatch " + tb.getSchema().toString());

    for (int agg = 0; agg < aggregators.length; ++agg) {
      LOGGER.info("Aggclass in process tuplebatch: " + aggregators[agg].getClass().getName());

      for (int i = 0; i < tb.numTuples(); ++i) {
        Object[] groupAgg = getAggState(tb, i);
        if (aggregators[agg].getClass().getName().equals(StatefulUserDefinedAggregator.class.getName())) {
          setBitset(tb, i, groupAgg);
        } else {
          aggregators[agg].addRow(tb, i, groupAgg[agg]);
        }
      }
      if (aggregators[agg].getClass().getName().equals(StatefulUserDefinedAggregator.class.getName())) {
        updateltbGroups(tb);
      }
    }

  }

  private void updateltbGroups(final TupleBatch table) throws DbException {
    LOGGER.info("updating tuplebatch");
    LOGGER.info("columntype " + gColumnType.toString());

    switch (gColumnType) {
      case BOOLEAN_TYPE:
        for (int key = 0; key < 2; key++) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }
        }
        break;
      case STRING_TYPE:
        for (String key : stringAggState.keySet()) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }

        }
        break;
      case DATETIME_TYPE:
        for (DateTime key : datetimeAggState.keySet()) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }
        }
        break;
      case INT_TYPE:
        for (Integer key : intAggState.keySet().toArray()) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }
        }
        break;
      case LONG_TYPE:
        LOGGER.info("size of aggstate " + longAggState.keySet().size());
        for (Long key : longAggState.keySet().toArray()) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }
        }
        break;
      case FLOAT_TYPE:
        for (Float key : floatAggState.keySet().toArray()) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }

        }
        break;
      case DOUBLE_TYPE:
        for (Double key : doubleAggState.keySet().toArray()) {
          BitSet bs = tbbs.get(key);
          if (bs != null && !(bs.isEmpty())) {
            LOGGER.info("bit set is not empty" + bs.toString());
            List<TupleBatch> listTb = ltb.get(key);
            if (listTb == null) {
              List<TupleBatch> nlTb = new ArrayList<TupleBatch>();
              nlTb.add(table.filter(bs));
              ltb.put(key, nlTb);
            } else {
              listTb.add(table.filter(bs));
            }
            bs.clear();
          }
        }
        break;
      default:
        throw new DbException("type not supported for SingleColumnGroupby");
    }

  }

  private void setBitset(final ReadableTable table, final int row, final Object[] groupAgg) throws DbException {
    LOGGER.info("set bitset");
    BitSet bs;
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        boolean groupByBool = table.getBoolean(gColumn, row);
        bs = tbbs.get(groupByBool);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByBool, bs);
        } else {
          bs.set(row);
        }
        break;
      case STRING_TYPE:
        String groupByString = table.getString(gColumn, row);
        bs = tbbs.get(groupByString);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByString, bs);
        } else {
          bs.set(row);
        }
        break;
      case DATETIME_TYPE:
        DateTime groupByDateTime = table.getDateTime(gColumn, row);
        bs = tbbs.get(groupByDateTime);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByDateTime, bs);
        } else {
          bs.set(row);
        }
        break;
      case INT_TYPE:
        int groupByInt = table.getInt(gColumn, row);
        bs = tbbs.get(groupByInt);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByInt, bs);
        } else {
          bs.set(row);
        }
        break;
      case LONG_TYPE:
        long groupByLong = table.getLong(gColumn, row);
        bs = tbbs.get(groupByLong);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByLong, bs);
        } else {
          bs.set(row);
        }
        break;
      case FLOAT_TYPE:
        float groupByFloat = table.getFloat(gColumn, row);
        bs = tbbs.get(groupByFloat);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByFloat, bs);
        } else {
          bs.set(row);
        }
        break;
      case DOUBLE_TYPE:
        double groupByDouble = table.getDouble(gColumn, row);
        bs = tbbs.get(groupByDouble);
        if (bs == null) {
          bs = new BitSet(table.numTuples());
          bs.set(row);
          tbbs.put(groupByDouble, bs);
        } else {
          bs.set(row);
        }
        break;
      default:
        throw new DbException("type not supported for SingleColumnGroupby");
    }
  }

  /**
   * Helper function for appending results to an output tuple buffer. By convention, the single-column aggregation key
   * goes in column 0, and the aggregates are appended starting at column 1.
   *
   * @param resultBuffer where the tuples will be appended.
   * @param aggState the states corresponding to all aggregators.
   * @throws DbException if there is an error.
   * @throws IOException
   */
  private void concatResults(final TupleBatchBuffer resultBuffer, final Object[] aggState, final Object key)
      throws DbException, IOException {
    LOGGER.info("concat results called");
    int index = 1;
    for (int agg = 0; agg < aggregators.length; ++agg) {
      if (aggregators[agg].getClass().getName().equals(StatefulUserDefinedAggregator.class.getName())) {
        LOGGER.info("get in here?");
        LOGGER.info("key for sending tuples is" + key.toString());
        List<TupleBatch> listTb = ltb.get(key);
        if (listTb.size() > 0) {
          LOGGER.info("size of tuples for key " + key.toString() + " is " + listTb.size());
          aggregators[agg].add(listTb, aggState[agg]);
        }
      }

      aggregators[agg].getResult(resultBuffer, index, aggState[agg]);
      index += aggregators[agg].getResultSchema().numColumns();
    }
  }

  // private void sendTuples(final Object key, final Object[] aggState) throws DbException {
  // LOGGER.info("send tuples called!");
  // for (int agg = 0; agg < aggregators.length; ++agg) {
  //
  // @SuppressWarnings("unchecked")
  //
  //
  // }
  //
  // }
  // }

  /**
   * @param resultBuffer where the results are stored.
   * @throws DbException if there is an error.
   * @throws IOException
   */
  private void generateResult(final TupleBatchBuffer resultBuffer) throws DbException, IOException {
    LOGGER.info("generate results called in agg");

    switch (gColumnType) {
      case BOOLEAN_TYPE:
        for (int boolBucket = 0; boolBucket < 2; ++boolBucket) {
          Object[] aggState = booleanAggState[boolBucket];
          if (aggState != null) {
            /* True is index 0 in booleanAggState, False is index 1. */
            resultBuffer.putBoolean(0, boolBucket == 0);
            // sendTuples(boolBucket, aggState);
            concatResults(resultBuffer, aggState, boolBucket);
          }
        }
        break;
      case STRING_TYPE:
        for (final Map.Entry<String, Object[]> e : stringAggState.entrySet()) {
          resultBuffer.putString(0, e.getKey());
          // sendTuples(e.getKey(), e.getValue());
          concatResults(resultBuffer, e.getValue(), e.getKey());
        }
        break;
      case DATETIME_TYPE:
        for (final Map.Entry<DateTime, Object[]> e : datetimeAggState.entrySet()) {
          resultBuffer.putDateTime(0, e.getKey());
          // sendTuples(e.getKey(), e.getValue());
          concatResults(resultBuffer, e.getValue(), e.getKey());
        }
        break;
      case INT_TYPE:
        for (int key : intAggState.keySet().toArray()) {
          resultBuffer.putInt(0, key);
          // sendTuples(key, intAggState.get(key));
          concatResults(resultBuffer, intAggState.get(key), key);
        }
        break;
      case LONG_TYPE:
        for (long key : longAggState.keySet().toArray()) {
          LOGGER.info("key: " + key);
          resultBuffer.putLong(0, key);
          // sendTuples(key, longAggState.get(key));
          concatResults(resultBuffer, longAggState.get(key), key);
        }
        break;
      case FLOAT_TYPE:
        for (float key : floatAggState.keySet().toArray()) {
          resultBuffer.putFloat(0, key);
          // sendTuples(key, floatAggState.get(key));
          concatResults(resultBuffer, floatAggState.get(key), key);
        }
        break;
      case DOUBLE_TYPE:
        for (double key : doubleAggState.keySet().toArray()) {
          resultBuffer.putDouble(0, key);
          // sendTuples(key, doubleAggState.get(key));
          concatResults(resultBuffer, doubleAggState.get(key), key);
        }
        break;
    }
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException, IOException {
    TupleBatch tb = null;
    final Operator child = getChild();

    if (resultBuffer.numTuples() > 0) {
      return resultBuffer.popAny();
    }

    if (child.eos()) {
      return null;
    }

    while ((tb = child.nextReady()) != null) {

      processTupleBatch(tb);
    }

    if (child.eos()) {
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
    Preconditions.checkState(getSchema() != null, "unable to determine schema in init");

    aggregators = AggUtils.allocateAggs(factories, getChild().getSchema(), getPythonFunctionRegistrar());
    resultBuffer = new TupleBatchBuffer(getSchema());

    LOGGER.info("resultbuffer schema" + resultBuffer.getSchema().toString());
    LOGGER.info("column type" + gColumnType);
    ltb = new HashMap<Object, List<TupleBatch>>();
    tbbs = new HashMap<Object, BitSet>();

    switch (gColumnType) {
      case BOOLEAN_TYPE:
        booleanAggState = new Object[2][];

        break;
      case INT_TYPE:
        intAggState = new IntObjectHashMap<Object[]>();
        break;
      case LONG_TYPE:
        longAggState = new LongObjectHashMap<Object[]>();
        break;
      case FLOAT_TYPE:
        floatAggState = new FloatObjectHashMap<Object[]>();
        break;
      case DOUBLE_TYPE:
        doubleAggState = new DoubleObjectHashMap<Object[]>();
        break;
      case STRING_TYPE:
        stringAggState = new HashMap<String, Object[]>();
        break;
      case DATETIME_TYPE:
        datetimeAggState = new HashMap<DateTime, Object[]>();
        break;
    }

  }

  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    Schema inputSchema = child.getSchema();
    if (inputSchema == null) {
      return null;
    }
    LOGGER.info("input schema " + inputSchema.toString());

    Preconditions.checkElementIndex(gColumn, inputSchema.numColumns(), "group column");

    Schema outputSchema = Schema.ofFields(inputSchema.getColumnType(gColumn), inputSchema.getColumnName(gColumn));

    gColumnType = inputSchema.getColumnType(gColumn);
    try {
      for (Aggregator a : AggUtils.allocateAggs(factories, inputSchema, getPythonFunctionRegistrar())) {
        outputSchema = Schema.merge(outputSchema, a.getResultSchema());
      }
    } catch (DbException e) {
      throw new RuntimeException("unable to allocate aggregators to determine output schema", e);
    }
    return outputSchema;
  }
}
