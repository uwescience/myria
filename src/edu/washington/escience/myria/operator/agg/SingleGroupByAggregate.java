package edu.washington.escience.myria.operator.agg;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
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
   * A cache of the input schema.
   */
  private Schema inputSchema;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * String
   */
  private transient HashMap<String, Aggregator[]> groupAggsString;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array} when the group key is
   * DateTime.
   */
  private transient HashMap<DateTime, Aggregator[]> groupAggsDatetime;

  /**
   * The buffer storing in-progress group by results when the group key is int.
   */
  private transient TIntObjectMap<Aggregator[]> groupAggsInt;
  /**
   * The buffer storing in-progress group by results when the group key is boolean.
   */
  private transient Aggregator[][] groupAggsBoolean;
  /**
   * The buffer storing in-progress group by results when the group key is long.
   */
  private transient TLongObjectMap<Aggregator[]> groupAggsLong;
  /**
   * The buffer storing in-progress group by results when the group key is float.
   */
  private transient TFloatObjectMap<Aggregator[]> groupAggsFloat;
  /**
   * The buffer storing in-progress group by results when the group key is double.
   */
  private transient TDoubleObjectMap<Aggregator[]> groupAggsDouble;

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
  public SingleGroupByAggregate(final Operator child, final int gfield, final AggregatorFactory[] factories) {
    super(child);
    gColumn = Objects.requireNonNull(gfield, "gfield");
    this.factories = Objects.requireNonNull(factories, "factories");
  }

  /**
   * Utility constructor for simplifying hand-written code. Constructs a SingleGroupByAggregate with only a single
   * Aggregator.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param gfield The column over which we are grouping the result.
   * @param factory Factory for the aggregation operator to use.
   */
  public SingleGroupByAggregate(final Operator child, final int gfield, final AggregatorFactory factory) {
    this(child, gfield, new AggregatorFactory[] { Objects.requireNonNull(factory, "factory") });
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
   * Utility function to fetch or create/initialize the aggregators for the group corresponding to the data in the
   * specified table and row.
   * 
   * @param table the data to be aggregated.
   * @param row which row of the table is to be aggregated.
   * @return the aggregators for that row.
   */
  private Aggregator[] getGroupAggregators(final ReadableTable table, final int row) {
    Aggregator[] groupAgg = null;
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        boolean groupByBool = table.getBoolean(gColumn, row);
        if (groupByBool) {
          groupAgg = groupAggsBoolean[0];
        } else {
          groupAgg = groupAggsBoolean[1];
        }
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          if (groupByBool) {
            groupAggsBoolean[0] = groupAgg;
          } else {
            groupAggsBoolean[1] = groupAgg;
          }
        }
        break;
      case STRING_TYPE:
        String groupByString = table.getString(gColumn, row);
        groupAgg = groupAggsString.get(groupByString);
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          groupAggsString.put(groupByString, groupAgg);
        }
        break;
      case DATETIME_TYPE:
        DateTime groupByDateTime = table.getDateTime(gColumn, row);
        groupAgg = groupAggsDatetime.get(groupByDateTime);
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          groupAggsDatetime.put(groupByDateTime, groupAgg);
        }
        break;
      case INT_TYPE:
        int groupByInt = table.getInt(gColumn, row);
        groupAgg = groupAggsInt.get(groupByInt);
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          groupAggsInt.put(groupByInt, groupAgg);
        }
        break;
      case LONG_TYPE:
        long groupByLong = table.getLong(gColumn, row);
        groupAgg = groupAggsLong.get(groupByLong);
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          groupAggsLong.put(groupByLong, groupAgg);
        }
        break;
      case FLOAT_TYPE:
        float groupByFloat = table.getFloat(gColumn, row);
        groupAgg = groupAggsFloat.get(groupByFloat);
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          groupAggsFloat.put(groupByFloat, groupAgg);
        }
        break;
      case DOUBLE_TYPE:
        double groupByDouble = table.getDouble(gColumn, row);
        groupAgg = groupAggsDouble.get(groupByDouble);
        if (groupAgg == null) {
          groupAgg = allocateAggs();
          groupAggsDouble.put(groupByDouble, groupAgg);
        }
        break;
    }
    if (groupAgg == null) {
      throw new IllegalStateException("Aggregating values of unknown type.");
    }
    return groupAgg;
  }

  /**
   * @param tb the TupleBatch to be processed.
   */
  private void processTupleBatch(final TupleBatch tb) {
    for (int i = 0; i < tb.numTuples(); ++i) {
      Aggregator[] groupAgg = getGroupAggregators(tb, i);
      for (Aggregator agg : groupAgg) {
        agg.addRow(tb, i);
      }
    }
  }

  /**
   * @param resultBuffer where the results are stored.
   */
  private void generateResult(final TupleBatchBuffer resultBuffer) {
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        Aggregator[] t = groupAggsBoolean[0];
        if (t != null) {
          resultBuffer.putBoolean(0, true);
          int fromIndex = 1;
          for (final Aggregator element : t) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        Aggregator[] f = groupAggsBoolean[1];
        if (f != null) {
          resultBuffer.putBoolean(1, true);
          int fromIndex = 1;
          for (final Aggregator element : f) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case STRING_TYPE:
        for (final Map.Entry<String, Aggregator[]> e : groupAggsString.entrySet()) {
          final String groupByValue = e.getKey();
          final Aggregator[] aggLocal = e.getValue();
          resultBuffer.putString(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case DATETIME_TYPE:
        for (final Map.Entry<DateTime, Aggregator[]> e : groupAggsDatetime.entrySet()) {
          final DateTime groupByValue = e.getKey();
          final Aggregator[] aggLocal = e.getValue();
          resultBuffer.putDateTime(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case INT_TYPE:
        TIntObjectIterator<Aggregator[]> itInt = groupAggsInt.iterator();
        while (itInt.hasNext()) {
          itInt.advance();
          int groupByValue = itInt.key();
          final Aggregator[] aggLocal = itInt.value();
          resultBuffer.putInt(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case LONG_TYPE:
        TLongObjectIterator<Aggregator[]> itLong = groupAggsLong.iterator();
        while (itLong.hasNext()) {
          itLong.advance();
          long groupByValue = itLong.key();
          final Aggregator[] aggLocal = itLong.value();
          resultBuffer.putLong(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        groupAggsLong = new TLongObjectHashMap<Aggregator[]>();
        break;
      case FLOAT_TYPE:
        groupAggsFloat = new TFloatObjectHashMap<Aggregator[]>();
        TFloatObjectIterator<Aggregator[]> itFloat = groupAggsFloat.iterator();
        while (itFloat.hasNext()) {
          itFloat.advance();
          float groupByValue = itFloat.key();
          final Aggregator[] aggLocal = itFloat.value();
          resultBuffer.putFloat(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
            element.getResult(resultBuffer, fromIndex);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        break;
      case DOUBLE_TYPE:
        TDoubleObjectIterator<Aggregator[]> itDouble = groupAggsDouble.iterator();
        while (itDouble.hasNext()) {
          itDouble.advance();
          double groupByValue = itDouble.key();
          final Aggregator[] aggLocal = itDouble.value();
          resultBuffer.putDouble(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
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

    if (child.eos()) {
      return null;
    }

    while ((tb = child.nextReady()) != null) {

      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("get a TB from child");
      }
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
    generateSchema();
    inputSchema = getChild().getSchema();
    Preconditions.checkState(inputSchema != null, "child schema is null");
    switch (gColumnType) {
      case BOOLEAN_TYPE:
        groupAggsBoolean = new Aggregator[2][];
        break;
      case INT_TYPE:
        groupAggsInt = new TIntObjectHashMap<Aggregator[]>();
        break;
      case LONG_TYPE:
        groupAggsLong = new TLongObjectHashMap<Aggregator[]>();
        break;
      case FLOAT_TYPE:
        groupAggsFloat = new TFloatObjectHashMap<Aggregator[]>();
        break;
      case DOUBLE_TYPE:
        groupAggsDouble = new TDoubleObjectHashMap<Aggregator[]>();
        break;
      case STRING_TYPE:
        groupAggsString = new HashMap<String, Aggregator[]>();
        break;
      case DATETIME_TYPE:
        groupAggsDatetime = new HashMap<DateTime, Aggregator[]>();
        break;
    }
    resultBuffer = new TupleBatchBuffer(getSchema());

  }

  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    inputSchema = child.getSchema();
    if (inputSchema == null) {
      return null;
    }

    if (gColumn < 0 || gColumn >= inputSchema.numColumns()) {
      throw new IllegalArgumentException("Invalid group field");
    }

    Schema outputSchema = null;

    outputSchema =
        new Schema(ImmutableList.of(inputSchema.getColumnType(gColumn)), ImmutableList.of(inputSchema
            .getColumnName(gColumn)));

    gColumnType = inputSchema.getColumnType(gColumn);
    for (AggregatorFactory f : factories) {
      outputSchema = Schema.merge(outputSchema, f.get(inputSchema).getResultSchema());
    }
    return outputSchema;
  }

  /**
   * Utility class to allocate a set of aggregators from the factories.
   * 
   * @return the aggregators for this operator.
   */
  private Aggregator[] allocateAggs() {
    Aggregator[] aggregators = new Aggregator[factories.length];
    for (int j = 0; j < factories.length; ++j) {
      aggregators[j] = factories[j].get(inputSchema);
    }
    return aggregators;
  }
}
