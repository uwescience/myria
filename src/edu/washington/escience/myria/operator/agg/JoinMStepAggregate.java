package edu.washington.escience.myria.operator.agg;

import java.util.HashMap;
import java.util.Objects;

import javax.annotation.Nullable;

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
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TDoubleObjectMap;
import gnu.trove.map.TFloatObjectMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min) with a single
 * group by column.
 */
public class JoinMStepAggregate extends UnaryOperator {

  /**
   * The Logger.
   */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(JoinMStepAggregate.class);

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
   * The number of component Gaussians in the model
   */
  private final int numComponents;

  /**
   * 
   */
  private final int numDimensions;

  /**
   * A cache of the input schema.
   */
  private Schema inputSchema;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array}
   * when the group key is String
   */
  private transient HashMap<String, Aggregator[]> groupAggsString;

  /**
   * The buffer storing in-progress group by results. {groupby-column-value -> Aggregator Array}
   * when the group key is DateTime.
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
  public JoinMStepAggregate(@Nullable final Operator child, final int gfield,
      final AggregatorFactory[] factories, final int numDimensions, final int numComponents) {
    super(child);
    gColumn = Objects.requireNonNull(gfield, "gfield");
    this.numDimensions = numDimensions;
    this.numComponents = numComponents;

    this.factories = Objects.requireNonNull(factories, "factories");
  }

  /**
   * Utility constructor for simplifying hand-written code. Constructs a SingleGroupByAggregate with
   * only a single Aggregator.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param gfield The column over which we are grouping the result.
   * @param factory Factory for the aggregation operator to use.
   */
  public JoinMStepAggregate(final Operator child, final int gfield, final AggregatorFactory factory,
      final int numDimensions, final int numComponents) {
    this(child, gfield, new AggregatorFactory[] {Objects.requireNonNull(factory, "factory")},
        numDimensions, numComponents);
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
   * Utility function to fetch or create/initialize the aggregators for the group corresponding to
   * the data in the specified table and row.
   * 
   * @param table the data to be aggregated.
   * @param row which row of the table is to be aggregated.
   * @return the aggregators for that row.
   * @throws DbException if there is an error.
   */
  private Aggregator[] getGroupAggregators(final ReadableTable table, final int row)
      throws DbException {
    Aggregator[] groupAgg = null;
    switch (gColumnType) {
      case LONG_TYPE:
        long groupByLong = table.getLong(gColumn, row);
        groupAgg = groupAggsLong.get(groupByLong);
        if (groupAgg == null) {
          groupAgg = new Aggregator[1];
          groupAgg[0] =
              new JoinMStepAggregator(inputSchema, 55, null, numDimensions, numComponents);
          // AggUtils.allocateAggs(factories, inputSchema);
          groupAggsLong.put(groupByLong, groupAgg);
          // groupAggsLong[0] = new EStepAggregator(inputSchema, 13,
          // groupAggs);
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
   * @throws DbException if there is an error.
   */
  private void processTupleBatch(final TupleBatch tb) throws DbException {
    for (int i = 0; i < tb.numTuples(); ++i) {
      Aggregator[] groupAgg = getGroupAggregators(tb, i);
      for (Aggregator agg : groupAgg) {
        agg.addRow(tb, i, null);
      }
    }
  }

  /**
   * @param resultBuffer where the results are stored.
   * @throws DbException if there is an error.
   */
  private void generateResult(final TupleBatchBuffer resultBuffer) throws DbException {
    switch (gColumnType) {
      case LONG_TYPE:
        TLongObjectIterator<Aggregator[]> itLong = groupAggsLong.iterator();
        while (itLong.hasNext()) {
          itLong.advance();
          long groupByValue = itLong.key();
          final Aggregator[] aggLocal = itLong.value();
          resultBuffer.putLong(0, groupByValue);
          int fromIndex = 1;
          for (final Aggregator element : aggLocal) {
            element.getResult(resultBuffer, fromIndex, null);
            fromIndex += element.getResultSchema().numColumns();
          }
        }
        groupAggsLong = new TLongObjectHashMap<Aggregator[]>();
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
      case LONG_TYPE:
        groupAggsLong = new TLongObjectHashMap<Aggregator[]>();
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

    outputSchema = new Schema(ImmutableList.of(inputSchema.getColumnType(gColumn)),
        ImmutableList.of(inputSchema.getColumnName(gColumn)));

    gColumnType = inputSchema.getColumnType(gColumn);
    // for (AggregatorFactory f : factories) {
    // try {
    // outputSchema = Schema.merge(outputSchema, f.get(inputSchema)
    // .getResultSchema());
    // } catch (DbException e) {
    // throw new RuntimeException("Error generating output schema", e);
    // }
    // }
    final ImmutableList.Builder<Type> aggtypes = ImmutableList.builder();
    final ImmutableList.Builder<String> aggnames = ImmutableList.builder();

    aggtypes.add(Type.DOUBLE_TYPE);
    aggnames.add("pi");

    for (int i = 0; i < numDimensions; i++) {
      aggtypes.add(Type.DOUBLE_TYPE);
      aggnames.add("mu" + (1 + i) + "1");
    }

    for (int i = 0; i < numDimensions; i++) {
      for (int j = 0; j < numDimensions; j++) {
        aggtypes.add(Type.DOUBLE_TYPE);
        aggnames.add("cov" + String.valueOf(i + 1) + String.valueOf(j + 1));
      }
    }

    outputSchema = Schema.merge(outputSchema, new Schema(aggtypes, aggnames));
    return outputSchema;
  }
}
