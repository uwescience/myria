package edu.washington.escience.myria.operator.agg;

import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBuffer;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * This aggregate operator computes the aggregation in streaming manner (requires input sorted on grouping column(s)).
 * This supports aggregation over multiple columns, with one or more group by columns. Intend to substitute
 * SingleGroupByAggregate and MultiGroupByAggregate when input is known to be sorted.
 *
 * @see Aggregate
 * @see SingleGroupByAggregate
 * @see MultiGroupByAggregate
 */
public class StreamingAggregate extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The schema of the aggregation result. */
  private Schema aggSchema;
  /** The schema of the columns indicated by the group keys. */
  private Schema groupSchema;
  /** Holds the current grouping key. */
  private Tuple curGroupKey;

  /** Group fields. **/
  private final int[] gFields;
  /** Group field types. **/
  private final Type[] gTypes;
  /** An array [0, 1, .., gFields.length-1] used for comparing tuples. */
  private final int[] gRange;
  /** Factories to make the Aggregators. **/
  private final AggregatorFactory[] factories;
  /** The actual Aggregators. **/
  private Aggregator[] aggregators;
  /** The state of the aggregators. */
  private Object[] aggregatorStates;

  /** Buffer for holding intermediate results. */
  private transient TupleBuffer resultBuffer;
  /** Buffer for holding finished results as tuple batches. */
  private transient ImmutableList<TupleBatch> finalBuffer;

  /**
   * Groups the input tuples according to the specified grouping fields, then produces the specified aggregates.
   *
   * @param child The Operator that is feeding us tuples.
   * @param gfields The columns over which we are grouping the result.
   * @param factories The factories that will produce the {@link Aggregator}s for each group.
   */
  public StreamingAggregate(@Nullable final Operator child, final int[] gfields, final AggregatorFactory... factories) {
    super(child);
    gFields = Objects.requireNonNull(gfields, "gfields");
    gTypes = new Type[gfields.length];
    this.factories = Objects.requireNonNull(factories, "factories");
    Preconditions.checkArgument(gfields.length > 0, " must have at least one group by field");
    Preconditions.checkArgument(factories.length != 0, "to use StreamingAggregate, must specify some aggregates");
    gRange = new int[gfields.length];
    for (int i = 0; i < gfields.length; ++i) {
      gRange[i] = i;
    }
  }

  /**
   * Returns the next tuple batch containing the result of this aggregate. Grouping field(s) followed by aggregate
   * field(s).
   *
   * @throws DbException if any error occurs.
   * @return result tuple batch
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    if (child.eos()) {
      return getResultBatch();
    }

    TupleBatch tb = child.nextReady();
    while (tb != null) {
      for (int row = 0; row < tb.numTuples(); ++row) {
        if (curGroupKey == null) {
          /*
           * first time accessing this tb, no aggregation performed previously
           */
          // store current group key as a tuple
          curGroupKey = new Tuple(groupSchema);
          for (int gKey = 0; gKey < gFields.length; ++gKey) {
            gTypes[gKey] = tb.getSchema().getColumnType(gFields[gKey]);
            switch (gTypes[gKey]) {
              case BOOLEAN_TYPE:
                curGroupKey.set(gKey, tb.getBoolean(gFields[gKey], row));
                break;
              case STRING_TYPE:
                curGroupKey.set(gKey, tb.getString(gFields[gKey], row));
                break;
              case DATETIME_TYPE:
                curGroupKey.set(gKey, tb.getDateTime(gFields[gKey], row));
                break;
              case INT_TYPE:
                curGroupKey.set(gKey, tb.getInt(gFields[gKey], row));
                break;
              case LONG_TYPE:
                curGroupKey.set(gKey, tb.getLong(gFields[gKey], row));
                break;
              case FLOAT_TYPE:
                curGroupKey.set(gKey, tb.getFloat(gFields[gKey], row));
                break;
              case DOUBLE_TYPE:
                curGroupKey.set(gKey, tb.getDouble(gFields[gKey], row));
                break;
            }
          }
        } else if (!TupleUtils.tupleEquals(tb, gFields, row, curGroupKey, gRange, 0)) {
          /*
           * different grouping key than current one, flush current agg result to result buffer
           */
          addToResult();
          // store current group key as a tuple
          for (int gKey = 0; gKey < gFields.length; ++gKey) {
            switch (gTypes[gKey]) {
              case BOOLEAN_TYPE:
                curGroupKey.set(gKey, tb.getBoolean(gFields[gKey], row));
                break;
              case STRING_TYPE:
                curGroupKey.set(gKey, tb.getString(gFields[gKey], row));
                break;
              case DATETIME_TYPE:
                curGroupKey.set(gKey, tb.getDateTime(gFields[gKey], row));
                break;
              case INT_TYPE:
                curGroupKey.set(gKey, tb.getInt(gFields[gKey], row));
                break;
              case LONG_TYPE:
                curGroupKey.set(gKey, tb.getLong(gFields[gKey], row));
                break;
              case FLOAT_TYPE:
                curGroupKey.set(gKey, tb.getFloat(gFields[gKey], row));
                break;
              case DOUBLE_TYPE:
                curGroupKey.set(gKey, tb.getDouble(gFields[gKey], row));
                break;
            }
          }
          reinitializeAggStates();
        }
        // update aggregator states with current tuple
        for (int agg = 0; agg < aggregators.length; ++agg) {
          aggregators[agg].addRow(tb, row, aggregatorStates[agg]);
        }
      }
      tb = child.nextReady();
    }

    /*
     * We know that child.nextReady() has returned <code>null</code>, so we have processed all tuple we can. Child is
     * either EOS or we have to wait for more data.
     */
    if (child.eos()) {
      addToResult();
      return getResultBatch();
    }
    return null;
  }

  /**
   * Re-initialize aggregator states for new group key.
   *
   * @throws DbException if any error
   */
  private void reinitializeAggStates() throws DbException {
    aggregatorStates = null;
    aggregatorStates = AggUtils.allocateAggStates(aggregators);
  }

  /**
   * Add aggregate results with previous grouping key to result buffer.
   *
   * @throws DbException if any error
   */
  private void addToResult() throws DbException {
    int fromIndex = 0;
    for (; fromIndex < curGroupKey.numColumns(); ++fromIndex) {
      TupleUtils.copyValue(curGroupKey, fromIndex, 0, resultBuffer, fromIndex);
    }
    for (int agg = 0; agg < aggregators.length; ++agg) {
      aggregators[agg].getResult(resultBuffer, fromIndex, aggregatorStates[agg]);
      fromIndex += aggregators[agg].getResultSchema().numColumns();
    }
  }

  /**
   * @return A batch's worth of result tuples from this aggregate.
   * @throws DbException if there is an error.
   */
  private TupleBatch getResultBatch() throws DbException {
    Preconditions.checkState(getChild().eos(), "cannot extract results from an aggregate until child has reached EOS");
    if (finalBuffer == null) {
      finalBuffer = resultBuffer.finalResult();
      if (resultBuffer.numTuples() == 0) {
        throw new DbException("0 tuples in result buffer");
      }
      resultBuffer = null;
    }
    if (finalBuffer.isEmpty()) {
      return null;
    } else {
      return finalBuffer.get(0);
    }
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields.
   *
   * @return the resulting schema
   */
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

    groupSchema = inputSchema.getSubSchema(gFields);

    /* Build the output schema from the group schema and the aggregates. */
    final ImmutableList.Builder<Type> aggTypes = ImmutableList.<Type> builder();
    final ImmutableList.Builder<String> aggNames = ImmutableList.<String> builder();

    try {
      for (Aggregator agg : AggUtils.allocateAggs(factories, inputSchema)) {
        Schema curAggSchema = agg.getResultSchema();
        aggTypes.addAll(curAggSchema.getColumnTypes());
        aggNames.addAll(curAggSchema.getColumnNames());
      }
    } catch (DbException e) {
      throw new RuntimeException("unable to allocate aggregators to determine output schema", e);
    }
    aggSchema = new Schema(aggTypes, aggNames);
    return Schema.merge(groupSchema, aggSchema);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkState(getSchema() != null, "unable to determine schema in init");
    aggregators = AggUtils.allocateAggs(factories, getChild().getSchema());
    aggregatorStates = AggUtils.allocateAggStates(aggregators);
    resultBuffer = new TupleBuffer(getSchema());
  }

  @Override
  protected void cleanup() throws DbException {
    aggregatorStates = null;
    curGroupKey = null;
    resultBuffer = null;
    finalBuffer = null;
  }
}
