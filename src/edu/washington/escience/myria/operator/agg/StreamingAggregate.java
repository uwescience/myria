package edu.washington.escience.myria.operator.agg;

import java.io.IOException;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.Tuple;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
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

  /** The schema of the result. */
  private Schema resultSchema;
  /** The schema of the columns indicated by the group keys. */
  private Schema groupSchema;

  /** Holds the current grouping key. */
  private Tuple curGroupKey;
  /** Child of this aggregator. **/
  private final Operator child = getChild();
  /** Currently processing input tuple batch. **/
  private TupleBatch tb;
  /** Current row in the input tuple batch. **/
  private int row;

  /** Group fields. **/
  private final int[] gFields;
  /** An array [0, 1, .., gFields.length-1] used for comparing tuples. */
  private final int[] gRange;
  /** Factories to make the Aggregators. **/
  private final AggregatorFactory[] factories;
  /** The actual Aggregators. **/
  private Aggregator[] aggregators;
  /** The state of the aggregators. */
  private Object[] aggregatorStates;

  /** Buffer for holding intermediate results. */
  private transient TupleBatchBuffer resultBuffer;

  /**
   * Groups the input tuples according to the specified grouping fields, then produces the specified aggregates.
   *
   * @param child The Operator that is feeding us tuples.
   * @param gfields The columns over which we are grouping the result.
   * @param factories The factories that will produce the {@link Aggregator}s for each group.
   */
  public StreamingAggregate(@Nullable final Operator child, @Nonnull final int[] gfields,
      @Nonnull final AggregatorFactory... factories) {
    super(child);
    gFields = Objects.requireNonNull(gfields, "gfields");
    this.factories = Objects.requireNonNull(factories, "factories");
    Preconditions.checkArgument(gfields.length > 0, " must have at least one group by field");
    Preconditions.checkArgument(factories.length > 0, "to use StreamingAggregate, must specify some aggregates");
    gRange = new int[gfields.length];
    for (int i = 0; i < gRange.length; ++i) {
      gRange[i] = i;
    }
  }

  /**
   * Returns the next tuple batch containing the result of this aggregate. Grouping field(s) followed by aggregate
   * field(s).
   *
   * @throws DbException if any error occurs.
   * @return result tuple batch
   * @throws IOException
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException, IOException {
    if (child.eos()) {
      return null;
    }
    if (tb == null) {
      tb = child.nextReady();
      row = 0;
    }
    while (tb != null) {
      while (row < tb.numTuples()) {
        if (curGroupKey == null) {
          /* First time accessing this tb, no aggregation performed previously. */
          // store current group key as a tuple
          curGroupKey = new Tuple(groupSchema);
          for (int gKey = 0; gKey < gFields.length; ++gKey) {
            TupleUtils.copyValue(tb, gFields[gKey], row, curGroupKey, gKey);
          }
        } else if (!TupleUtils.tupleEquals(tb, gFields, row, curGroupKey, gRange, 0)) {
          /* Different grouping key than current one, flush current agg result to result buffer. */
          addToResult();
          // store current group key as a tuple
          for (int gKey = 0; gKey < gFields.length; ++gKey) {
            TupleUtils.copyValue(tb, gFields[gKey], row, curGroupKey, gKey);
          }
          aggregatorStates = AggUtils.allocateAggStates(aggregators);
          if (resultBuffer.hasFilledTB()) {
            return resultBuffer.popFilled();
          }
        }
        // update aggregator states with current tuple
        for (int agg = 0; agg < aggregators.length; ++agg) {
          aggregators[agg].addRow(tb, row, aggregatorStates[agg]);
        }
        row++;
      }
      tb = child.nextReady();
      row = 0;
    }

    /*
     * We know that child.nextReady() has returned <code>null</code>, so we have processed all tuple we can. Child is
     * either EOS or we have to wait for more data.
     */
    if (child.eos()) {
      addToResult();
      return resultBuffer.popAny();
    }
    return null;
  }

  /**
   * Add aggregate results with previous grouping key to result buffer.
   *
   * @throws DbException if any error
   * @throws IOException
   */
  private void addToResult() throws DbException, IOException {
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
    resultSchema = Schema.of(groupSchema.getColumnTypes(), groupSchema.getColumnNames());
    try {
      for (Aggregator agg : AggUtils.allocateAggs(factories, inputSchema, getPythonFunctionRegistrar())) {
        Schema curAggSchema = agg.getResultSchema();
        resultSchema = Schema.merge(resultSchema, curAggSchema);
      }
    } catch (DbException e) {
      throw new RuntimeException("unable to allocate aggregators to determine output schema", e);
    }
    return resultSchema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkState(getSchema() != null, "unable to determine schema in init");
    aggregators = AggUtils.allocateAggs(factories, getChild().getSchema(), getPythonFunctionRegistrar());
    aggregatorStates = AggUtils.allocateAggStates(aggregators);
    resultBuffer = new TupleBatchBuffer(getSchema());
  }

  @Override
  protected void cleanup() throws DbException {
    aggregatorStates = null;
    curGroupKey = null;
    resultBuffer = null;
  }
}
