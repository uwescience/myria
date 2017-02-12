package edu.washington.escience.myria.operator.agg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.gs.collections.api.iterator.IntIterator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.GenericEvaluator;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.TupleHashTable;
import edu.washington.escience.myria.operator.UnaryOperator;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). This variant supports aggregates over
 * multiple columns, group by multiple columns.
 */
public class Aggregate extends UnaryOperator {

  /** Java requires this. **/
  private static final long serialVersionUID = 1L;

  /** The hash table containing groups and states. */
  protected transient TupleHashTable groupStates;
  /** Factories to make the Aggregators. **/
  private final AggregatorFactory[] factories;
  /** Aggregators of the internal state. */
  protected List<Aggregator> internalAggs;
  /** Expressions that emit output. */
  protected List<GenericEvaluator> emitEvals;
  /** Group fields. Empty array means no grouping. **/
  protected final int[] gfields;
  /** Buffer for restoring results. */
  protected TupleBatchBuffer resultBuffer;

  /**
   * Groups the input tuples according to the specified grouping fields, then produces the specified aggregates.
   *
   * @param child The Operator that is feeding us tuples.
   * @param gfields The columns over which we are grouping the result. Null means no group by.
   * @param factories The factories that will produce the {@link Aggregator}s;
   */
  public Aggregate(
      @Nullable final Operator child, final int[] gfields, final AggregatorFactory... factories) {
    super(child);
    this.gfields = gfields;
    this.factories = Objects.requireNonNull(factories, "factories");
  }

  @Override
  protected void cleanup() throws DbException {
    groupStates.cleanup();
    resultBuffer.clear();
  }

  /**
   * Returns the next tuple. The first few columns are group-by fields if there are any, followed by columns of
   * aggregate results generated by {@link Aggregate#emitEvals}.
   *
   * @throws DbException if any error occurs.
   * @return result TB.
   */
  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator child = getChild();
    TupleBatch tb = child.nextReady();
    while (tb != null) {
      for (int row = 0; row < tb.numTuples(); ++row) {
        IntIterator iter = groupStates.getIndices(tb, gfields, row).intIterator();
        int index;
        if (!iter.hasNext()) {
          groupStates.addTuple(tb, gfields, row, true);
          int offset = gfields.length;
          for (Aggregator agg : internalAggs) {
            agg.initState(groupStates.getData(), offset);
            offset += agg.getStateSize();
          }
          index = groupStates.getData().numTuples() - 1;
        } else {
          index = iter.next();
        }
        int offset = gfields.length;
        for (Aggregator agg : internalAggs) {
          agg.addRow(tb, row, groupStates.getData(), index, offset);
          offset += agg.getStateSize();
        }
      }
      tb = child.nextReady();
    }
    if (child.eos()) {
      generateResult();
      return resultBuffer.popAny();
    }
    return null;
  }

  /**
   * @return A batch's worth of result tuples from this aggregate.
   * @throws DbException if there is an error.
   */
  protected void generateResult() throws DbException {
    Schema inputSchema = getChild().getSchema();
    for (TupleBatch tb : groupStates.getData().getAll()) {
      List<Column<?>> columns = new ArrayList<Column<?>>();
      columns.addAll(tb.getDataColumns().subList(0, gfields.length));
      int stateOffset = gfields.length;
      int emitOffset = 0;
      for (AggregatorFactory factory : factories) {
        int stateSize = factory.generateStateSchema(inputSchema).numColumns();
        int emitSize = factory.generateSchema(inputSchema).numColumns();
        TupleBatch state =
            tb.selectColumns(MyriaArrayUtils.range(stateOffset, stateOffset + stateSize));
        for (GenericEvaluator eval : emitEvals.subList(emitOffset, emitOffset + emitSize)) {
          columns.addAll(eval.evaluateColumn(state).getResultColumns());
        }
        stateOffset += stateSize;
        emitOffset += emitSize;
      }
      if (this instanceof StreamingAggregate) {
        resultBuffer.absorb(new TupleBatch(getSchema(), columns), false);
      } else {
        resultBuffer.absorb(new TupleBatch(getSchema(), columns), true);
      }
    }
    groupStates.cleanup();
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields. The aggregate
   *
   * @return the resulting schema
   */
  @Override
  protected Schema generateSchema() {
    if (getChild() == null) {
      return null;
    }
    Schema inputSchema = getChild().getSchema();
    Schema aggSchema = Schema.EMPTY_SCHEMA;
    for (int i = 0; i < factories.length; ++i) {
      aggSchema = Schema.merge(aggSchema, factories[i].generateSchema(inputSchema));
    }
    return Schema.merge(inputSchema.getSubSchema(gfields), aggSchema);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Schema inputSchema = getChild().getSchema();
    Preconditions.checkState(inputSchema != null, "unable to determine schema in init");
    internalAggs = new ArrayList<Aggregator>();
    emitEvals = new ArrayList<GenericEvaluator>();
    Schema groupingSchema = inputSchema.getSubSchema(gfields);
    Schema stateSchema = Schema.EMPTY_SCHEMA;
    for (int i = 0; i < factories.length; ++i) {
      internalAggs.addAll(factories[i].generateInternalAggs(inputSchema));
      List<Expression> emits = factories[i].generateEmitExpressions(inputSchema);
      Schema newStateSchema = factories[i].generateStateSchema(inputSchema);
      stateSchema = Schema.merge(stateSchema, newStateSchema);
      for (Expression exp : emits) {
        GenericEvaluator evaluator =
            new GenericEvaluator(
                exp, new ExpressionOperatorParameter(newStateSchema, newStateSchema));
        // if (evaluator.needsCompiling()) {
        evaluator.compile();
        // }
        emitEvals.add(evaluator);
      }
    }
    groupStates =
        new TupleHashTable(
            Schema.merge(groupingSchema, stateSchema), MyriaArrayUtils.range(0, gfields.length));
    resultBuffer = new TupleBatchBuffer(getSchema());
  }
};
