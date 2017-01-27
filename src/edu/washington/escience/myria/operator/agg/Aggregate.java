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
  /** Aggregators that emit output. */
  protected List<Aggregator> emitAggs;
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
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
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
        int indice;
        if (!iter.hasNext()) {
          groupStates.addTuple(tb, gfields, row, true);
          for (int j = 0; j < internalAggs.size(); ++j) {
            internalAggs.get(j).initState(groupStates.getData());
          }
          indice = groupStates.getData().numTuples() - 1;
        } else {
          indice = iter.next();
        }
        for (int j = 0; j < internalAggs.size(); ++j) {
          internalAggs.get(j).addRow(tb, row, groupStates.getData(), indice);
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
  private void generateResult() throws DbException {
    for (TupleBatch tb : groupStates.getData().getAll()) {
      List<Column<?>> columns = new ArrayList<Column<?>>();
      for (int i = 0; i < gfields.length; ++i) {
        columns.add(tb.getDataColumns().get(i));
      }
      for (Aggregator agg : emitAggs) {
        columns.addAll(agg.emitOutput(tb));
      }
      resultBuffer.absorb(new TupleBatch(getSchema(), columns), true);
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
    Schema schema = getChild().getSchema().getSubSchema(gfields);
    for (Aggregator agg : emitAggs) {
      schema = Schema.merge(schema, agg.getOutputSchema());
    }
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Schema inputSchema = getChild().getSchema();
    Preconditions.checkState(inputSchema != null, "unable to determine schema in init");
    Schema stateSchema = inputSchema.getSubSchema(gfields);
    internalAggs = new ArrayList<Aggregator>();
    emitAggs = new ArrayList<Aggregator>();
    for (int i = 0; i < factories.length; ++i) {
      internalAggs.addAll(factories[i].generateInternalAggs(inputSchema, stateSchema.numColumns()));
      emitAggs.addAll(factories[i].generateEmitAggs(inputSchema, stateSchema.numColumns()));
      Schema newSchema = factories[i].generateStateSchema(inputSchema);
      stateSchema = Schema.merge(stateSchema, newSchema);
    }
    groupStates = new TupleHashTable(stateSchema, MyriaArrayUtils.range(0, gfields.length));
    resultBuffer = new TupleBatchBuffer(getSchema());
  }
};
