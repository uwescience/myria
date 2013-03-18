package edu.washington.escience.myriad.operator.agg;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.operator.Operator;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min) with a single group by column.
 */
public class SingleGroupByAggregate extends Operator {

  private static final long serialVersionUID = 1L;

  private final Schema schema;
  private Operator child;
  private final Aggregator[] agg;
  private final int[] afields; // Compute aggregate on each of the afields
  private final int gfield; // group by fields
  private final HashMap<Object, Aggregator[]> groupAggs;
  private final Map<Object, Pair<Object, TupleBatchBuffer>> groupedTupleBatches;
  private transient TupleBatchBuffer resultBuffer;

  /**
   * Constructor.
   * 
   * @param child The Operator that is feeding us tuples.
   * @param afields The columns over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result
   * @param aggOps The aggregation operator to use
   */
  public SingleGroupByAggregate(final Operator child, final int[] afields, final int gfield, final int[] aggOps) {
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
    this.gfield = gfield;
    agg = new Aggregator[aggOps.length];
    groupAggs = new HashMap<Object, Aggregator[]>();
    groupedTupleBatches = new HashMap<Object, Pair<Object, TupleBatchBuffer>>();

    int idx = 0;
    for (final int afield : afields) {
      switch (childSchema.getColumnType(afield)) {
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
    groupAggs.clear();
    resultBuffer = null;
  }

  private void processTupleBatch(TupleBatch tb) {
    final Set<Pair<Object, TupleBatchBuffer>> readyTBB = tb.groupby(gfield, groupedTupleBatches);
    if (readyTBB != null) {
      for (final Pair<Object, TupleBatchBuffer> p : readyTBB) {
        final Object groupColumnValue = p.getKey();
        final TupleBatchBuffer tbb = p.getRight();
        Aggregator[] groupAgg = groupAggs.get(groupColumnValue);
        if (groupAgg == null) {
          groupAgg = new Aggregator[agg.length];
          for (int j = 0; j < agg.length; j++) {
            groupAgg[j] = agg[j].freshCopyYourself();
          }
          groupAggs.put(groupColumnValue, groupAgg);
        }

        TupleBatch filledTB = null;
        while ((filledTB = tbb.popFilled()) != null) {
          for (final Aggregator ag : groupAgg) {
            ag.add(filledTB);
          }
        }
      }
    }
  }

  private void processEnd() {
    for (final Pair<Object, TupleBatchBuffer> p : groupedTupleBatches.values()) {
      final Object groupColumnValue = p.getKey();
      Aggregator[] groupAgg = groupAggs.get(groupColumnValue);
      if (groupAgg == null) {
        groupAgg = new Aggregator[agg.length];
        for (int j = 0; j < agg.length; j++) {
          groupAgg[j] = agg[j].freshCopyYourself();
        }
        groupAggs.put(groupColumnValue, groupAgg);
      }
      final TupleBatchBuffer tbb = p.getRight();
      TupleBatch anyTBB = null;
      while ((anyTBB = tbb.popAny()) != null) {
        for (final Aggregator ag : groupAgg) {
          ag.add(anyTBB);
        }
      }
    }
  }

  private void generateResult(final TupleBatchBuffer resultBuffer) {

    for (final Map.Entry<Object, Aggregator[]> e : groupAggs.entrySet()) {
      final Object groupByValue = e.getKey();
      final Aggregator[] agg = e.getValue();
      resultBuffer.put(0, groupByValue);
      int fromIndex = 1;
      for (final Aggregator element : agg) {
        element.getResult(resultBuffer, fromIndex);
        fromIndex += element.getResultSchema().numColumns();
      }
    }
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
   */
  @Override
  protected final TupleBatch fetchNext() throws DbException, InterruptedException {
    if (child.eos()) {
      return null;
    }
    TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      processTupleBatch(tb);
    }
    processEnd();
    TupleBatchBuffer resultBuffer = new TupleBatchBuffer(schema);
    generateResult(resultBuffer);
    // setEOS();
    return resultBuffer.popAny();
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb = null;

    if (child.eos()) {
      return resultBuffer.popAny();
    }

    while ((tb = child.nextReady()) != null) {
      processTupleBatch(tb);
    }

    if (child.eos()) {
      processEnd();
      resultBuffer = new TupleBatchBuffer(schema);
      generateResult(resultBuffer);
      // setEOS();
      return resultBuffer.popAny();
    }
    return null;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public final Schema getSchema() {
    return schema;
  }

  public final int groupByField() {
    return gfield;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    resultBuffer = new TupleBatchBuffer(schema);
  }

  @Override
  public final void setChildren(final Operator[] children) {
    child = children[0];
  }

}
