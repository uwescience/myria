package edu.washington.escience.myriad.operator.agg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public class MultiGroupByAggregate_NotYetDone extends Operator {

  /**
   * A simple implementation of multiple-field group key
   * */
  protected static class SimpleArrayWrapper {
    public final Object[] groupFields;

    public SimpleArrayWrapper(final Object[] groupFields) {
      this.groupFields = groupFields;
    }

    @Override
    public boolean equals(final Object another) {
      if (another == null || !(another instanceof SimpleArrayWrapper)) {
        return false;
      }
      return Arrays.equals(groupFields, ((SimpleArrayWrapper) another).groupFields);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(groupFields);
    }
  }

  private static final long serialVersionUID = 1L;
  private final Schema schema;
  private Operator child;
  private final Aggregator[] agg;
  private final int[] afields; // Compute aggregate on each of the afields
  private final int[] gfields; // group by fields
  private final boolean groupBy;

  private final HashMap<SimpleArrayWrapper, Aggregator[]> groupAggs;

  /**
   * Constructor.
   * 
   * Implementation hint: depending on the type of afield, you will want to construct an {@link IntAggregator} or
   * {@link StringAggregator} to help you with your implementation of readNext().
   * 
   * 
   * @param child The Operator that is feeding us tuples.
   * @param afields The columns over which we are computing an aggregate.
   * @param gfields The columns over which we are grouping the result, or -1 if there is no grouping
   * @param aggOps The aggregation operator to use
   */
  public MultiGroupByAggregate_NotYetDone(final Operator child, final int[] afields, int[] gfields, final int[] aggOps) {
    Objects.requireNonNull(afields);
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }

    Schema outputSchema = null;
    if (gfields == null) {
      gfields = new int[0];
      groupBy = false;
      groupAggs = null;
    } else if (gfields.length == 0) {
      groupBy = false;
      groupAggs = null;
    } else {
      groupBy = true;
      groupAggs = new HashMap<SimpleArrayWrapper, Aggregator[]>();
    }

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    final Schema childSchema = child.getSchema();
    for (final int i : gfields) {
      gTypes.add(childSchema.getFieldType(i));
      gNames.add(childSchema.getFieldName(i));
    }

    outputSchema = new Schema(gTypes, gNames);

    this.child = child;
    this.afields = afields;
    this.gfields = gfields;
    agg = new Aggregator[aggOps.length];

    int idx = 0;
    for (final int afield : afields) {
      switch (childSchema.getFieldType(afield)) {
        case BOOLEAN_TYPE:
          agg[idx] = new BooleanAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case INT_TYPE:
          agg[idx] = new IntegerAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case LONG_TYPE:
          agg[idx] = new LongAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case FLOAT_TYPE:
          agg[idx] = new FloatAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case DOUBLE_TYPE:
          agg[idx] = new DoubleAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
          break;
        case STRING_TYPE:
          agg[idx] = new StringAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
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
  public int[] aggregateFields() {
    return afields;
  }

  @Override
  protected void cleanup() throws DbException {
    groupAggs.clear();
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
   */
  @Override
  protected TupleBatch fetchNext() throws DbException {

    // Actually perform the aggregation
    TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      if (!groupBy) {
        for (final Aggregator ag : agg) {
          ag.add(tb);
        }
      } else {
        // group by

      }
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    // TODO non-blocking
    return fetchNext();
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields. The aggregate
   */
  @Override
  public Schema getSchema() {
    return schema;
  }

  public int[] groupFields() {
    return gfields;
  }

  @Override
  protected void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

}
