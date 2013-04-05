package edu.washington.escience.myriad.operator.agg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public final class MultiGroupByAggregate extends Operator {

  /**
   * A simple implementation of multiple-field group key.
   * */
  private static class SimpleArrayWrapper {
    /** the group fields. **/
    private final Object[] groupFields;

    /**
     * Constructs a new group representation.
     * 
     * @param groupFields the group fields
     */
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
    public String toString() {
      return Arrays.toString(groupFields);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(groupFields);
    }
  }

  /** Java requires this. **/
  private static final long serialVersionUID = 1L;
  /** The schema after the aggregate is done. **/
  private final Schema schema;
  /** The child operator that will feed tuples in. **/
  private Operator child;
  /** The aggregators being used. **/
  private final Aggregator[] agg;
  /** Aggregate fields. **/
  private final int[] afields;
  /** Group fields. **/
  private int[] gfields;
  /** Should the aggregator do group by or not. **/
  private final boolean groupBy;
  /** The resulting buffer to return. **/
  private TupleBatchBuffer resultBuffer = null;
  /** Mapping between the group to the aggregators being used. **/
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
  public MultiGroupByAggregate(final Operator child, final int[] afields, final int[] gfields, final int[] aggOps) {
    Objects.requireNonNull(afields);
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }

    Schema outputSchema = null;
    if (gfields == null || gfields.length == 0) {
      this.gfields = new int[0];
      groupBy = false;
      groupAggs = null;
    } else {
      this.gfields = gfields;
      groupBy = true;
      groupAggs = new HashMap<SimpleArrayWrapper, Aggregator[]>();
    }

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    final Schema childSchema = child.getSchema();
    for (final int i : this.gfields) {
      gTypes.add(childSchema.getColumnType(i));
      gNames.add(childSchema.getColumnName(i));
    }

    // Generates the output schema
    outputSchema = new Schema(gTypes, gNames);

    this.child = child;
    this.afields = afields;
    agg = new Aggregator[aggOps.length];

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
  public int[] aggregateFields() {
    return afields;
  }

  @Override
  protected void cleanup() throws DbException {
    groupAggs.clear();
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {
    resultBuffer = new TupleBatchBuffer(schema);
    if (resultBuffer.numTuples() == 0) {
      // Actually perform the aggregation
      TupleBatch tb = null;
      while ((tb = child.next()) != null) {
        if (!groupBy) {
          for (final Aggregator ag : agg) {
            ag.add(tb);
          }
        } else {
          // get all the tuple batches from the child operator
          // we want to get the value for each key.
          HashMap<SimpleArrayWrapper, TupleBatchBuffer> tmpMap = new HashMap<SimpleArrayWrapper, TupleBatchBuffer>();
          for (int i = 0; i < tb.numTuples(); i++) {
            // generate the SimpleArrayWrapper
            Object[] groupFields = new Object[gfields.length];
            for (int j = 0; j < gfields.length; j++) {
              Object val = tb.getObject(gfields[j], i);
              groupFields[j] = val;
            }
            SimpleArrayWrapper grpFields = new SimpleArrayWrapper(groupFields);

            // for each tuple try pulling a value from it
            if (!groupAggs.containsKey(grpFields)) {
              // if the aggregator for the key doesn't exists,
              // create a new array of operators and put it in the map
              Aggregator[] groupAgg = new Aggregator[agg.length];
              for (int j = 0; j < groupAgg.length; j++) {
                groupAgg[j] = agg[j].freshCopyYourself();
              }
              groupAggs.put(grpFields, groupAgg);
            }

            // foreach row, we need to put the tuples into its corresponding
            // group
            TupleBatchBuffer groupedTupleBatch = tmpMap.get(grpFields);
            if (groupedTupleBatch == null) {
              groupedTupleBatch = new TupleBatchBuffer(child.getSchema());
              tmpMap.put(grpFields, groupedTupleBatch);
            }
            for (int j = 0; j < child.getSchema().numColumns(); j++) {
              groupedTupleBatch.put(j, tb.getObject(j, i));
            }
          }
          // add the tuples into the aggregator
          for (SimpleArrayWrapper saw : tmpMap.keySet()) {
            Aggregator[] aggs = groupAggs.get(saw);
            TupleBatchBuffer tbb = tmpMap.get(saw);
            TupleBatch filledTb = null;
            while ((filledTb = tbb.popAny()) != null) {
              for (Aggregator aggregator : aggs) {
                aggregator.add(filledTb);
              }
            }
          }
        }
      }
      for (SimpleArrayWrapper groupByFields : groupAggs.keySet()) {
        // populate the result tuple batch buffer
        for (int i = 0; i < gfields.length; i++) {
          resultBuffer.put(i, groupByFields.groupFields[i]);
        }
        Aggregator[] value = groupAggs.get(groupByFields);
        for (int i = gfields.length; i < schema.numColumns(); i++) {
          value[i - gfields.length].getResult(resultBuffer, i);
        }
      }
    }
    return resultBuffer.popAny();
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
   * 
   * @return the resulting schema
   */
  @Override
  public Schema getSchema() {
    return schema;
  }

  /**
   * @return the group fields
   */
  public int[] groupFields() {
    return Arrays.copyOf(gfields, gfields.length);
  }

  @Override
  protected void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

}
