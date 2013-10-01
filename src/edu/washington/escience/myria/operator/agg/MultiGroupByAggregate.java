package edu.washington.escience.myria.operator.agg;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.Operator;
import edu.washington.escience.myria.operator.UnaryOperator;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). We support aggregates over multiple
 * columns, group by multiple columns.
 */
public final class MultiGroupByAggregate extends UnaryOperator {

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
  /** The aggregators being used. **/
  private final Aggregator<?>[] agg;
  /** Aggregate fields. **/
  private final int[] afields;
  /** Group fields. **/
  private final int[] gfields;
  /** Aggregate operators. */
  private final int[] aggOps;

  /** The resulting buffer to return. **/
  private TupleBatchBuffer resultBuffer = null;
  /** Mapping between the group to the aggregators being used. **/
  private final HashMap<SimpleArrayWrapper, Aggregator<?>[]> groupAggs;

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
    super(child);
    Objects.requireNonNull(afields);
    Objects.requireNonNull(gfields);
    Objects.requireNonNull(aggOps);
    Preconditions.checkArgument(gfields.length > 1);
    Preconditions.checkArgument(afields.length != 0, "aggregation fields must not be empty");
    this.afields = afields;
    this.gfields = gfields;
    this.aggOps = aggOps;
    groupAggs = new HashMap<SimpleArrayWrapper, Aggregator<?>[]>();
    agg = new Aggregator<?>[aggOps.length];
  }

  /**
   * @return the aggregate field
   * */
  public int[] aggregateFields() {
    return afields;
  }

  @Override
  protected void cleanup() throws DbException {
    resultBuffer.clear();
    groupAggs.clear();
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
    if (resultBuffer.numTuples() > 0) {
      return resultBuffer.popAny();
    }

    if (child.eos() || child.eoi()) {
      return null;
    }

    // Actually perform the aggregation
    TupleBatch tb = null;
    while ((tb = child.nextReady()) != null) {
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
          Aggregator<?>[] groupAgg = new Aggregator<?>[agg.length];
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
          groupedTupleBatch.put(j, tb.getDataColumns().get(j), i);
        }
      }
      // add the tuples into the aggregator
      for (SimpleArrayWrapper saw : tmpMap.keySet()) {
        Aggregator<?>[] aggs = groupAggs.get(saw);
        TupleBatchBuffer tbb = tmpMap.get(saw);
        TupleBatch filledTb = null;
        while ((filledTb = tbb.popAny()) != null) {
          for (final Aggregator<?> aggLocal : aggs) {
            aggLocal.add(filledTb);
          }
        }
      }
    }

    if (child.eos() || child.eoi()) {
      for (SimpleArrayWrapper groupByFields : groupAggs.keySet()) {
        // populate the result tuple batch buffer
        for (int i = 0; i < gfields.length; i++) {
          resultBuffer.put(i, groupByFields.groupFields[i]);
        }
        Aggregator<?>[] value = groupAggs.get(groupByFields);
        int currentIndex = gfields.length;
        for (Aggregator<?> element : value) {
          element.getResult(resultBuffer, currentIndex);
          currentIndex += element.getResultSchema().numColumns();
        }
      }
    }
    return resultBuffer.popAny();
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields. The aggregate
   * 
   * @return the resulting schema
   */
  @Override
  public Schema generateSchema() {
    return generateSchema(getChild(), groupAggs, gfields, afields, agg, aggOps);
  }

  /**
   * @return the group fields
   */
  public int[] groupFields() {
    return Arrays.copyOf(gfields, gfields.length);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    resultBuffer = new TupleBatchBuffer(getSchema());
  }

  /**
   * Generates the schema for MultiGroupByAggregate.
   * 
   * @param child the child operator
   * @param groupAggs the aggregator for each grouping
   * @param gfields the group fields
   * @param afields the aggregae fields
   * @param agg the aggregators to be populated
   * @param aggOps ints representing the aggregate operations
   * 
   * @return the schema based on the aggregate, if the child is null, will return null.
   */
  private static Schema generateSchema(final Operator child, final Map<SimpleArrayWrapper, Aggregator<?>[]> groupAggs,
      final int[] gfields, final int[] afields, final Aggregator<?>[] agg, final int[] aggOps) {
    if (child == null) {
      return null;
    }
    Schema outputSchema = null;

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    final Schema childSchema = child.getSchema();
    for (final int i : gfields) {
      gTypes.add(childSchema.getColumnType(i));
      gNames.add(childSchema.getColumnName(i));
    }

    // Generates the output schema
    outputSchema = new Schema(gTypes, gNames);

    int idx = 0;
    for (final int afield : afields) {
      switch (childSchema.getColumnType(afield)) {
        case BOOLEAN_TYPE:
          agg[idx] = new BooleanAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case INT_TYPE:
          agg[idx] = new IntegerAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case LONG_TYPE:
          agg[idx] = new LongAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case FLOAT_TYPE:
          agg[idx] = new FloatAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case DOUBLE_TYPE:
          agg[idx] = new DoubleAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case STRING_TYPE:
          agg[idx] = new StringAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        case DATETIME_TYPE:
          agg[idx] = new DateTimeAggregator(afield, childSchema.getColumnName(afield), aggOps[idx]);
          break;
        default:
          throw new IllegalArgumentException("unsupported type: " + childSchema.getColumnType(afield));
      }
      outputSchema = Schema.merge(outputSchema, agg[idx].getResultSchema());
      idx++;
    }
    return outputSchema;
  }
}
