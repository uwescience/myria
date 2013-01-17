package edu.washington.escience.myriad.operator.agg;

import java.util.Objects;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;

/**
 * The Aggregation operator that computes an aggregate.
 * 
 * This class does not do group by.
 */
public final class Aggregate extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The schema of the tuples returned by this operator. */
  private final Schema schema;
  /** The source of tuples to be aggregated. */
  private Operator child;
  /** Does the actual aggregation work. */
  private final Aggregator[] agg;
  /** Which fields the aggregate is computed over. */
  private final int[] afields;

  /**
   * Constructor.
   * 
   * Implementation hint: depending on the type of afield, you will want to construct an {@link IntAggregator} or
   * {@link StringAggregator} to help you with your implementation of readNext().
   * 
   * 
   * @param child The Operator that is feeding us tuples.
   * @param afields The columns over which we are computing an aggregate.
   * @param aggOps The aggregation operator to use
   */
  public Aggregate(final Operator child, final int[] afields, final int[] aggOps) {
    Objects.requireNonNull(afields);
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }

    final ImmutableList.Builder<Type> gTypes = ImmutableList.builder();
    final ImmutableList.Builder<String> gNames = ImmutableList.builder();

    final Schema childSchema = child.getSchema();

    this.child = child;
    this.afields = afields;
    agg = new Aggregator[aggOps.length];

    int idx = 0;
    for (final int afield : afields) {
      switch (childSchema.getFieldType(afield)) {
        case BOOLEAN_TYPE:
          agg[idx] = new BooleanAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          break;
        case INT_TYPE:
          agg[idx] = new IntegerAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          break;
        case LONG_TYPE:
          agg[idx] = new LongAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          break;
        case FLOAT_TYPE:
          agg[idx] = new FloatAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          break;
        case DOUBLE_TYPE:
          agg[idx] = new DoubleAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          break;
        case STRING_TYPE:
          agg[idx] = new StringAggregator(afield, childSchema.getFieldName(afield), aggOps[idx]);
          break;
      }
      gTypes.addAll(agg[idx].getResultSchema().getTypes());
      gNames.addAll(agg[idx].getResultSchema().getFieldNames());
      idx++;
    }
    schema = new Schema(gTypes, gNames);
  }

  /**
   * @return the aggregate field
   * */
  public int[] aggregateFields() {
    return afields;
  }

  @Override
  protected void cleanup() throws DbException {
    for (int i = 0; i < agg.length; i++) {
      agg[i] = agg[i].freshCopyYourself();
    }
  }

  @Override
  protected TupleBatch fetchNext() throws DbException {

    TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      for (final Aggregator ag : agg) {
        ag.add(tb);
      }
    }
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    int fromIndex = 0;
    for (final Aggregator element : agg) {
      element.getResult(tbb, fromIndex);
      fromIndex += element.getResultSchema().numFields();
    }
    final TupleBatch result = tbb.popAny();
    setEOS();
    return result;
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

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  protected void init() throws DbException {
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

}
