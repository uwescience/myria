package edu.washington.escience.myriad.operator.agg;

import java.util.Objects;

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

    Schema outputSchema = null;

    Type[] gTypes = new Type[0];
    String[] gNames = new String[0];

    Schema childSchema = child.getSchema();

    outputSchema = new Schema(gTypes, gNames);

    this.child = child;
    this.afields = afields;
    agg = new Aggregator[aggOps.length];

    int idx = 0;
    for (int afield : afields) {
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
  protected TupleBatch fetchNext() throws DbException {

    TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      for (Aggregator ag : agg) {
        ag.add(tb);
      }
    }
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    int fromIndex = 0;
    for (Aggregator element : agg) {
      element.getResult(tbb, fromIndex);
      fromIndex += element.getResultSchema().numFields();
    }
    TupleBatch result = tbb.popAny();
    setEOS();
    return result;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public void setChildren(final Operator[] children) {
    child = children[0];
  }

  @Override
  protected void init() throws DbException {
  }

  @Override
  protected void cleanup() throws DbException {
    for (int i = 0; i < agg.length; i++) {
      agg[i] = agg[i].freshCopyYourself();
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    // TODO non-blocking
    return fetchNext();
  }

}
