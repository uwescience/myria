package edu.washington.escience.myriad.operator.agg;

import java.util.Objects;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Operator;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

  private static final long serialVersionUID = 1L;

  private final Schema schema;
  private Operator child;
  private final Aggregator[] agg;
  private final int[] afields; // Compute aggregate on each of the afields
  private final int[] gfields; // group by fields

  /**
   * Constructor.
   * 
   * Implementation hint: depending on the type of afield, you will want to construct an {@link IntAggregator} or
   * {@link StringAggregator} to help you with your implementation of readNext().
   * 
   * 
   * @param child The DbIterator that is feeding us tuples.
   * @param afields The columns over which we are computing an aggregate.
   * @param gfields The columns over which we are grouping the result, or -1 if there is no grouping
   * @param aggOps The aggregation operator to use
   */
  public Aggregate(Operator child, int[] afields, int[] gfields, int[] aggOps) {
    Objects.requireNonNull(afields);
    if (afields.length == 0) {
      throw new IllegalArgumentException("aggregation fields must not be empty");
    }

    Schema outputSchema = null;
    if (gfields == null) {
      gfields = new int[0];
    }

    Type[] gTypes = new Type[gfields.length];
    String[] gNames = new String[gfields.length];

    Schema childSchema = child.getSchema();
    for (int i = 0; i < gfields.length; i++) {
      gTypes[i] = childSchema.getFieldType(gfields[i]);
      gNames[i] = childSchema.getFieldName(gfields[i]);
    }

    outputSchema = new Schema(gTypes, gNames);

    this.child = child;
    this.afields = afields;
    this.gfields = gfields;
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

  public int[] groupFields() {
    return gfields;
  }

  /**
   * @return the aggregate field
   * */
  public int[] aggregateFields() {
    return afields;
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
   */
  @Override
  protected _TupleBatch fetchNext() throws DbException {

    // Actually perform the aggregation
    _TupleBatch tb = null;
    while ((tb = child.next()) != null) {
      for (Aggregator ag : agg) {
        ag.add(tb);
      }
    }
    return null;
  }

  /**
   * The schema of the aggregate output. Grouping fields first and then aggregate fields. The aggregate
   */
  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child };
  }

  @Override
  public void setChildren(Operator[] children) {
    child = children[0];
  }

  @Override
  protected void init() throws DbException {
    // TODO Auto-generated method stub

  }

  @Override
  protected void cleanup() throws DbException {
    // TODO Auto-generated method stub

  }

  @Override
  public _TupleBatch fetchNextReady() throws DbException {
    // TODO Auto-generated method stub
    return null;
  }

}
