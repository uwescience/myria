package edu.washington.escience.myriad.operator;

import java.util.NoSuchElementException;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.Aggregator.AggOp;
import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max, min). Note that we only support aggregates
 * over a single column, grouped by a single column.
 */
public class Aggregate extends Operator {

  private static final long serialVersionUID = 1L;

  private Schema schema = null;
  private Operator child = null;
  private DbIterator it = null;
  private Aggregator agg = null;
  private Aggregator.AggOp aop = null;
  private String gFieldName = null;
  private String aFieldName = null;
  private int afield, gfield;

  /**
   * Constructor.
   * 
   * Implementation hint: depending on the type of afield, you will want to construct an {@link IntAggregator} or
   * {@link StringAggregator} to help you with your implementation of readNext().
   * 
   * 
   * @param child The DbIterator that is feeding us tuples.
   * @param afield The column over which we are computing an aggregate.
   * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
   * @param aop The aggregation operator to use
   */
  public Aggregate(Operator child, int afield, int gfield, Aggregator.AggOp aop) {
    this.child = child;
    this.afield = afield;
    this.gfield = gfield;
    this.aop = aop;
    if (this.aop == AggOp.SC_AVG)
      if (this.gfield == Aggregator.NO_GROUPING)
        this.afield = 0;
      else {
        this.afield = 1;
        this.gfield = 0;
      }
    Schema child_td = child.getSchema();

    if (child_td.getFieldType(afield) == Type.INT_TYPE) {
      this.agg =
          new IntegerAggregator(gfield, gfield == Aggregator.NO_GROUPING ? null : child_td.getFieldType(gfield),
              afield, aop);
    } else {
      this.agg =
          new StringAggregator(gfield, gfield == Aggregator.NO_GROUPING ? null : child_td.getFieldType(gfield), afield,
              aop);
    }

    if (gfield == Aggregator.NO_GROUPING) {
      int nFields = 1;
      if (aop == AggOp.SUM_COUNT)
        nFields = 2;
      Type typeAr[] = new Type[nFields];
      String fields[] = new String[nFields];

      typeAr[0] = Type.INT_TYPE;// child_td.getFieldType(afield);

      if (aop == AggOp.SUM_COUNT) {
        typeAr[1] = Type.INT_TYPE;
        fields[1] = "";
      }

      fields[0] = nameOfAggregatorOp(aop) + "(" + child_td.getFieldName(afield) + ")";
      aFieldName = fields[0];

      schema = new Schema(typeAr, fields);
    } else {
      int nFields = 2;
      if (aop == AggOp.SUM_COUNT)
        nFields = 3;

      Type typeAr[] = new Type[nFields];
      String fields[] = new String[nFields];

      typeAr[0] = child_td.getFieldType(gfield);
      typeAr[1] = Type.INT_TYPE;// child_td.getFieldType(afield);
      fields[0] = child_td.getFieldName(gfield);
      gFieldName = fields[0];

      fields[1] = nameOfAggregatorOp(aop) + "(" + child_td.getFieldName(afield) + ")";

      if (aop == AggOp.SUM_COUNT) {
        typeAr[2] = Type.INT_TYPE;
        fields[2] = "";
      }

      aFieldName = fields[1];

      schema = new Schema(typeAr, fields);
    }
  }

  /**
   * @return If this aggregate is accompanied by a groupby, return the groupby field index in the <b>INPUT</b> tuples.
   *         If not, return {@link simpledb.Aggregator#NO_GROUPING}
   * */
  public int groupField() {
    return this.gfield;
  }

  /**
   * @return If this aggregate is accompanied by a group by, return the name of the groupby field in the <b>OUTPUT</b>
   *         tuples If not, return null;
   * */
  public String groupFieldName() {
    return this.gFieldName;
  }

  /**
   * @return the aggregate field
   * */
  public int aggregateField() {
    return this.afield;
  }

  /**
   * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
   * */
  public String aggregateFieldName() {
    return this.aFieldName;
  }

  /**
   * @return return the aggregate operator
   * */
  public Aggregator.AggOp aggregateOp() {
    return this.aop;
  }

  public static String nameOfAggregatorOp(Aggregator.AggOp aop) {
    return aop.toString();
  }

  @Override
  public void open() throws NoSuchElementException, DbException {
    child.open();
    if (it != null)
      it.open();
    super.open();
  }

  /**
   * Returns the next tuple. If there is a group by field, then the first field is the field by which we are grouping,
   * and the second field is the result of computing the aggregate, If there is no group by field, then the result tuple
   * should contain one field representing the result of the aggregate. Should return null if there are no more tuples.
   */
  @Override
  protected _TupleBatch fetchNext() throws DbException {
    // Actually perform the aggregation
    if (it == null) {
      while (child.hasNext()) {
        _TupleBatch t = child.next();
        agg.mergeTupleIntoGroup(t);
      }

      it = agg.iterator();
      it.open();
    }

    if (it.hasNext())
      return it.next();
    return null;
  }

  /**
   * Returns the Schema of this Aggregate. If there is no group by field, this will have one field - the aggregate
   * column. If there is a group by field, the first field will be the group by field, and the second will be the
   * aggregate value column.
   * 
   * The name of an aggregate column should be informative. For example: "aggName(aop) (child_td.getFieldName(afield))"
   * where aop and afield are given in the constructor, and child_td is the Schema of the child iterator.
   */
  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void close() {
    super.close();
    child.close();
    if (it != null) {
      it.close();
    }
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { this.child };
  }

  @Override
  public void setChildren(Operator[] children) {
    this.child = children[0];
  }

}
