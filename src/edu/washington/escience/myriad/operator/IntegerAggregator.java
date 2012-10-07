package edu.washington.escience.myriad.operator;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

  private static final long serialVersionUID = 1L;

  private final AggOp what;
  private final int gbfield;
  private final Type gbfieldtype;
  private final int afield;
  // a map of groupVal -> AggregateFields
  private final HashMap<String, AggregateFields> groups;

  /**
   * Aggregate constructor
   * 
   * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
   * @param afield the 0-based index of the aggregate field in the tuple
   * @param what the aggregation operator
   */

  public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, AggOp what) {
    this.what = what;
    this.gbfield = gbfield;
    this.afield = afield;
    this.gbfieldtype = gbfieldtype;
    this.groups = new HashMap<String, AggregateFields>();
  }

  /**
   * Merge a new tuple into the aggregate, grouping as indicated in the constructor
   * 
   * @param tup the Tuple containing an aggregate field and a group-by field
   */
  @Override
  public void mergeTupleIntoGroup(TupleBatch tup) {
    String groupVal = "";
    if (gbfield != NO_GROUPING) {
      groupVal = tup.getField(gbfield).toString();
    }
    AggregateFields agg = groups.get(groupVal);
    if (agg == null)
      agg = new AggregateFields(groupVal);

    int x = ((IntField) tup.getField(afield)).getValue();

    agg.count++;
    agg.sum += x;
    agg.min = (x < agg.min ? x : agg.min);
    agg.max = (x > agg.max ? x : agg.max);
    if (what == AggOp.SC_AVG)
      agg.sumCount += ((IntField) tup.getField(afield + 1)).getValue();

    groups.put(groupVal, agg);
  }

  /**
   * Create a DbIterator over group aggregate results.
   * 
   * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal) if using group, or a single (aggregateVal)
   *         if no grouping. The aggregateVal is determined by the type of aggregate specified in the constructor.
   */
  @Override
  public DbIterator iterator() {
    LinkedList<Tuple> result = new LinkedList<Tuple>();
    int aggField = 1;
    Schema td;

    if (gbfield == NO_GROUPING) {
      if (what == AggOp.SUM_COUNT)
        td = new Schema(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
      else
        td = new Schema(new Type[] { Type.INT_TYPE });
      aggField = 0;
    } else {
      if (what == AggOp.SUM_COUNT)
        td = new Schema(new Type[] { gbfieldtype, Type.INT_TYPE, Type.INT_TYPE });
      else
        td = new Schema(new Type[] { gbfieldtype, Type.INT_TYPE });
    }

    // iterate over groups and create summary tuples
    for (String groupVal : groups.keySet()) {
      AggregateFields agg = groups.get(groupVal);
      Tuple tup = new Tuple(td);

      if (gbfield != NO_GROUPING) {
        if (gbfieldtype == Type.INT_TYPE)
          tup.setField(0, new IntField(new Integer(groupVal)));
        else
          tup.setField(0, new StringField(groupVal, Type.STRING_LEN));
      }
      switch (what) {
        case MIN:
          tup.setField(aggField, new IntField(agg.min));
          break;
        case MAX:
          tup.setField(aggField, new IntField(agg.max));
          break;
        case SUM:
          tup.setField(aggField, new IntField(agg.sum));
          break;
        case COUNT:
          tup.setField(aggField, new IntField(agg.count));
          break;
        case AVG:
          tup.setField(aggField, new IntField(agg.sum / agg.count));
          break;
        case SUM_COUNT:
          tup.setField(aggField, new IntField(agg.sum));
          tup.setField(aggField + 1, new IntField(agg.count));
          break;
        case SC_AVG:
          tup.setField(aggField, new IntField(agg.sum / agg.sumCount));
          break;
      }

      result.add(tup);
    }

    DbIterator retVal = null;
    retVal = new TupleIterator(td, Collections.unmodifiableList(result));
    return retVal;
  }

  /**
   * A helper struct to store accumulated aggregate values.
   */
  private class AggregateFields {
    public String groupVal;
    public int min, max, sum, count, sumCount;

    public AggregateFields(String groupVal) {
      this.groupVal = groupVal;
      min = Integer.MAX_VALUE;
      max = Integer.MIN_VALUE;
      sum = count = sumCount = 0;
    }
  }

}
