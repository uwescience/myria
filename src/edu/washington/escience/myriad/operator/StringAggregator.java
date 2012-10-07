package edu.washington.escience.myriad.operator;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

  /**
   * java Serialization id.
   * */
  private static final long serialVersionUID = 1L;
  /**
   * type of aggregator.
   * */
  private final Aggregator.AggOp what;
  /**
   * groups.
   * */
  private final int gbfield;
  /**
   * groups.
   * */
  private final Type gbfieldtype;
  /**
   * groups.
   * */
  private final int afield;
  /**
   * map of groupVal -> AggregateFields.
   * */
  private final HashMap<String, AggregateFields> groups;

  /**
   * Aggregate constructor.
   * 
   * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping.
   * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping.
   * @param afield the 0-based index of the aggregate field in the tuple.
   * @param what aggregation operator to use -- only supports COUNT.
   * @throws IllegalArgumentException if what != COUNT.
   */

  public StringAggregator(int gbfield, Type gbfieldtype, int afield, AggOp what) {
    this.what = what;
    if (what != AggOp.COUNT) {
      throw new IllegalArgumentException("Invalid operator type " + what);
    }
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

    agg.count++;

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
    LinkedList<TupleBatch> result = new LinkedList<TupleBatch>();
    int aggField = 1;
    Schema td;

    if (gbfield == NO_GROUPING) {
      td = new Schema(new Type[] { Type.INT_TYPE });
      aggField = 0;
    } else {
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
        case COUNT:
          tup.setField(aggField, new IntField(agg.count));
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
    String groupVal;
    int count;

    /**
     * Constructor.
     * 
     * @param groupVal group value.
     * */
    public AggregateFields(String groupVal) {
      this.groupVal = groupVal;
      count = 0;
    }
  }
}
