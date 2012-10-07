package edu.washington.escience.myriad.operator;

import java.io.Serializable;

import edu.washington.escience.myriad.table._TupleBatch;

/**
 * The common interface for any class that can compute an aggregate over a list of Tuples.
 */
public interface Aggregator extends Serializable {
  int NO_GROUPING = -1;

  /**
   * SUM_COUNT and SC_AVG will only be used in lab6, you are not required to implement them until then.
   * */
  public enum AggOp implements Serializable {
    MIN, MAX, SUM, AVG, COUNT,
    /**
     * SUM_COUNT: compute sum and count simultaneously, will be needed to compute distributed avg in lab6.
     * */
    SUM_COUNT,
    /**
     * SC_AVG: compute the avg of a set of SUM_COUNT tuples, will be used to compute distributed avg in lab6.
     * */
    SC_AVG;

    /**
     * Interface to access operations by a string containing an integer index for command-line convenience.
     * 
     * @param s a string containing a valid integer Op index
     */
    public static AggOp getOp(String s) {
      return getOp(Integer.parseInt(s));
    }

    /**
     * Interface to access operations by integer value for command-line convenience.
     * 
     * @param i a valid integer Op index
     */
    public static AggOp getOp(int i) {
      return values()[i];
    }

    @Override
    public String toString() {
      if (this == MIN)
        return "min";
      if (this == MAX)
        return "max";
      if (this == SUM)
        return "sum";
      if (this == SUM_COUNT)
        return "sum_count";
      if (this == AVG)
        return "avg";
      if (this == COUNT)
        return "count";
      if (this == SC_AVG)
        return "sc_avg";
      throw new IllegalStateException("impossible to reach here");
    }
  }

  /**
   * Merge a new tuple into the aggregate for a distinct group value; creates a new group aggregate result if the group
   * value has not yet been encountered.
   * 
   * @param t the Tuple containing an aggregate field and a group-by field
   */
  void mergeTupleIntoGroup(_TupleBatch t);

  /**
   * Create a DbIterator over group aggregate results.
   * 
   * @see simpledb.TupleIterator for a possible helper
   */
  DbIterator iterator();

}
