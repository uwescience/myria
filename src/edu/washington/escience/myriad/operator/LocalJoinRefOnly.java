package edu.washington.escience.myriad.operator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;

/**
 * Join and keep the references to the source TupleBatches.
 * */
public final class LocalJoinRefOnly extends Operator {

  /**
   * Pointer data structure for pointing to a tuple in a TupleBatch.
   * */
  private class IndexedTuple {
    /**
     * The row index.
     * */
    private final int index;
    /**
     * The source data TB.
     * */
    private final TupleBatch tb;

    /**
     * @param tb the source data TB.
     * @param index the row index.
     * */
    public IndexedTuple(final TupleBatch tb, final int index) {
      this.tb = tb;
      this.index = index;
    }

    /**
     * compare the equality of a column of the indexed tuple and another tuple.
     * 
     * @return true if equal.
     * @param another another indexed tuple
     * @param colIndx1 column index of this indexed tuple
     * @param colIndx2 column index of another indexed tuple
     * */
    public boolean columnEquals(final IndexedTuple another, final int colIndx1, final int colIndx2) {
      final Type type1 = tb.getSchema().getColumnType(colIndx1);
      // type check in query plan?
      final int rowIndx1 = index;
      final int rowIndx2 = another.index;
      switch (type1) {
        case INT_TYPE:
          return tb.getInt(colIndx1, rowIndx1) == another.tb.getInt(colIndx2, rowIndx2);
        case DOUBLE_TYPE:
          return tb.getDouble(colIndx1, rowIndx1) == another.tb.getDouble(colIndx2, rowIndx2);
        case STRING_TYPE:
          return tb.getString(colIndx1, rowIndx1).equals(another.tb.getString(colIndx2, rowIndx2));
        case FLOAT_TYPE:
          return tb.getFloat(colIndx1, rowIndx1) == another.tb.getFloat(colIndx2, rowIndx2);
        case BOOLEAN_TYPE:
          return tb.getBoolean(colIndx1, rowIndx1) == another.tb.getBoolean(colIndx2, rowIndx2);
        case LONG_TYPE:
          return tb.getLong(colIndx1, rowIndx1) == another.tb.getLong(colIndx2, rowIndx2);
      }
      return false;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      if (!(tb.getSchema().equals(another.tb.getSchema()))) {
        return false;
      }
      for (int i = 0; i < tb.getSchema().numColumns(); ++i) {
        if (!columnEquals(another, i, i)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int hashCode() {
      return tb.hashCode(index);
    }

    /**
     * compute a hash code for all the columns indexed by colIndx.
     * 
     * @param colIndx the columns to compute hash code.
     * @return the hash code of the columns
     * */
    public int hashCode4Keys(final int[] colIndx) {
      return tb.hashCode(index, colIndx);
    }

    /**
     * @return if this index tuple can be equi-joined with another index tuple
     * @param o another indexed tuple
     * @param compareIndx1 column indices of this indexed tuple to compare with
     * @param compareIndx2 column indices of another indexed tuple to compare with
     * */
    public boolean joinEquals(final Object o, final int[] compareIndx1, final int[] compareIndx2) {
      if (!(o instanceof IndexedTuple)) {
        return false;
      }
      if (compareIndx1.length != compareIndx2.length) {
        return false;
      }
      final IndexedTuple another = (IndexedTuple) o;
      for (int i = 0; i < compareIndx1.length; ++i) {
        if (!columnEquals(another, compareIndx1[i], compareIndx2[i])) {
          return false;
        }
      }
      return true;
    }
  }

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The two children.
   * */
  private Operator child1, child2;
  /**
   * The result schema.
   * */
  private Schema outputSchema;
  /**
   * Comparing column indices of child1.
   * */
  private int[] compareIndx1;
  /**
   * Comparing column indices of child2.
   * */
  private int[] compareIndx2;

  /**
   * Hash table for child1 tuples. { hash code -> list of indexed tuples}.
   * */
  private transient HashMap<Integer, List<IndexedTuple>> hashTable1;

  /**
   * Hash table for child2 tuples. { hash code -> list of indexed tuples}.
   * */
  private transient HashMap<Integer, List<IndexedTuple>> hashTable2;
  /**
   * Buffer holding the result.
   * */
  private transient TupleBatchBuffer ans;

  /**
   * For Java serialization.
   */
  public LocalJoinRefOnly() {
  }

  /**
   * @param outputSchema output schema.
   * @param child1 .
   * @param child2 .
   * @param compareIndx1 comparing column indices for child1
   * @param compareIndx2 comparing column indices for child2
   * */
  public LocalJoinRefOnly(final Schema outputSchema, final Operator child1, final Operator child2,
      final int[] compareIndx1, final int[] compareIndx2) {
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
  }

  /**
   * @param tuple1 join source tuple 1
   * @param tuple2 join source tuple 2
   * */
  protected void addToAns(final IndexedTuple tuple1, final IndexedTuple tuple2) {
    final int num1 = tuple1.tb.getSchema().numColumns();
    final int num2 = tuple2.tb.getSchema().numColumns();
    for (int i = 0; i < num1; ++i) {
      ans.put(i, tuple1.tb.getObject(i, tuple1.index));
    }
    for (int i = 0; i < num2; ++i) {
      ans.put(i + num1, tuple2.tb.getObject(i, tuple2.index));
    }
  }

  @Override
  protected void cleanup() throws DbException {
    hashTable1 = null;
    hashTable2 = null;
    ans = null;
  }

  @Override
  public void checkEOSAndEOI() {

    if (child1.eos() && child2.eos()) {
      setEOS();
      return;
    }

    // EOS could be used as an EOI
    if ((childrenEOI[0] || child1.eos()) && (childrenEOI[1] || child2.eos())) {
      setEOI(true);
      Arrays.fill(childrenEOI, false);
    }
  }

  /**
   * recording the status of children EOI.
   * */
  private final boolean[] childrenEOI = new boolean[2];

  /**
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic works.
   * 
   * @return result TB.
   * @throws DbException if any error occurs.
   * */
  private TupleBatch fetchNextReadySynchronousEOI() throws DbException {
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasnewtuple = false;
      if (!child1.eos() && !childrenEOI[0]) {
        TupleBatch tb = child1.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, true);
        } else {
          if (child1.eoi()) {
            child1.setEOI(false);
            childrenEOI[0] = true;
          }
        }
      }
      if (!child2.eos() && !childrenEOI[1]) {
        TupleBatch tb = child2.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, false);
        } else {
          if (child2.eoi()) {
            child2.setEOI(false);
            childrenEOI[1] = true;
          }
        }
      }
      nexttb = ans.popFilled();
      if (nexttb != null) {
        return nexttb;
      }
      if (!hasnewtuple) {
        break;
      }
    }
    if (nexttb == null) {
      if (ans.numTuples() > 0) {
        nexttb = ans.popAny();
      }
      checkEOSAndEOI();
    }
    return nexttb;
  }

  /**
   * The query execution mode is nonBlocking.
   * */
  private transient boolean nonBlocking = true;

  @Override
  public TupleBatch fetchNextReady() throws DbException {
    if (!nonBlocking) {
      return fetchNextReadySynchronousEOI();
    }
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    }

    if (eoi()) {
      return ans.popAny();
    }

    TupleBatch child1TB = null;
    TupleBatch child2TB = null;

    int numEOS = 0;
    if (child1.eos()) {
      numEOS += 1;
    }
    if (child2.eos()) {
      numEOS += 1;
    }
    int numNoData = numEOS;

    while (numEOS < 2 && numNoData < 2) {
      child1TB = null;
      child2TB = null;
      if (!child1.eos()) {
        child1TB = child1.nextReady();
        if (child1TB != null) { // data
          processChildTB(child1TB, true);
          nexttb = ans.popFilled();
          if (nexttb != null) {
            return nexttb;
          }
        } else {
          // eoi or eos or no data
          if (child1.eoi()) {
            child1.setEOI(false);
            childrenEOI[0] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (child1.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }
      if (!child2.eos()) {
        child2TB = child2.nextReady();
        if (child2TB != null) {
          processChildTB(child2TB, false);
          nexttb = ans.popFilled();
          if (nexttb != null) {
            return nexttb;
          }
        } else {
          if (child2.eoi()) {
            child2.setEOI(false);
            childrenEOI[1] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (child2.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }
    }

    checkEOSAndEOI();
    if (eoi() || eos()) {
      nexttb = ans.popAny();
    }
    return nexttb;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  /**
   * @param tb the source tb for processing.
   * @param tbFromChild1 if the TB is from child1.
   * */
  protected void processChildTB(final TupleBatch tb, final boolean tbFromChild1) {
    List<IndexedTuple> tupleList = null;
    int[] compareIndx2Add = compareIndx1;
    int[] compareIndx2Join = compareIndx2;
    HashMap<Integer, List<IndexedTuple>> hashTable2Add = hashTable1;
    HashMap<Integer, List<IndexedTuple>> hashTable2Join = hashTable2;
    if (!tbFromChild1) {
      compareIndx2Add = compareIndx2;
      compareIndx2Join = compareIndx1;
      hashTable2Add = hashTable2;
      hashTable2Join = hashTable1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {
      final IndexedTuple tuple = new IndexedTuple(tb, i);
      final int cntHashCode = tuple.hashCode4Keys(compareIndx2Add);
      tupleList = hashTable2Join.get(cntHashCode);
      if (tupleList != null) {
        for (final IndexedTuple tuple2 : tupleList) {
          if (tuple.joinEquals(tuple2, compareIndx2Add, compareIndx2Join)) {
            if (tbFromChild1) {
              addToAns(tuple, tuple2);
            } else {
              addToAns(tuple2, tuple);
            }
          }
        }
      }
      tupleList = hashTable2Add.get(cntHashCode);
      if (tupleList == null) {
        tupleList = new LinkedList<IndexedTuple>();
        hashTable2Add.put(cntHashCode, tupleList);
      }
      tupleList.add(tuple);
    }
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    hashTable1 = new HashMap<Integer, List<IndexedTuple>>();
    hashTable2 = new HashMap<Integer, List<IndexedTuple>>();
    ans = new TupleBatchBuffer(outputSchema);
    QueryExecutionMode qem = (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE);
    nonBlocking = qem == QueryExecutionMode.NON_BLOCKING;
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }
}
