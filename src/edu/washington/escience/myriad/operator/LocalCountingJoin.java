package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 * */
public final class LocalCountingJoin extends Operator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The two children. */
  private Operator child1, child2;
  /** The result schema. */
  private final Schema outputSchema;
  /** The column indices for comparing of child 1. */
  private final int[] compareIndx1;
  /** The column indices for comparing of child 2. */
  private final int[] compareIndx2;
  /** A hash table for tuples from child 1. {Hashcode -> List of tuple indices with the same hash code} */
  private transient HashMap<Integer, List<Integer>> hashTable1Indices;
  /** A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code} */
  private transient HashMap<Integer, List<Integer>> hashTable2Indices;
  /** The buffer holding the valid tuples from child1. */
  private transient TupleBatchBuffer hashTable1;
  /** The buffer holding the valid tuples from child2. */
  private transient TupleBatchBuffer hashTable2;
  /** How many times each key occurred from child1. */
  private transient List<Integer> occurredTimes1;
  /** How many times each key occurred from child2. */
  private transient List<Integer> occurredTimes2;
  /** The number of join output tuples so far. */
  private int ans;
  /** The buffer for storing and returning answer. */
  private transient TupleBatchBuffer ansTBB;

  /**
   * Construct an LocalCountingJoin operator.
   * 
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public LocalCountingJoin(final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2) {
    this(null, child1, child2, compareIndx1, compareIndx2);
  }

  /**
   * Construct a LocalCountingJoin operator with schema specified.
   * 
   * @param outputSchema the Schema of the output table.
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalCountingJoin(final Schema outputSchema, final Operator child1, final Operator child2,
      final int[] compareIndx1, final int[] compareIndx2) {
    if (outputSchema == null) {
      this.outputSchema = getSchema();
    } else {
      this.outputSchema = outputSchema;
    }
    this.child1 = child1;
    this.child2 = child2;
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
  }

  @Override
  protected void cleanup() throws DbException {
    hashTable1 = null;
    hashTable2 = null;
    ans = 0;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    while (!child1.eos() || !child2.eos()) {
      if (!child1.eos()) {
        TupleBatch tb = child1.next();
        if (tb != null) {
          processChildTB(tb, true);
        } else {
          if (child1.eoi()) {
            child1.setEOI(false);
            childrenEOI[0] = true;
          }
        }
      }
      if (!child2.eos()) {
        TupleBatch tb = child2.next();
        if (tb != null) {
          processChildTB(tb, false);
        } else {
          if (child2.eoi()) {
            child2.setEOI(false);
            childrenEOI[1] = true;
          }
        }
      }
    }
    TupleBatchBuffer tmp = new TupleBatchBuffer(outputSchema);
    tmp.put(0, ans);
    return tmp.popAny();
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
   * Recording the EOI status of the children.
   * */
  private final boolean[] childrenEOI = new boolean[2];

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch tb;
    if (child1.eos() && child2.eos()) {
      return ansTBB.popAny();
    }
    while (!child1.eos()) {
      while ((tb = child1.nextReady()) != null) {
        processChildTB(tb, true);
      }
      if (child1.eoi()) {
        child1.setEOI(false);
        childrenEOI[0] = true;
      } else {
        break;
      }
    }
    while (!child2.eos()) {
      while ((tb = child2.nextReady()) != null) {
        processChildTB(tb, false);
      }
      if (child2.eoi()) {
        child2.setEOI(false);
        childrenEOI[1] = true;
      } else {
        break;
      }
    }
    if (child1.eos() && child2.eos()) {
      ansTBB.put(0, ans);
    }
    return ansTBB.popAny();
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    final ImmutableList.Builder<Type> types = ImmutableList.builder();
    final ImmutableList.Builder<String> names = ImmutableList.builder();
    types.add(Type.INT_TYPE);
    names.add("count");
    return new Schema(types, names);
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    occurredTimes1 = new ArrayList<Integer>();
    occurredTimes2 = new ArrayList<Integer>();
    hashTable2Indices = new HashMap<Integer, List<Integer>>();
    hashTable1 = new TupleBatchBuffer(child1.getSchema());
    hashTable2 = new TupleBatchBuffer(child2.getSchema());
    ans = 0;
    ansTBB = new TupleBatchBuffer(getSchema());
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @param cntTuple a list representation of a tuple
   * @param compareIndx1 the comparing list of columns of cntTuple
   * @param compareIndx2 the comparing list of columns of hashTable
   * @return true if equals.
   * */
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBatchBuffer hashTable, final int index,
      final int[] compareIndx1, final int[] compareIndx2) {
    if (compareIndx1.length != compareIndx2.length) {
      return false;
    }
    for (int i = 0; i < compareIndx1.length; ++i) {
      if (!cntTuple.get(compareIndx1[i]).equals(hashTable.get(compareIndx2[i], index))) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromChild1 if the tb is from child1.
   * */
  protected void processChildTB(final TupleBatch tb, final boolean fromChild1) {

    TupleBatchBuffer hashTable1Local = hashTable1;
    TupleBatchBuffer hashTable2Local = hashTable2;
    HashMap<Integer, List<Integer>> hashTable1IndicesLocal = hashTable1Indices;
    HashMap<Integer, List<Integer>> hashTable2IndicesLocal = hashTable2Indices;
    List<Integer> occurredTimes1Local = occurredTimes1;
    List<Integer> occurredTimes2Local = occurredTimes2;
    int[] compareIndx1Local = compareIndx1;
    int[] compareIndx2Local = compareIndx2;
    if (!fromChild1) {
      hashTable1Local = hashTable2;
      hashTable2Local = hashTable1;
      hashTable1IndicesLocal = hashTable2Indices;
      hashTable2IndicesLocal = hashTable1Indices;
      compareIndx1Local = compareIndx2;
      compareIndx2Local = compareIndx1;
      occurredTimes1Local = occurredTimes2;
      occurredTimes2Local = occurredTimes1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = hashTable1Local.numTuples();
      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<Integer> indexList = hashTable2IndicesLocal.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable2Local, index, compareIndx1Local, compareIndx2Local)) {
            ans += occurredTimes2Local.get(index);
          }
        }
      }

      boolean found = false;
      indexList = hashTable1IndicesLocal.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable1Local, index, compareIndx1Local, compareIndx1Local)) {
            occurredTimes1Local.set(index, occurredTimes1Local.get(index) + 1);
            found = true;
            break;
          }
        }
      }
      if (!found) {
        if (hashTable1IndicesLocal.get(cntHashCode) == null) {
          hashTable1IndicesLocal.put(cntHashCode, new ArrayList<Integer>());
        }
        hashTable1IndicesLocal.get(cntHashCode).add(nextIndex);
        for (int j = 0; j < tb.numColumns(); ++j) {
          hashTable1Local.put(j, cntTuple.get(j));
        }
        occurredTimes1Local.add(1);
      }
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
  }
}
