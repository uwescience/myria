package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.TaskResourceManager;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 * */
public final class LocalCountingJoin extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

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
  /** The buffer holding the valid tuples from left. */
  private transient TupleBatchBuffer hashTable1;
  /** The buffer holding the valid tuples from right. */
  private transient TupleBatchBuffer hashTable2;
  /** How many times each key occurred from left. */
  private transient List<Integer> occurredTimes1;
  /** How many times each key occurred from right. */
  private transient List<Integer> occurredTimes2;
  /** The number of join output tuples so far. */
  private long ans;
  /** The buffer for storing and returning answer. */
  private transient TupleBatchBuffer ansTBB;

  /**
   * Construct an LocalCountingJoin operator.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public LocalCountingJoin(final Operator left, final Operator right, final int[] compareIndx1, final int[] compareIndx2) {
    this(null, left, right, compareIndx1, compareIndx2);
  }

  /**
   * Construct a LocalCountingJoin operator with schema specified.
   * 
   * @param outputColumnName the name of the column of the output table.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalCountingJoin(final String outputColumnName, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2) {
    super(left, right);
    ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE);
    ImmutableList<String> names;
    if (outputColumnName == null) {
      names = ImmutableList.of("count");
    } else {
      names = ImmutableList.of(outputColumnName);
    }
    outputSchema = Schema.of(types, names);
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
  public void checkEOSAndEOI() {
    final Operator left = getLeft();
    final Operator right = getRight();
    if (left.eos() && right.eos()) {
      setEOS();
      return;
    }

    // EOS could be used as an EOI
    if ((childrenEOI[0] || left.eos()) && (childrenEOI[1] || right.eos())) {
      setEOI(true);
      Arrays.fill(childrenEOI, false);
    }
  }

  /**
   * Recording the EOI status of the children.
   * */
  private final boolean[] childrenEOI = new boolean[2];

  /**
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic works.
   * 
   * @return result TB.
   * @throws DbException if any error occurs.
   * */
  private TupleBatch fetchNextReadySynchronousEOI() throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();
    while (true) {
      boolean hasNewTuple = false;
      if (!left.eos() && !childrenEOI[0]) {
        TupleBatch tb = left.nextReady();
        if (tb != null) {
          hasNewTuple = true;
          processChildTB(tb, true);
        } else {
          if (left.eoi()) {
            left.setEOI(false);
            childrenEOI[0] = true;
          }
        }
      }
      if (!right.eos() && !childrenEOI[1]) {
        TupleBatch tb = right.nextReady();
        if (tb != null) {
          hasNewTuple = true;
          processChildTB(tb, false);
        } else {
          if (right.eoi()) {
            right.setEOI(false);
            childrenEOI[1] = true;
          }
        }
      }
      if (!hasNewTuple) {
        break;
      }
    }

    checkEOSAndEOI();
    if (eoi() || eos()) {
      ansTBB.put(0, ans);
      return ansTBB.popAny();
    }
    return null;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();
    if (!nonBlocking) {
      return fetchNextReadySynchronousEOI();
    }
    TupleBatch tb;
    if (left.eos() && right.eos()) {
      return ansTBB.popAny();
    }
    while (!left.eos()) {
      while ((tb = left.nextReady()) != null) {
        processChildTB(tb, true);
      }
      if (left.eoi()) {
        left.setEOI(false);
        childrenEOI[0] = true;
      } else {
        break;
      }
    }
    while (!right.eos()) {
      while ((tb = right.nextReady()) != null) {
        processChildTB(tb, false);
      }
      if (right.eoi()) {
        right.setEOI(false);
        childrenEOI[1] = true;
      } else {
        break;
      }
    }
    checkEOSAndEOI();
    if (eos() || eoi()) {
      ansTBB.put(0, ans);
      return ansTBB.popAny();
    }
    return null;
  }

  /**
   * The query execution mode is nonBlocking.
   * */
  private transient boolean nonBlocking = true;

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    occurredTimes1 = new ArrayList<Integer>();
    occurredTimes2 = new ArrayList<Integer>();
    hashTable2Indices = new HashMap<Integer, List<Integer>>();
    hashTable1 = new TupleBatchBuffer(getLeft().getSchema().getSubSchema(compareIndx1));
    hashTable2 = new TupleBatchBuffer(getLeft().getSchema().getSubSchema(compareIndx2));
    ans = 0;
    ansTBB = new TupleBatchBuffer(outputSchema);
    TaskResourceManager qem = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    nonBlocking = qem.getExecutionMode() == QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param rowIndex the row index in the hashTable
   * @param cntTuple a list representation of a tuple
   * @param compareIndx the comparing list of columns of cntTuple
   * @return true if equals.
   * */
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBatchBuffer hashTable, final int rowIndex,
      final int[] compareIndx) {
    if (compareIndx.length != hashTable.getSchema().numColumns()) {
      return false;
    }
    for (int i = 0; i < compareIndx.length; ++i) {
      if (!cntTuple.get(compareIndx[i]).equals(hashTable.get(i, rowIndex))) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromleft if the tb is from left.
   * */
  protected void processChildTB(final TupleBatch tb, final boolean fromleft) {

    TupleBatchBuffer hashTable1Local = hashTable1;
    TupleBatchBuffer hashTable2Local = hashTable2;
    HashMap<Integer, List<Integer>> hashTable1IndicesLocal = hashTable1Indices;
    HashMap<Integer, List<Integer>> hashTable2IndicesLocal = hashTable2Indices;
    List<Integer> occurredTimes1Local = occurredTimes1;
    List<Integer> occurredTimes2Local = occurredTimes2;
    int[] compareIndx1Local = compareIndx1;
    if (!fromleft) {
      hashTable1Local = hashTable2;
      hashTable2Local = hashTable1;
      hashTable1IndicesLocal = hashTable2Indices;
      hashTable2IndicesLocal = hashTable1Indices;
      compareIndx1Local = compareIndx2;
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
          if (tupleEquals(cntTuple, hashTable2Local, index, compareIndx1Local)) {
            ans += occurredTimes2Local.get(index);
          }
        }
      }

      boolean found = false;
      indexList = hashTable1IndicesLocal.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable1Local, index, compareIndx1Local)) {
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
        for (int j = 0; j < compareIndx1Local.length; ++j) {
          hashTable1Local.put(j, cntTuple.get(compareIndx1Local[j]));
        }
        occurredTimes1Local.add(1);
      }
    }
  }
}
