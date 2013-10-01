package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.TaskResourceManager;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 * */
public final class SymmetricHashCountingJoin extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The column indices for comparing of child 1. */
  private final int[] compareIndx1;
  /** The column indices for comparing of child 2. */
  private final int[] compareIndx2;
  /** A hash table for tuples from child 1. {Hashcode -> List of tuple indices with the same hash code} */
  private transient HashMap<Integer, List<Integer>> hashTable1Indices;
  /** A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code} */
  private transient HashMap<Integer, List<Integer>> hashTable2Indices;
  /** The buffer holding the valid tuples from left. */
  private transient TupleBuffer hashTable1;
  /** The buffer holding the valid tuples from right. */
  private transient TupleBuffer hashTable2;
  /** How many times each key occurred from left. */
  private transient List<Integer> occurredTimes1;
  /** How many times each key occurred from right. */
  private transient List<Integer> occurredTimes2;
  /** The number of join output tuples so far. */
  private long ans;
  /** The buffer for storing and returning answer. */
  private transient TupleBatchBuffer ansTBB;
  /** The name of the single column output from this operator. */
  private final String columnName;

  /**
   * Construct a {@link SymmetricHashCountingJoin}.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public SymmetricHashCountingJoin(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2) {
    this("count", left, right, compareIndx1, compareIndx2);
  }

  /**
   * Construct a {@link SymmetricHashCountingJoin} operator with schema specified.
   * 
   * @param outputColumnName the name of the column of the output table.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public SymmetricHashCountingJoin(final String outputColumnName, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2) {
    super(left, right);
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
    columnName = Objects.requireNonNull(outputColumnName);
  }

  @Override
  protected void cleanup() throws DbException {
    hashTable1 = null;
    hashTable2 = null;
    occurredTimes1 = null;
    occurredTimes2 = null;
    hashTable1Indices = null;
    hashTable2Indices = null;
    ansTBB = null;
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

    if (!nonBlocking) {
      return fetchNextReadySynchronousEOI();
    }

    if (eoi()) {
      ansTBB.put(0, ans);
      return ansTBB.popAny();
    }

    final Operator left = getLeft();
    final Operator right = getRight();
    TupleBatch leftTB = null;
    TupleBatch rightTB = null;
    int numEOS = 0;
    int numNoData = 0;

    while (numEOS < 2 && numNoData < 2) {

      numEOS = 0;
      if (left.eos()) {
        numEOS += 1;
      }
      if (right.eos()) {
        numEOS += 1;
      }
      numNoData = numEOS;

      leftTB = null;
      rightTB = null;
      if (!left.eos()) {
        leftTB = left.nextReady();
        if (leftTB != null) { // data
          processChildTB(leftTB, true);
        } else {
          // eoi or eos or no data
          if (left.eoi()) {
            left.setEOI(false);
            childrenEOI[0] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (left.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }
      if (!right.eos()) {
        rightTB = right.nextReady();
        if (rightTB != null) {
          processChildTB(rightTB, false);
        } else {
          if (right.eoi()) {
            right.setEOI(false);
            childrenEOI[1] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (right.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }
    }
    Preconditions.checkArgument(numEOS <= 2);
    Preconditions.checkArgument(numNoData <= 2);

    checkEOSAndEOI();
    if (eoi() || eos()) {
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
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    hashTable2Indices = new HashMap<Integer, List<Integer>>();
    occurredTimes1 = new ArrayList<Integer>();
    occurredTimes2 = new ArrayList<Integer>();
    hashTable1 = new TupleBuffer(getLeft().getSchema().getSubSchema(compareIndx1));
    hashTable2 = new TupleBuffer(getRight().getSchema().getSubSchema(compareIndx2));
    ans = 0;
    ansTBB = new TupleBatchBuffer(getSchema());
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
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBuffer hashTable, final int rowIndex,
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

    final Operator left = getLeft();
    final Operator right = getRight();

    TupleBuffer hashTable1Local = hashTable1;
    TupleBuffer hashTable2Local = hashTable2;
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

    if (left.eos() && !right.eos()) {
      /*
       * delete right child's hash table if the left child is EOS, since there will be no incoming tuples from right as
       * it will never be probed again.
       */
      hashTable2Indices = null;
      hashTable2 = null;
    } else if (right.eos() && !left.eos()) {
      /*
       * delete left child's hash table if the right child is EOS, since there will be no incoming tuples from left as
       * it will never be probed again.
       */
      hashTable1Indices = null;
      hashTable1 = null;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {

      /*
       * update number of count by probing the other child's hash table.
       */
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }

      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<Integer> indexList = hashTable2IndicesLocal.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable2Local, index, compareIndx1Local)) {
            ans += occurredTimes2Local.get(index);
          }
        }
      }

      /*
       * update its own hash table when necessary.
       */
      if (!left.eos() && !right.eos()) {
        final int nextIndex = hashTable1Local.numTuples();
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

  @Override
  protected Schema generateSchema() {
    return Schema.of(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of(columnName));
  }
}
