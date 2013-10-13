package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
 */
public final class SymmetricHashJoin extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The names of the output columns.
   */
  private final ImmutableList<String> outputColumns;

  /**
   * The column indices for comparing of child 1.
   */
  private final int[] compareIndx1;
  /**
   * The column indices for comparing of child 2.
   */
  private final int[] compareIndx2;
  /**
   * A hash table for tuples from child 1. {Hashcode -> List of tuple indices with the same hash code}
   */
  private transient HashMap<Integer, List<Integer>> hashTable1Indices;
  /**
   * A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code}
   */
  private transient HashMap<Integer, List<Integer>> hashTable2Indices;
  /**
   * The buffer holding the valid tuples from left.
   */
  private transient TupleBuffer hashTable1;
  /**
   * The buffer holding the valid tuples from right.
   */
  private transient TupleBuffer hashTable2;
  /**
   * The buffer holding the results.
   */
  private transient TupleBatchBuffer ans;
  /** Which columns in the left child are to be output. */
  private final int[] answerColumns1;
  /** Which columns in the right child are to be output. */
  private final int[] answerColumns2;

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public SymmetricHashJoin(final Operator left, final Operator right, final int[] compareIndx1, final int[] compareIndx2) {
    this(null, left, right, compareIndx1, compareIndx2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public SymmetricHashJoin(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
    this(null, left, right, compareIndx1, compareIndx2, answerColumns1, answerColumns2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding columns will be
   *          copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>, or if
   *        <tt>outputColumns</tt> does not have the correct number of columns and column types.
   */
  public SymmetricHashJoin(final List<String> outputColumns, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
    super(left, right);
    Preconditions.checkArgument(compareIndx1.length == compareIndx2.length);
    if (outputColumns != null) {
      Preconditions.checkArgument(outputColumns.size() == answerColumns1.length + answerColumns2.length,
          "length mismatch between output column names and columns selected for output");
      Preconditions.checkArgument(ImmutableSet.copyOf(outputColumns).size() == outputColumns.size(),
          "duplicate column names in outputColumns");
      this.outputColumns = ImmutableList.copyOf(outputColumns);
    } else {
      this.outputColumns = null;
    }
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
    this.answerColumns1 = answerColumns1;
    this.answerColumns2 = answerColumns2;
    if (left != null && right != null) {
      generateSchema();
    }
  }

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   * 
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding columns will be
   *          copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public SymmetricHashJoin(final List<String> outputColumns, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2) {
    this(outputColumns, left, right, compareIndx1, compareIndx2, range(left.getSchema().numColumns()), range(right
        .getSchema().numColumns()));
  }

  /**
   * Helper function that generates an array of the numbers 0..max-1.
   * 
   * @param max the size of the array.
   * @return an array of the numbers 0..max-1.
   */
  private static int[] range(final int max) {
    int[] ret = new int[max];
    for (int i = 0; i < max; ++i) {
      ret[i] = i;
    }
    return ret;
  }

  @Override
  protected Schema generateSchema() {
    final Operator left = getLeft();
    final Operator right = getRight();
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();

    for (int i : answerColumns1) {
      types.add(left.getSchema().getColumnType(i));
      names.add(left.getSchema().getColumnName(i));
    }

    for (int i : answerColumns2) {
      types.add(right.getSchema().getColumnType(i));
      names.add(right.getSchema().getColumnName(i));
    }

    if (outputColumns != null) {
      return new Schema(types.build(), outputColumns);
    } else {
      return new Schema(types, names);
    }
  }

  /**
   * @param cntTuple a list representation of a tuple
   * @param hashTable the buffer holding the tuples to join against
   * @param index the index of hashTable, which the cntTuple is to join with
   * @param fromleft if the tuple is from child 1
   */
  protected void addToAns(final List<Object> cntTuple, final TupleBuffer hashTable, final int index,
      final boolean fromleft) {
    if (fromleft) {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, cntTuple.get(answerColumns1[i]));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, hashTable.get(answerColumns2[i], index));
      }
    } else {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, hashTable.get(answerColumns1[i], index));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, cntTuple.get(answerColumns2[i]));
      }
    }
  }

  @Override
  protected void cleanup() throws DbException {
    hashTable1 = null;
    hashTable2 = null;
    ans = null;
  }

  /**
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic works.
   * 
   * @return result TB.
   * @throws DbException if any error occurs.
   */
  private TupleBatch fetchNextReadySynchronousEOI() throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasnewtuple = false;
      if (!left.eos() && !childrenEOI[0]) {
        TupleBatch tb = left.nextReady();
        if (tb != null) {
          hasnewtuple = true;
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
          hasnewtuple = true;
          processChildTB(tb, false);
        } else {
          if (right.eoi()) {
            right.setEOI(false);
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
   */
  private final boolean[] childrenEOI = new boolean[2];

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
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
          nexttb = ans.popAnyUsingTimeout();
          if (nexttb != null) {
            return nexttb;
          }
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
          nexttb = ans.popAnyUsingTimeout();
          if (nexttb != null) {
            return nexttb;
          }
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
      nexttb = ans.popAny();
    }
    return nexttb;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    hashTable2Indices = new HashMap<Integer, List<Integer>>();
    hashTable1 = new TupleBuffer(left.getSchema());
    hashTable2 = new TupleBuffer(right.getSchema());
    ans = new TupleBatchBuffer(getSchema());
    TaskResourceManager qem = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    nonBlocking = qem.getExecutionMode() == QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * The query execution mode is nonBlocking.
   */
  private transient boolean nonBlocking = true;

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @param cntTuple a list representation of a tuple
   * @param compareIndx1 the comparing list of columns of cntTuple
   * @param compareIndx2 the comparing list of columns of hashTable
   * @return true if equals.
   */
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBuffer hashTable, final int index,
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
   * @param fromleft if the tb is from left.
   */
  protected void processChildTB(final TupleBatch tb, final boolean fromleft) {

    final Operator left = getLeft();
    final Operator right = getRight();

    TupleBuffer hashTable1Local = hashTable1;
    TupleBuffer hashTable2Local = hashTable2;
    HashMap<Integer, List<Integer>> hashTable1IndicesLocal = hashTable1Indices;
    HashMap<Integer, List<Integer>> hashTable2IndicesLocal = hashTable2Indices;
    int[] compareIndx1Local = compareIndx1;
    int[] compareIndx2Local = compareIndx2;
    if (!fromleft) {
      hashTable1Local = hashTable2;
      hashTable2Local = hashTable1;
      hashTable1IndicesLocal = hashTable2Indices;
      hashTable2IndicesLocal = hashTable1Indices;
      compareIndx1Local = compareIndx2;
      compareIndx2Local = compareIndx1;
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
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<Integer> indexList = hashTable2IndicesLocal.get(cntHashCode);

      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable2Local, index, compareIndx1Local, compareIndx2Local)) {
            addToAns(cntTuple, hashTable2Local, index, fromleft);
          }
        }
      }

      // only build hash table on two sides if none of the children is EOS
      if (!left.eos() && !right.eos()) {
        final int nextIndex = hashTable1Local.numTuples();
        if (hashTable1IndicesLocal.get(cntHashCode) == null) {
          hashTable1IndicesLocal.put(cntHashCode, new ArrayList<Integer>());
        }
        hashTable1IndicesLocal.get(cntHashCode).add(nextIndex);
        for (int j = 0; j < tb.numColumns(); ++j) {
          hashTable1Local.put(j, cntTuple.get(j));
        }
      }
    }

  }
}
