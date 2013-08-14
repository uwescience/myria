package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.TaskResourceManager;
import edu.washington.escience.myria.util.MyriaUtils;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 * */
public final class LocalJoinByReference extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Data structure for keeping references to a tuple in a TupleBatch.
   * */
  private static final class IndexedTuple {
    /**
     * The row index.
     * */
    private final int index;
    /**
     * The source data tuple batch.
     * */
    private final TupleBatch tupleBatch;

    /**
     * Factory.
     * 
     * @param tb the tuple batch
     * @param index the row of the tuple in the tuple batch
     * @return the IndexedTuple object
     */
    public static IndexedTuple of(final TupleBatch tb, final int index) {
      return new IndexedTuple(tb, index);
    }

    /**
     * The private constructor.
     * 
     * @param tb the tuple batch
     * @param index the row of the tuple in the tuple batch
     * */
    private IndexedTuple(final TupleBatch tb, final int index) {
      tupleBatch = tb;
      this.index = index;
    }

    /**
     * @return the index
     */
    public int getIndex() {
      return index;
    }

    /**
     * @return the tb
     */
    public TupleBatch getTupleBatch() {
      return tupleBatch;
    }
  }

  /**
   * The result schema.
   */
  private Schema outputSchema;
  /**
   * The column names of the result.
   * */
  private final ImmutableList<String> outputColumns;
  /**
   * The column indices for comparing of child 1.
   * */
  private final int[] compareIndx1;
  /**
   * The column indices for comparing of child 2.
   * */
  private final int[] compareIndx2;
  /**
   * A hash table for tuples from left child.
   * */
  private transient IntObjectOpenHashMap<List<IndexedTuple>> leftHashTable;
  /**
   * A hash table for tuples from right child.
   * */
  private transient IntObjectOpenHashMap<List<IndexedTuple>> rightHashTable;
  /**
   * The buffer holding the results.
   * */
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
  public LocalJoinByReference(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2) {
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
  public LocalJoinByReference(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
    this(null, left, right, compareIndx1, compareIndx2, answerColumns1, answerColumns2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param outputColumns the column names of the output table.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>, or if
   *        <tt>outputColumns</tt> does not have the correct number of columns.
   */
  public LocalJoinByReference(final List<String> outputColumns, final Operator left, final Operator right,
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
   * @param outputColumns the names of the columns of the output table.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  private LocalJoinByReference(final List<String> outputColumns, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2) {
    this(outputColumns, left, right, compareIndx1, compareIndx2, MyriaUtils.range(left.getSchema().numColumns()),
        MyriaUtils.range(right.getSchema().numColumns()));
  }

  /**
   * Generate the proper output schema from the parameters.
   */
  private void generateSchema() {
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
      outputSchema = new Schema(types.build(), outputColumns);
    } else {
      outputSchema = new Schema(types, names);
    }
  }

  /**
   * Adds a tuple to the answer.
   * 
   * @param leftTb the tuple batch that contains the left tuple
   * @param leftIdx the index of the left tuple in the leftTb
   * @param rightTb the tuple batch that contains the right tuple
   * @param rightIdx the index of the right tuple in the rightTb
   * @param fromLeft if the tuple starts from the left
   */
  protected void addToAns(final TupleBatch leftTb, final int leftIdx, final TupleBatch rightTb, final int rightIdx,
      final boolean fromLeft) {
    if (fromLeft) {
      ans.put(leftTb, leftIdx, answerColumns1, rightTb, rightIdx, answerColumns2);
    } else {
      ans.put(rightTb, rightIdx, answerColumns1, leftTb, leftIdx, answerColumns2);
    }
  }

  @Override
  protected void cleanup() throws DbException {
    leftHashTable = null;
    rightHashTable = null;
    ans = null;
  }

  /**
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic works.
   * 
   * @return result TB.
   * @throws DbException if any error occurs.
   * */
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
   * */
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
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    leftHashTable = new IntObjectOpenHashMap<List<IndexedTuple>>();
    rightHashTable = new IntObjectOpenHashMap<List<IndexedTuple>>();
    ans = new TupleBatchBuffer(outputSchema);
    TaskResourceManager qem = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    nonBlocking = qem.getExecutionMode() == QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * The query execution mode is nonBlocking.
   * */
  private transient boolean nonBlocking = true;

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromleft if the tb is from left.
   * */
  protected void processChildTB(final TupleBatch tb, final boolean fromleft) {

    IntObjectOpenHashMap<List<IndexedTuple>> hashTable1Local = leftHashTable;
    IntObjectOpenHashMap<List<IndexedTuple>> hashTable2Local = rightHashTable;
    int[] compareIndx1Local = compareIndx1;
    int[] compareIndx2Local = compareIndx2;
    if (!fromleft) {
      hashTable1Local = rightHashTable;
      hashTable2Local = leftHashTable;
      compareIndx1Local = compareIndx2;
      compareIndx2Local = compareIndx1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {
      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<IndexedTuple> tupleList = hashTable2Local.get(cntHashCode);
      if (tupleList != null) {
        for (final IndexedTuple tuple : tupleList) {
          if (tb.tupleMatches(i, compareIndx1Local, tuple.getTupleBatch(), tuple.getIndex(), compareIndx2Local)) {
            addToAns(tb, i, tuple.getTupleBatch(), tuple.getIndex(), fromleft);
          }
        }
      }

      if (hashTable1Local.containsKey(cntHashCode)) {
        tupleList = hashTable1Local.get(cntHashCode);
      } else {
        tupleList = new ArrayList<IndexedTuple>();
        hashTable1Local.put(cntHashCode, tupleList);
      }
      tupleList.add(IndexedTuple.of(tb, i));
    }
  }
}
