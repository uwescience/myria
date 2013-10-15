package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.ColumnBuilder;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.TaskResourceManager;
import edu.washington.escience.myria.util.MyriaArrayUtils;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntProcedure;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

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
  private final int[] leftCompareIndx;
  /**
   * The column indices for comparing of child 2.
   */
  private final int[] rightCompareIndx;
  /**
   * A hash table for tuples from child 1. {Hashcode -> List of tuple indices with the same hash code}
   */
  private transient TIntObjectMap<TIntList> leftHashTableIndices;
  /**
   * A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code}
   */
  private transient TIntObjectMap<TIntList> rightHashTableIndices;

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
  private final int[] leftAnswerColumns;
  /** Which columns in the right child are to be output. */
  private final int[] rightAnswerColumns;

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class JoinProcedure implements TIntProcedure {

    /**
     * Hash table.
     * */
    private TupleBuffer joinAgainstHashTable;

    /**
     * 
     * */
    private int[] inputCmpColumns;

    /**
     * the columns to compare against.
     * */
    private int[] joinAgainstCmpColumns;
    /**
     * row index of the tuple.
     * */
    private int row;

    /**
     * input TupleBatch.
     * */
    private TupleBatch inputTB;
    /**
     * if the tuple which is comparing against the list of tuples with the same hash code is from left child.
     * */
    private boolean fromLeft;

    @Override
    public boolean execute(final int index) {
      if (inputTB.tupleEquals(row, joinAgainstHashTable, index, inputCmpColumns, joinAgainstCmpColumns)) {
        addToAns(inputTB, row, joinAgainstHashTable, index, fromLeft);
      }
      return true;
    }
  };

  /**
   * Traverse through the list of tuples.
   * */
  private transient JoinProcedure doJoin;

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
    leftCompareIndx = MyriaArrayUtils.checkSet(compareIndx1);
    rightCompareIndx = MyriaArrayUtils.checkSet(compareIndx2);
    leftAnswerColumns = MyriaArrayUtils.checkSet(answerColumns1);
    rightAnswerColumns = MyriaArrayUtils.checkSet(answerColumns2);

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

    for (int i : leftAnswerColumns) {
      types.add(left.getSchema().getColumnType(i));
      names.add(left.getSchema().getColumnName(i));
    }

    for (int i : rightAnswerColumns) {
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
   * @param cntTB current TB
   * @param row current row
   * @param hashTable the buffer holding the tuples to join against
   * @param index the index of hashTable, which the cntTuple is to join with
   * @param fromLeft if the tuple is from child 1
   */
  protected void addToAns(final TupleBatch cntTB, final int row, final TupleBuffer hashTable, final int index,
      final boolean fromLeft) {
    List<Column<?>> tbColumns = cntTB.getDataColumns();
    final int rowInColumn = cntTB.getValidIndices().get(row);
    Column<?>[] hashTblColumns = hashTable.getColumns(index);
    ColumnBuilder<?>[] hashTblColumnBuilders = null;
    if (hashTblColumns == null) {
      hashTblColumnBuilders = hashTable.getColumnBuilders(index);
    }
    int tupleIdx = hashTable.getTupleIndexInContainingTB(index);
    if (fromLeft) {
      for (int i = 0; i < leftAnswerColumns.length; ++i) {
        ans.put(i, tbColumns.get(leftAnswerColumns[i]), rowInColumn);
      }

      for (int i = 0; i < rightAnswerColumns.length; ++i) {
        if (hashTblColumns != null) {
          ans.put(i + leftAnswerColumns.length, hashTblColumns[rightAnswerColumns[i]], tupleIdx);
        } else {
          ans.put(i + leftAnswerColumns.length, hashTblColumnBuilders[rightAnswerColumns[i]], tupleIdx);
        }
      }
    } else {
      for (int i = 0; i < leftAnswerColumns.length; ++i) {
        if (hashTblColumns != null) {
          ans.put(i, hashTblColumns[leftAnswerColumns[i]], tupleIdx);
        } else {
          ans.put(i, hashTblColumnBuilders[leftAnswerColumns[i]], tupleIdx);
        }
      }

      for (int i = 0; i < rightAnswerColumns.length; ++i) {
        ans.put(i + leftAnswerColumns.length, tbColumns.get(rightAnswerColumns[i]), rowInColumn);
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
    leftHashTableIndices = new TIntObjectHashMap<TIntList>();
    rightHashTableIndices = new TIntObjectHashMap<TIntList>();

    generateSchema();

    hashTable1 = new TupleBuffer(left.getSchema());
    hashTable2 = new TupleBuffer(right.getSchema());

    ans = new TupleBatchBuffer(getSchema());

    TaskResourceManager qem = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    nonBlocking = qem.getExecutionMode() == QueryExecutionMode.NON_BLOCKING;
    doJoin = new JoinProcedure();
  }

  /**
   * The query execution mode is nonBlocking.
   */
  private transient boolean nonBlocking = true;

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromLeft if the tb is from left.
   */
  protected void processChildTB(final TupleBatch tb, final boolean fromLeft) {

    final Operator left = getLeft();
    final Operator right = getRight();

    if (left.eos() && rightHashTableIndices != null) {
      /*
       * delete right child's hash table if the left child is EOS, since there will be no incoming tuples from right as
       * it will never be probed again.
       */
      rightHashTableIndices = null;
      hashTable2 = null;
    }
    if (right.eos() && leftHashTableIndices != null) {
      /*
       * delete left child's hash table if the right child is EOS, since there will be no incoming tuples from left as
       * it will never be probed again.
       */
      leftHashTableIndices = null;
      hashTable1 = null;
    }

    TupleBuffer hashTable1Local = null;
    TIntObjectMap<TIntList> hashTable1IndicesLocal = null;
    TIntObjectMap<TIntList> hashTable2IndicesLocal = null;
    if (fromLeft) {
      hashTable1Local = hashTable1;
      doJoin.joinAgainstHashTable = hashTable2;
      hashTable1IndicesLocal = leftHashTableIndices;
      hashTable2IndicesLocal = rightHashTableIndices;
      doJoin.inputCmpColumns = leftCompareIndx;
      doJoin.joinAgainstCmpColumns = rightCompareIndx;
    } else {
      hashTable1Local = hashTable2;
      doJoin.joinAgainstHashTable = hashTable1;
      hashTable1IndicesLocal = rightHashTableIndices;
      hashTable2IndicesLocal = leftHashTableIndices;
      doJoin.inputCmpColumns = rightCompareIndx;
      doJoin.joinAgainstCmpColumns = leftCompareIndx;
    }
    doJoin.fromLeft = fromLeft;
    doJoin.inputTB = tb;

    for (int row = 0; row < tb.numTuples(); ++row) {
      final int cntHashCode = tb.hashCode(row, doJoin.inputCmpColumns);
      TIntList tuplesWithHashCode = hashTable2IndicesLocal.get(cntHashCode);
      if (tuplesWithHashCode != null) {
        doJoin.row = row;
        tuplesWithHashCode.forEach(doJoin);
      }

      if (hashTable1Local != null) {
        // only build hash table on two sides if none of the children is EOS
        addToHashTable(tb, row, hashTable1Local, hashTable1IndicesLocal, cntHashCode);
      }
    }
  }

  /**
   * @param tb the source TupleBatch
   * @param row the row number to get added to hash table
   * @param hashTable the target hash table
   * @param hashTable1IndicesLocal hash table 1 indices local
   * @param hashCode the hashCode of the tb.
   * */
  private void addToHashTable(final TupleBatch tb, final int row, final TupleBuffer hashTable,
      final TIntObjectMap<TIntList> hashTable1IndicesLocal, final int hashCode) {
    final int nextIndex = hashTable.numTuples();
    TIntList tupleIndicesList = hashTable1IndicesLocal.get(hashCode);
    if (tupleIndicesList == null) {
      tupleIndicesList = new TIntArrayList();
      hashTable1IndicesLocal.put(hashCode, tupleIndicesList);
    }
    tupleIndicesList.add(nextIndex);
    List<Column<?>> inputColumns = tb.getDataColumns();
    int inColumnRow = tb.getValidIndices().get(row);
    for (int column = 0; column < tb.numColumns(); column++) {
      hashTable.put(column, inputColumns.get(column), inColumnRow);
    }
  }
}
