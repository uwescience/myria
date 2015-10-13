package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.gs.collections.api.block.procedure.primitive.IntProcedure;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does
 * not keep the references to the incoming TupleBatches in order to get better memory performance.
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
   * A hash table for tuples from child 1. {Hashcode -> List of tuple indices with the same hash
   * code}
   */
  private transient IntObjectHashMap<IntArrayList> leftHashTableIndices;
  /**
   * A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash
   * code}
   */
  private transient IntObjectHashMap<IntArrayList> rightHashTableIndices;

  /**
   * The buffer holding the valid tuples from left.
   */
  private transient MutableTupleBuffer hashTable1;
  /**
   * The buffer holding the valid tuples from right.
   */
  private transient MutableTupleBuffer hashTable2;
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
  private final class JoinProcedure implements IntProcedure {

    /** serialization id. */
    private static final long serialVersionUID = 1L;

    /**
     * Hash table.
     * */
    private MutableTupleBuffer joinAgainstHashTable;

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
     * if the tuple which is comparing against the list of tuples with the same hash code is from
     * left child.
     * */
    private boolean fromLeft;

    @Override
    public void value(final int index) {
      if (TupleUtils.tupleEquals(inputTB, inputCmpColumns, row, joinAgainstHashTable,
          joinAgainstCmpColumns, index)) {

        addToAns(inputTB, row, joinAgainstHashTable, index, fromLeft);
      }
    }
  };

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class ReplaceProcedure implements IntProcedure {

    /** serialization id. */
    private static final long serialVersionUID = 1L;

    /**
     * Hash table.
     * */
    private MutableTupleBuffer hashTable;

    /**
     * the columns to compare against.
     * */
    private int[] keyColumns;
    /**
     * row index of the tuple.
     * */
    private int row;

    /**
     * input TupleBatch.
     * */
    private TupleBatch inputTB;

    /** if found a replacement. */
    private boolean replaced;

    @Override
    public void value(final int index) {
      if (TupleUtils.tupleEquals(inputTB, keyColumns, row, hashTable, keyColumns, index)) {
        replaced = true;
        List<? extends Column<?>> columns = inputTB.getDataColumns();
        for (int j = 0; j < inputTB.numColumns(); ++j) {
          hashTable.replace(j, index, columns.get(j), row);
        }
      }
    }
  };

  /**
   * Traverse through the list of tuples.
   * */
  private transient JoinProcedure doJoin;

  /**
   * Traverse through the list of tuples and replace old values.
   * */
  private transient ReplaceProcedure doReplace;

  /** Whether the last child polled was the left child. */
  private boolean pollLeft = false;

  /** Join pull order, default: ALTER. */
  private JoinPullOrder order = JoinPullOrder.ALTER;

  /** if the hash table of the left child should use set semantics. */
  private boolean setSemanticsLeft = false;
  /** if the hash table of the right child should use set semantics. */
  private boolean setSemanticsRight = false;

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the
   * corresponding columns in compareIndx1 and compareIndx2 match.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public SymmetricHashJoin(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2) {
    this(null, left, right, compareIndx1, compareIndx2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the
   * corresponding columns in compareIndx1 and compareIndx2 match.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>,
   *        or if <tt>outputSchema</tt> does not have the correct number of columns and column
   *        types.
   */
  public SymmetricHashJoin(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
    this(null, left, right, compareIndx1, compareIndx2, answerColumns1, answerColumns2);
  }

  /**
   * Construct an SymmetricHashJoin operator. It returns the specified columns from both children
   * when the corresponding columns in compareIndx1 and compareIndx2 match.
   * 
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @param setSemanticsLeft if the hash table of the left child should use set semantics.
   * @param setSemanticsRight if the hash table of the right child should use set semantics.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>,
   *        or if <tt>outputSchema</tt> does not have the correct number of columns and column
   *        types.
   */
  public SymmetricHashJoin(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2,
      final boolean setSemanticsLeft, final boolean setSemanticsRight) {
    this(null, left, right, compareIndx1, compareIndx2, answerColumns1, answerColumns2);
    this.setSemanticsLeft = setSemanticsLeft;
    this.setSemanticsRight = setSemanticsRight;
  }

  /**
   * Construct an SymmetricHashJoin operator. It returns the specified columns from both children
   * when the corresponding columns in compareIndx1 and compareIndx2 match.
   * 
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding
   *        columns will be copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters. * @param
   *        setSemanticsLeft if the hash table of the left child should use set semantics.
   * @param setSemanticsLeft if the hash table of the left child should use set semantics.
   * @param setSemanticsRight if the hash table of the right child should use set semantics.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>,
   *        or if <tt>outputColumns</tt> does not have the correct number of columns and column
   *        types.
   */
  public SymmetricHashJoin(final List<String> outputColumns, final Operator left,
      final Operator right, final int[] compareIndx1, final int[] compareIndx2,
      final int[] answerColumns1, final int[] answerColumns2, final boolean setSemanticsLeft,
      final boolean setSemanticsRight) {
    this(outputColumns, left, right, compareIndx1, compareIndx2, answerColumns1, answerColumns2);
    this.setSemanticsLeft = setSemanticsLeft;
    this.setSemanticsRight = setSemanticsRight;
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the
   * corresponding columns in compareIndx1 and compareIndx2 match.
   * 
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding
   *        columns will be copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>,
   *        or if <tt>outputColumns</tt> does not have the correct number of columns and column
   *        types.
   */
  public SymmetricHashJoin(final List<String> outputColumns, final Operator left,
      final Operator right, final int[] compareIndx1, final int[] compareIndx2,
      final int[] answerColumns1, final int[] answerColumns2) {
    super(left, right);
    Preconditions.checkArgument(compareIndx1.length == compareIndx2.length);
    if (outputColumns != null) {
      Preconditions.checkArgument(outputColumns.size() == answerColumns1.length
          + answerColumns2.length,
          "length mismatch between output column names and columns selected for output");
      Preconditions.checkArgument(
          ImmutableSet.copyOf(outputColumns).size() == outputColumns.size(),
          "duplicate column names in outputColumns");
      this.outputColumns = ImmutableList.copyOf(outputColumns);
    } else {
      this.outputColumns = null;
    }
    leftCompareIndx = MyriaArrayUtils.warnIfNotSet(compareIndx1);
    rightCompareIndx = MyriaArrayUtils.warnIfNotSet(compareIndx2);
    leftAnswerColumns = MyriaArrayUtils.warnIfNotSet(answerColumns1);
    rightAnswerColumns = MyriaArrayUtils.warnIfNotSet(answerColumns2);
  }

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the
   * corresponding columns in compareIndx1 and compareIndx2 match.
   * 
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding
   *        columns will be copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>,
   *        or if <tt>outputSchema</tt> does not have the correct number of columns and column
   *        types.
   */
  public SymmetricHashJoin(final List<String> outputColumns, final Operator left,
      final Operator right, final int[] compareIndx1, final int[] compareIndx2) {
    this(outputColumns, left, right, compareIndx1, compareIndx2, range(left.getSchema()
        .numColumns()), range(right.getSchema().numColumns()));
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
    final Schema leftSchema = getLeft().getSchema();
    final Schema rightSchema = getRight().getSchema();
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();

    /* Assert that the compare index types are the same. */
    for (int i = 0; i < rightCompareIndx.length; ++i) {
      int leftIndex = leftCompareIndx[i];
      int rightIndex = rightCompareIndx[i];
      Type leftType = leftSchema.getColumnType(leftIndex);
      Type rightType = rightSchema.getColumnType(rightIndex);
      Preconditions
          .checkState(
              leftType == rightType,
              "column types do not match for join at index %s: left column type %s [%s] != right column type %s [%s]",
              i, leftIndex, leftType, rightIndex, rightType);
    }

    for (int i : leftAnswerColumns) {
      types.add(leftSchema.getColumnType(i));
      names.add(leftSchema.getColumnName(i));
    }

    for (int i : rightAnswerColumns) {
      types.add(rightSchema.getColumnType(i));
      names.add(rightSchema.getColumnName(i));
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
  protected void addToAns(final TupleBatch cntTB, final int row,
      final MutableTupleBuffer hashTable, final int index, final boolean fromLeft) {
    List<? extends Column<?>> tbColumns = cntTB.getDataColumns();
    ReadableColumn[] hashTblColumns = hashTable.getColumns(index);
    int tupleIdx = hashTable.getTupleIndexInContainingTB(index);
    if (fromLeft) {
      for (int i = 0; i < leftAnswerColumns.length; ++i) {
        ans.put(i, tbColumns.get(leftAnswerColumns[i]), row);
      }

      for (int i = 0; i < rightAnswerColumns.length; ++i) {
        ans.put(i + leftAnswerColumns.length, hashTblColumns[rightAnswerColumns[i]], tupleIdx);
      }
    } else {
      for (int i = 0; i < leftAnswerColumns.length; ++i) {
        ans.put(i, hashTblColumns[leftAnswerColumns[i]], tupleIdx);
      }

      for (int i = 0; i < rightAnswerColumns.length; ++i) {
        ans.put(i + leftAnswerColumns.length, tbColumns.get(rightAnswerColumns[i]), row);
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
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic
   * works.
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
        } else if (left.eoi()) {
          left.setEOI(false);
          childrenEOI[0] = true;
        }
      }
      if (!right.eos() && !childrenEOI[1]) {
        TupleBatch tb = right.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, false);
        } else if (right.eoi()) {
          right.setEOI(false);
          childrenEOI[1] = true;
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
      nexttb = ans.popAny();
    }
    return nexttb;
  }

  @Override
  public void checkEOSAndEOI() {
    final Operator left = getLeft();
    final Operator right = getRight();

    if (left.eos() && right.eos() && ans.numTuples() == 0) {
      setEOS();
      return;
    }

    // EOS could be used as an EOI
    if ((childrenEOI[0] || left.eos()) && (childrenEOI[1] || right.eos()) && ans.numTuples() == 0) {
      setEOI(true);
      Arrays.fill(childrenEOI, false);
    }
  }

  /**
   * Recording the EOI status of the children.
   */
  private final boolean[] childrenEOI = new boolean[2];

  /**
   * consume EOI from Child 1. reset the child's EOI to false 2. record the EOI in childrenEOI[]
   * 
   * @param fromLeft true if consuming eoi from left child, false if consuming eoi from right child
   */
  private void consumeChildEOI(final boolean fromLeft) {
    final Operator left = getLeft();
    final Operator right = getRight();
    if (fromLeft) {
      Preconditions.checkArgument(left.eoi());
      left.setEOI(false);
      childrenEOI[0] = true;
    } else {
      Preconditions.checkArgument(right.eoi());
      right.setEOI(false);
      childrenEOI[1] = true;
    }
  }

  /**
   * Note: If this operator is ready for EOS, this function will return true since EOS is a special
   * EOI.
   * 
   * @return whether this operator is ready to set itself EOI
   */
  private boolean isEOIReady() {
    if ((childrenEOI[0] || getLeft().eos()) && (childrenEOI[1] || getRight().eos())) {
      return true;
    }
    return false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (!nonBlocking) {
      return fetchNextReadySynchronousEOI();
    }

    if (order.equals(JoinPullOrder.LEFT) || order.equals(JoinPullOrder.LEFT_EOS)) {
      pollLeft = true;
    } else if (order.equals(JoinPullOrder.RIGHT) || order.equals(JoinPullOrder.RIGHT_EOS)) {
      pollLeft = false;
    }

    /* If any full tuple batches are ready, output them. */
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    }

    /*
     * if both children are eos or both children have recorded eoi, pop any tuples in buffer. If the
     * buffer is empty, set EOS or EOI.
     */
    if (isEOIReady()) {
      nexttb = ans.popAny();
      if (nexttb == null) {
        checkEOSAndEOI();
      }
      return nexttb;
    }

    final Operator left = getLeft();
    final Operator right = getRight();
    int noDataStreak = 0;
    while (noDataStreak < 2 && (!left.eos() || !right.eos())) {

      Operator current;
      if (pollLeft) {
        current = left;
      } else {
        current = right;
      }

      /* process tuple from child */
      TupleBatch tb = current.nextReady();
      if (tb != null) {
        processChildTB(tb, pollLeft);
        noDataStreak = 0;
        /* ALTER: switch to the other child */
        if (order.equals(JoinPullOrder.ALTER)) {
          pollLeft = !pollLeft;
        }
        nexttb = ans.popAnyUsingTimeout();
        if (nexttb != null) {
          return nexttb;
        }
      } else if (current.eoi()) {
        /* if current operator is eoi, consume it, check whether it will cause EOI of this operator */
        consumeChildEOI(pollLeft);
        noDataStreak = 0;
        if (order.equals(JoinPullOrder.ALTER)) {
          pollLeft = !pollLeft;
        }
        /*
         * If this operator is ready to emit EOI (reminder that it needs to clear buffer), break to
         * EOI handle part
         */
        if (isEOIReady()) {
          break;
        }
      } else {
        if ((pollLeft && order.equals(JoinPullOrder.LEFT_EOS))
            || (!pollLeft && order.equals(JoinPullOrder.RIGHT_EOS))) {
          if (!current.eos()) {
            break;
          }
        }
        /* current.eos() or no data, switch to the other child */
        pollLeft = !pollLeft;
        noDataStreak++;
      }
    }

    /*
     * If the operator is ready to emit EOI, empty its output buffer first. If the buffer is already
     * empty, set EOI and/or EOS
     */
    if (isEOIReady()) {
      nexttb = ans.popAny();
      if (nexttb == null) {
        checkEOSAndEOI();
      }
    }
    return nexttb;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();
    leftHashTableIndices = new IntObjectHashMap<IntArrayList>();
    rightHashTableIndices = new IntObjectHashMap<IntArrayList>();

    hashTable1 = new MutableTupleBuffer(left.getSchema());
    hashTable2 = new MutableTupleBuffer(right.getSchema());

    ans = new TupleBatchBuffer(getSchema());

    nonBlocking =
        (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE) == QueryExecutionMode.NON_BLOCKING;
    doJoin = new JoinProcedure();
    doReplace = new ReplaceProcedure();
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
       * delete right child's hash table if the left child is EOS, since there will be no incoming
       * tuples from right as it will never be probed again.
       */
      rightHashTableIndices = null;
      hashTable2 = null;
    }
    if (right.eos() && leftHashTableIndices != null) {
      /*
       * delete left child's hash table if the right child is EOS, since there will be no incoming
       * tuples from left as it will never be probed again.
       */
      leftHashTableIndices = null;
      hashTable1 = null;
    }

    final boolean useSetSemantics = fromLeft && setSemanticsLeft || !fromLeft && setSemanticsRight;
    MutableTupleBuffer hashTable1Local = null;
    IntObjectHashMap<IntArrayList> hashTable1IndicesLocal = null;
    IntObjectHashMap<IntArrayList> hashTable2IndicesLocal = null;
    if (fromLeft) {
      hashTable1Local = hashTable1;
      doJoin.joinAgainstHashTable = hashTable2;
      hashTable1IndicesLocal = leftHashTableIndices;
      hashTable2IndicesLocal = rightHashTableIndices;
      doJoin.inputCmpColumns = leftCompareIndx;
      doJoin.joinAgainstCmpColumns = rightCompareIndx;
      if (useSetSemantics) {
        doReplace.hashTable = hashTable1;
        doReplace.keyColumns = leftCompareIndx;
      }
    } else {
      hashTable1Local = hashTable2;
      doJoin.joinAgainstHashTable = hashTable1;
      hashTable1IndicesLocal = rightHashTableIndices;
      hashTable2IndicesLocal = leftHashTableIndices;
      doJoin.inputCmpColumns = rightCompareIndx;
      doJoin.joinAgainstCmpColumns = leftCompareIndx;
      if (useSetSemantics) {
        doReplace.hashTable = hashTable2;
        doReplace.keyColumns = rightCompareIndx;
      }
    }
    doJoin.fromLeft = fromLeft;
    doJoin.inputTB = tb;
    if (useSetSemantics) {
      doReplace.inputTB = tb;
    }

    for (int row = 0; row < tb.numTuples(); ++row) {
      final int cntHashCode = HashUtils.hashSubRow(tb, doJoin.inputCmpColumns, row);
      IntArrayList tuplesWithHashCode = hashTable2IndicesLocal.get(cntHashCode);
      if (tuplesWithHashCode != null) {
        doJoin.row = row;
        tuplesWithHashCode.forEach(doJoin);
      }

      if (hashTable1Local != null) {
        // only build hash table on two sides if none of the children is EOS
        addToHashTable(tb, row, hashTable1Local, hashTable1IndicesLocal, cntHashCode,
            useSetSemantics);
      }
    }
  }

  /**
   * @param tb the source TupleBatch
   * @param row the row number to get added to hash table
   * @param hashTable the target hash table
   * @param hashTable1IndicesLocal hash table 1 indices local
   * @param hashCode the hashCode of the tb.
   * @param useSetSemantics if need to update the hash table using set semantics.
   * */
  private void addToHashTable(final TupleBatch tb, final int row,
      final MutableTupleBuffer hashTable,
      final IntObjectHashMap<IntArrayList> hashTable1IndicesLocal, final int hashCode,
      final boolean useSetSemantics) {

    final int nextIndex = hashTable.numTuples();
    IntArrayList tupleIndicesList = hashTable1IndicesLocal.get(hashCode);
    if (tupleIndicesList == null) {
      tupleIndicesList = new IntArrayList(1);
      hashTable1IndicesLocal.put(hashCode, tupleIndicesList);
    }

    doReplace.replaced = false;
    if (useSetSemantics) {
      doReplace.row = row;
      tupleIndicesList.forEach(doReplace);
    }
    if (!doReplace.replaced) {
      /* not using set semantics || using set semantics but found nothing to replace (i.e. new) */
      tupleIndicesList.add(nextIndex);
      List<? extends Column<?>> inputColumns = tb.getDataColumns();
      for (int column = 0; column < tb.numColumns(); column++) {
        hashTable.put(column, inputColumns.get(column), row);
      }
    }
  }

  /**
   * @return the sum of the numbers of tuples in both hash tables.
   */
  public long getNumTuplesInHashTables() {
    long sum = 0;
    if (hashTable1 != null) {
      sum += hashTable1.numTuples();
    }
    if (hashTable2 != null) {
      sum += hashTable2.numTuples();
    }
    return sum;
  }

  /** Join pull order options. */
  public enum JoinPullOrder {
    /** Alternatively. */
    ALTER,
    /** Pull from the left child whenever there is data available. */
    LEFT,
    /** Pull from the right child whenever there is data available. */
    RIGHT,
    /** Pull from the left child until it reaches EOS. */
    LEFT_EOS,
    /** Pull from the right child until it reaches EOS. */
    RIGHT_EOS
  }

  /**
   * Set the pull order.
   * 
   * @param order the pull order.
   */
  public void setPullOrder(final JoinPullOrder order) {
    this.order = order;
  }
}
