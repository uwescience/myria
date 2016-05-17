package edu.washington.escience.myria.operator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * This is an implementation of merge join that requires the tuples from the children to come in order.
 *
 * The data is buffered in a linked list for each child operator. There is a main index on each list that indicates up
 * to where we have advanced in the last tuple batch. Also, there is a second index that points to the last tuple that
 * is equal. This index is valid in the first tuple batch in the linked list.
 */
public final class MergeJoin extends BinaryOperator {
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
   * True if column is sorted ascending in {@link #leftCompareIndx} and {@link MergeJoin#rightCompareIndx}.
   */
  private boolean[] ascending;

  /**
   * The tuples from the left.
   */
  private transient LinkedList<TupleBatch> leftBatches;

  /**
   * The tuples from the right.
   */
  private transient LinkedList<TupleBatch> rightBatches;

  /**
   * Location of reader in left batch.
   *
   * Index is in last batch.
   */
  private int leftRowIndex;

  /**
   * Index of first row that is equal to the one in {@link #leftRowIndex}.
   *
   * Index is in 0th batch.
   *
   */
  private int leftBeginIndex;

  /**
   * A tuple batch that goes into {@link #leftBatches} but does not fit yet because {@link #leftRowIndex} should always
   * point into the last TB in {@link #leftBatches}.
   */
  private TupleBatch leftNotProcessed;

  /**
   * Location of reader in right batch.
   *
   * Index is in last batch.
   */
  private int rightRowIndex;

  /**
   * Index of first row that is equal to the one in {@link #rightRowIndex}.
   */
  private int rightBeginIndex;

  /**
   * A tuple batch that goes into {@link #rightBatches} but does not fit yet because {@link #rightRowIndex} should
   * always point into the last TB in {@link #rightBatches}.
   */
  private TupleBatch rightNotProcessed;

  /**
   * The buffer holding the results.
   */
  private transient TupleBatchBuffer ans;

  /** Which columns in the left child are to be output. */
  private final int[] leftAnswerColumns;
  /** Which columns in the right child are to be output. */
  private final int[] rightAnswerColumns;

  /**
   * Enum for return values from methods that advance indexes after a join.
   */
  private enum AdvanceResult {
    /** Could advance, no need to try other one. **/
    OK,
    /** Could not advance because the next tuple is not equal to the current one on this side. **/
    NOT_EQUAL,
    /**
     * Could not advance because we don't have enough information available (but we tried to get it). Could be because
     * of EOS.
     **/
    NOT_ENOUGH_DATA,
    /** Nothing has been returned because method has not been called. **/
    INVALID
  }

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   *
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param ascending true for each column that is ordered ascending in left child
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public MergeJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final boolean[] ascending) {
    this(null, left, right, compareIndx1, compareIndx2, ascending);
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
   * @param ascending true for each column that is ordered ascending in left child
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public MergeJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final int[] answerColumns1,
      final int[] answerColumns2,
      final boolean[] ascending) {
    this(null, left, right, compareIndx1, compareIndx2, answerColumns1, answerColumns2, ascending);
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
   * @param ascending true for each column that is ordered ascending in left child
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputColumns</tt>, or if
   *        <tt>outputColumns</tt> does not have the correct number of columns and column types.
   */
  public MergeJoin(
      final List<String> outputColumns,
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final int[] answerColumns1,
      final int[] answerColumns2,
      final boolean[] ascending) {
    super(left, right);
    Preconditions.checkArgument(compareIndx1.length == compareIndx2.length);
    if (outputColumns != null) {
      Preconditions.checkArgument(
          outputColumns.size() == answerColumns1.length + answerColumns2.length,
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

    this.ascending = ascending;
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
   * @param ascending true for each column that is ordered ascending in left child
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public MergeJoin(
      final List<String> outputColumns,
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final boolean[] ascending) {
    this(
        outputColumns,
        left,
        right,
        compareIndx1,
        compareIndx2,
        range(left.getSchema().numColumns()),
        range(right.getSchema().numColumns()),
        ascending);
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
   * Insert a new tuple into {@link #ans}.
   *
   * @param leftTb left TB
   * @param leftRow in left TB
   * @param rightTb right TB
   * @param rightRow in the right TB
   */
  protected void addToAns(
      final TupleBatch leftTb, final int leftRow, final TupleBatch rightTb, final int rightRow) {
    Preconditions.checkArgument(
        TupleUtils.tupleCompare(
                leftTb, leftCompareIndx, leftRow, rightTb, rightCompareIndx, rightRow, ascending)
            == 0);

    for (int i = 0; i < leftAnswerColumns.length; ++i) {
      ans.put(i, leftTb.getDataColumns().get(leftAnswerColumns[i]), leftRow);
    }
    for (int i = 0; i < rightAnswerColumns.length; ++i) {
      ans.put(
          i + leftAnswerColumns.length,
          rightTb.getDataColumns().get(rightAnswerColumns[i]),
          rightRow);
    }
  }

  @Override
  protected void cleanup() throws DbException {
    ans = null;
    leftBatches.clear();
    rightBatches.clear();
    leftNotProcessed = null;
    rightNotProcessed = null;
  }

  /**
   * True if a join tuple has been created for the tuples that {@link #leftRowIndex} points to.
   */
  private boolean joinedLeft;
  /**
   * True if a join tuple has been created for the tuples that {@link #rightRowIndex} points to.
   */
  private boolean joinedRight;

  /**
   * Set EOS the next time null is returned from {@link #fetchNextReady()}.
   */
  private boolean deferredEOS;

  /**
   * Set to true if data is needed from children and we cannot proceed without it.
   */
  private boolean needData;

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    /* If any full tuple batches are ready, output them. */

    TupleBatch nexttb = ans.popAnyUsingTimeout();
    if (nexttb != null) {
      return nexttb;
    }

    loadInitially();
    if (leftBatches.size() == 0 || rightBatches.size() == 0) {
      return null;
    }

    needData = false;

    while (!deferredEOS && !needData) {
      final int compared =
          TupleUtils.tupleCompare(
              leftBatches.getLast(),
              leftCompareIndx,
              leftRowIndex,
              rightBatches.getLast(),
              rightCompareIndx,
              rightRowIndex,
              ascending);

      if (compared == 0) {
        Preconditions.checkState(
            TupleUtils.tupleCompare(
                    leftBatches.getLast(),
                    leftCompareIndx,
                    leftRowIndex,
                    rightBatches.getLast(),
                    rightCompareIndx,
                    rightRowIndex,
                    ascending)
                == 0);
        Preconditions.checkState(
            TupleUtils.tupleCompare(
                    leftBatches.getFirst(),
                    leftCompareIndx,
                    leftBeginIndex,
                    rightBatches.getFirst(),
                    rightCompareIndx,
                    rightBeginIndex,
                    ascending)
                == 0);
        leftAndRightEqual();
      } else if (compared > 0) {
        rightIsLess();
      } else {
        leftIsLess();
      }
      nexttb = ans.popFilled();
      if (nexttb != null) {
        return nexttb;
      }
    }

    Preconditions.checkState(
        deferredEOS || !getLeft().eos() || !getRight().eos(),
        "deferredEOS should be true if all children are eos");

    if (deferredEOS) {
      nexttb = ans.popAny();
    }

    return nexttb;
  }

  @Override
  public void checkEOSAndEOI() {
    if (deferredEOS && ans.numTuples() == 0) {
      setEOS();
      return;
    }
  }

  /**
   * @throws Exception if any error occurs
   */
  private void leftAndRightEqual() throws Exception {
    final Operator left = getLeft();
    final Operator right = getRight();

    if (!joinedLeft & !joinedRight) {
      addAllToAns(
          leftBatches.getLast(),
          rightBatches,
          leftRowIndex,
          leftCompareIndx,
          rightBeginIndex,
          rightRowIndex);
      joinedLeft = true;
      joinedRight = true;
    } else if (!joinedLeft) {
      addAllToAns(
          leftBatches.getLast(),
          rightBatches,
          leftRowIndex,
          leftCompareIndx,
          rightBeginIndex,
          rightRowIndex);
      joinedLeft = true;
    } else if (!joinedRight) {
      addAllToAns(
          rightBatches.getLast(),
          leftBatches,
          rightRowIndex,
          rightCompareIndx,
          leftBeginIndex,
          leftRowIndex);
      joinedRight = true;
    }

    // advance the one with the larger set of equal tuples because this produces fewer join tuples
    // not exact but good approximation
    final int leftSizeOfGroupOfEqualTuples =
        leftRowIndex + TupleBatch.BATCH_SIZE * (leftBatches.size() - 1) - leftBeginIndex;
    final int rightSizeOfGroupOfEqualTuples =
        rightRowIndex + TupleBatch.BATCH_SIZE * (rightBatches.size() - 1) - rightBeginIndex;
    final boolean advanceLeftFirst = leftSizeOfGroupOfEqualTuples > rightSizeOfGroupOfEqualTuples;

    AdvanceResult r1, r2 = AdvanceResult.INVALID;
    if (advanceLeftFirst) {
      r1 = advanceLeft();
      if (r1 != AdvanceResult.OK) {
        r2 = advanceRight();
      }
    } else {
      r1 = advanceRight();
      if (r1 != AdvanceResult.OK) {
        r2 = advanceLeft();
      }
    }

    if (r1 != AdvanceResult.OK && r2 != AdvanceResult.OK) {
      Operator child1, child2;
      if (advanceLeftFirst) {
        child1 = left;
        child2 = right;
      } else {
        child1 = right;
        child2 = left;
      }

      Preconditions.checkState(r2 != AdvanceResult.INVALID);
      if (r1 == AdvanceResult.NOT_EQUAL && r2 == AdvanceResult.NOT_EQUAL) {
        // We know that we do not need to join anything anymore so we can advance both sides.
        // This cannot be done earlier because we need information about both sides.
        final boolean leftAtLast = leftRowIndex == leftBatches.getLast().numTuples() - 1;
        if (leftAtLast) {
          Preconditions.checkState(
              leftNotProcessed != null, "Buffered TB should be ensured in advance.");
          leftMoveFromNotProcessed();
        } else {
          while (leftBatches.size() > 1) {
            leftBatches.removeFirst();
          }
          leftRowIndex++;
        }
        leftBeginIndex = leftRowIndex;

        final boolean rightAtLast = rightRowIndex == rightBatches.getLast().numTuples() - 1;
        if (rightAtLast) {
          Preconditions.checkState(
              rightNotProcessed != null, "Buffered TB should be ensured in advance.");
          rightMoveFromNotProcessed();
        } else {
          while (rightBatches.size() > 1) {
            rightBatches.removeFirst();
          }
          rightRowIndex++;
        }
        rightBeginIndex = rightRowIndex;

        joinedLeft = false;
        joinedRight = false;
      } else if (r1 == AdvanceResult.NOT_EQUAL && child2.eos()
          || r2 == AdvanceResult.NOT_EQUAL && child1.eos()
          || left.eos() && right.eos()) {
        deferredEOS = true;
      } else {
        Preconditions.checkState(
            r1 == AdvanceResult.NOT_ENOUGH_DATA || r2 == AdvanceResult.NOT_ENOUGH_DATA);
        needData = true;
      }
    }
  }

  /**
   * @throws DbException if any problem in {@link Operator.#nextReady()}.
   */
  private void leftIsLess() throws DbException {
    final Operator left = getLeft();

    final boolean atLast = leftRowIndex == leftBatches.getLast().numTuples() - 1;
    if (atLast) {
      if (leftNotProcessed != null) {
        leftMoveFromNotProcessed();
      } else {
        if (!left.eos()) {
          TupleBatch tb = left.nextReady();
          if (tb != null) {
            leftNotProcessed = tb;
          }
        }
        if (left.eos()) {
          deferredEOS = true;
        }
      }
    } else {
      leftRowIndex++;
    }
    leftBeginIndex = leftRowIndex;
  }

  /**
   * @throws DbException if any problem in {@link Operator.#nextReady()}.
   */
  private void rightIsLess() throws DbException {
    final Operator right = getRight();

    final boolean atLast = rightRowIndex == rightBatches.getLast().numTuples() - 1;
    if (atLast) {
      if (rightNotProcessed != null) {
        rightMoveFromNotProcessed();
      } else {
        if (!right.eos()) {
          TupleBatch tb = right.nextReady();
          if (tb != null) {
            rightNotProcessed = tb;
          }
        }
        if (right.eos()) {
          deferredEOS = true;
        }
      }
    } else {
      rightRowIndex++;
    }
    rightBeginIndex = rightRowIndex;
  }

  /**
   * Add {@link #leftNotProcessed} into {@link #leftBatches}.
   */
  private void leftMoveFromNotProcessed() {
    Preconditions.checkNotNull(leftNotProcessed);
    leftRowIndex = 0;
    leftBatches.clear();
    leftBatches.add(leftNotProcessed);
    leftNotProcessed = null;
  }

  /**
   * Add {@link #rightNotProcessed} into {@link #rightBatches}.
   */
  private void rightMoveFromNotProcessed() {
    Preconditions.checkNotNull(rightNotProcessed);
    rightRowIndex = 0;
    rightBatches.clear();
    rightBatches.add(rightNotProcessed);
    rightNotProcessed = null;
  }

  /**
   * Establishes invariant that buffers have at least one batch.
   *
   * @throws DbException if any problem on fetching
   */
  private void loadInitially() throws DbException {
    final Operator left = getLeft();
    final Operator right = getRight();

    if (leftBatches.isEmpty() && !left.eos()) {
      TupleBatch tb = left.nextReady();
      if (tb != null) {
        leftBatches.add(tb);
      }
    }

    if (rightBatches.isEmpty() && !right.eos()) {
      TupleBatch tb = right.nextReady();
      if (tb != null) {
        rightBatches.add(tb);
      }
    }
  }

  /**
   * @return {@link AdvanceResult.OK} if we could advance
   * @throws Exception if any error occurs
   */
  protected AdvanceResult advanceLeft() throws Exception {
    final Operator left = getLeft();
    final boolean atLast = leftRowIndex == leftBatches.getLast().numTuples() - 1;
    if (atLast) {
      if (!left.eos() && leftNotProcessed == null) {
        TupleBatch tb = left.nextReady();
        if (tb != null) {
          leftNotProcessed = tb;
        }
      }

      if (leftNotProcessed != null) {
        if (TupleUtils.tupleCompare(
                leftBatches.getLast(),
                leftCompareIndx,
                leftRowIndex,
                leftNotProcessed,
                leftCompareIndx,
                0,
                ascending)
            == 0) {
          leftBatches.add(leftNotProcessed);
          leftNotProcessed = null;
          leftRowIndex = 0;
          joinedLeft = false;
          return AdvanceResult.OK;
        } else {
          return AdvanceResult.NOT_EQUAL;
        }
      } else {
        return AdvanceResult.NOT_ENOUGH_DATA;
      }
    } else if (TupleUtils.tupleCompare(
            leftBatches.getLast(), leftCompareIndx, leftRowIndex, leftRowIndex + 1, ascending)
        == 0) {
      leftRowIndex++;
      joinedLeft = false;
      return AdvanceResult.OK;
    } else {
      return AdvanceResult.NOT_EQUAL;
    }
  }

  /**
   * @return {@link AdvanceResult.OK} if we could advance
   * @throws Exception if any error occurs
   */
  protected AdvanceResult advanceRight() throws Exception {
    final Operator right = getRight();
    final boolean atLast = rightRowIndex == rightBatches.getLast().numTuples() - 1;
    if (atLast) {
      if (!right.eos() && rightNotProcessed == null) {
        TupleBatch tb = right.nextReady();
        if (tb != null) {
          rightNotProcessed = tb;
        }
      }

      if (rightNotProcessed != null) {
        if (TupleUtils.tupleCompare(
                rightBatches.getLast(),
                rightCompareIndx,
                rightRowIndex,
                rightNotProcessed,
                rightCompareIndx,
                0,
                ascending)
            == 0) {
          rightBatches.add(rightNotProcessed);
          rightNotProcessed = null;
          rightRowIndex = 0;
          joinedRight = false;
          return AdvanceResult.OK;
        } else {
          return AdvanceResult.NOT_EQUAL;
        }
      } else {
        return AdvanceResult.NOT_ENOUGH_DATA;
      }
    } else if (TupleUtils.tupleCompare(
            rightBatches.getLast(), rightCompareIndx, rightRowIndex, rightRowIndex + 1, ascending)
        == 0) {
      rightRowIndex++;
      joinedRight = false;
      return AdvanceResult.OK;
    } else {
      return AdvanceResult.NOT_EQUAL;
    }
  }

  /**
   * Joins tuple from first batch with all tuples from the second batch between start and end.
   *
   * @param firstBatch the batch that has one tuple to be joined with n others
   * @param secondBatches the batches that have n tuples to joined with one
   * @param firstBatchRow the row in which we can find the tuple to join n other with
   * @param firstCompareIndx the compare index in the first TB. Used to determine whether the next tuple is equal to the
   *          one under the current index.
   * @param secondBeginRow the start of the n tuples (points into first TB in linked list)
   * @param secondEndRow the end of the n tuples (points into last TB in linked list)
   */
  protected void addAllToAns(
      final TupleBatch firstBatch,
      final LinkedList<TupleBatch> secondBatches,
      final int firstBatchRow,
      final int[] firstCompareIndx,
      final int secondBeginRow,
      final int secondEndRow) {
    int beginIndex = secondBeginRow;

    Preconditions.checkArgument(firstBatch != secondBatches.getLast());

    final boolean leftIsFirst = firstBatch == leftBatches.getLast();

    Iterator<TupleBatch> it = secondBatches.iterator();
    while (it.hasNext()) {
      TupleBatch tb = it.next();
      // in the last TB we only want to go till we hit the last processed tuple
      int endIndex = tb.numTuples();
      if (!it.hasNext()) {
        endIndex = secondEndRow + 1;
      }
      for (int i = beginIndex; i < endIndex; i++) {
        if (leftIsFirst) {
          addToAns(firstBatch, firstBatchRow, tb, i);
        } else {
          addToAns(tb, i, firstBatch, firstBatchRow);
        }
      }
      beginIndex = 0;
    }
  }

  /**
   * @param ascending whether the values in the compare index are sorted ascending.
   */
  public void setAscending(final boolean[] ascending) {
    this.ascending = ascending;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    Preconditions.checkArgument(ascending.length == leftCompareIndx.length);
    Preconditions.checkArgument(ascending.length == rightCompareIndx.length);

    leftRowIndex = 0;
    rightRowIndex = 0;
    leftBeginIndex = 0;
    rightBeginIndex = 0;

    deferredEOS = false;

    joinedLeft = false;
    joinedRight = false;

    leftNotProcessed = null;
    rightNotProcessed = null;

    leftBatches = new LinkedList<TupleBatch>();
    rightBatches = new LinkedList<TupleBatch>();

    ans = new TupleBatchBuffer(getSchema());
  }
}
