package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;
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
  private final boolean[] ascending;

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
  enum AdvanceResult {
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
  public MergeJoin(final Operator left, final Operator right, final int[] compareIndx1, final int[] compareIndx2,
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
  public MergeJoin(final Operator left, final Operator right, final int[] compareIndx1, final int[] compareIndx2,
      final int[] answerColumns1, final int[] answerColumns2, final boolean[] ascending) {
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
  public MergeJoin(final List<String> outputColumns, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2,
      final boolean[] ascending) {
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

    this.ascending = ascending;

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
   * @param ascending true for each column that is ordered ascending in left child
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public MergeJoin(final List<String> outputColumns, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2, final boolean[] ascending) {
    this(outputColumns, left, right, compareIndx1, compareIndx2, range(left.getSchema().numColumns()), range(right
        .getSchema().numColumns()), ascending);
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
  protected void addToAns(final TupleBatch leftTb, final int leftRow, final TupleBatch rightTb, final int rightRow) {
    Preconditions.checkArgument(leftTb.tupleCompare(leftCompareIndx, leftRow, rightTb, rightCompareIndx, rightRow,
        ascending) == 0);

    // System.out.println("=> Add join tuple from " + leftRow + " and " + rightRow);
    final int leftRowInColumn = leftTb.getValidIndices().get(leftRow);
    final int rightRowInColumn = rightTb.getValidIndices().get(rightRow);

    for (int i = 0; i < leftAnswerColumns.length; ++i) {
      ans.put(i, leftTb.getDataColumns().get(leftAnswerColumns[i]), leftRowInColumn);
    }
    for (int i = 0; i < rightAnswerColumns.length; ++i) {
      ans.put(i + leftAnswerColumns.length, rightTb.getDataColumns().get(rightAnswerColumns[i]), rightRowInColumn);
    }
  }

  @Override
  protected void cleanup() throws DbException {
    ans = null;
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
   * True if a join tuple has been created for the tuples that {@link #leftRowIndex} and {@link #rightRowIndex} point
   * to.
   */
  private boolean joined;

  /**
   * Note: If this operator is ready for EOS, this function will return true since EOS is a special EOI.
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
  protected TupleBatch fetchNextReady() throws Exception {
    /* If any full tuple batches are ready, output them. */
    TupleBatch nexttb = ans.popAnyUsingTimeout();
    if (nexttb != null) {
      return nexttb;
    }

    /* Load data into buffers initially */
    if (leftBatches.isEmpty() && !getLeft().eos()) {
      TupleBatch tb = getLeft().fetchNextReady();
      if (tb == null) {
        return null;
      }
      leftBatches.add(tb);
    }

    if (rightBatches.isEmpty() && !getRight().eos()) {
      TupleBatch tb = getRight().nextReady();
      if (tb == null) {
        return null;
      }
      rightBatches.add(tb);
    }

    while (nexttb == null && !eos()) {
      // System.out.println("Indexes " + leftBatches.getLast().getLong(0, leftRowIndex) + " "
      // + rightBatches.getLast().getLong(0, rightRowIndex));
      final int compared =
          leftBatches.getLast().tupleCompare(leftCompareIndx, leftRowIndex, rightBatches.getLast(), rightCompareIndx,
              rightRowIndex, ascending);

      if (compared == 0) {
        // advance the one with the larger set of equal tuples because this produces fewer join tuples
        // not exact but good approximation
        final int leftSizeOfGroupOfEqualTuples =
            leftRowIndex + TupleBatch.BATCH_SIZE * (leftBatches.size() - 1) - leftBeginIndex;
        final int rightSizeOfGroupOfEqualTuples =
            rightRowIndex + TupleBatch.BATCH_SIZE * (rightBatches.size() - 1) - rightBeginIndex;
        final boolean joinFromLeft = leftSizeOfGroupOfEqualTuples > rightSizeOfGroupOfEqualTuples;

        if (!joined) {
          if (joinFromLeft) {
            TupleBatch firstBatch = leftBatches.getLast();
            LinkedList<TupleBatch> secondBatches = rightBatches;
            int firstBatchRow = leftRowIndex;
            int[] firstCompareIndx = leftCompareIndx;
            int secondBeginRow = rightBeginIndex;
            int secondEndRow = rightRowIndex;
            addAllToAns(firstBatch, secondBatches, firstBatchRow, firstCompareIndx, secondBeginRow, secondEndRow);
          } else {
            TupleBatch firstBatch = rightBatches.getLast();
            LinkedList<TupleBatch> secondBatches = leftBatches;
            int firstBatchRow = rightRowIndex;
            int[] firstCompareIndx = rightCompareIndx;
            int secondBeginRow = leftBeginIndex;
            int secondEndRow = leftRowIndex;
            addAllToAns(firstBatch, secondBatches, firstBatchRow, firstCompareIndx, secondBeginRow, secondEndRow);
          }
          joined = true;
        }

        final boolean advanceLeftFirst = joinFromLeft;

        AdvanceResult r1, r2 = AdvanceResult.INVALID;
        if (advanceLeftFirst) {
          // System.out.println("Try left");
          r1 = advanceLeft();
          if (r1 != AdvanceResult.OK) {
            r2 = advanceRight();
          }
        } else {
          // System.out.println("Try right");
          r1 = advanceRight();
          if (r1 != AdvanceResult.OK) {
            r2 = advanceLeft();
          }
        }

        // System.out.println("Results " + r1 + " " + r2);

        if (r1 != AdvanceResult.OK && r2 != AdvanceResult.OK) {

          Operator child1, child2;
          if (advanceLeftFirst) {
            child1 = getLeft();
            child2 = getRight();
          } else {
            child1 = getRight();
            child2 = getLeft();
          }

          Preconditions.checkState(r2 != AdvanceResult.INVALID);
          if (r1 == AdvanceResult.NOT_EQUAL && r2 == AdvanceResult.NOT_EQUAL) {
            // We know that we do not need to join anything anymore so we can advance both sides.
            // This cannot be done earlier because we need information about both sides.

            final boolean leftAtLast = leftRowIndex == leftBatches.getLast().numTuples() - 1;
            if (leftAtLast) {
              Preconditions.checkState(leftNotProcessed != null);
              leftBatches.clear();
              leftBatches.add(leftNotProcessed);
              leftNotProcessed = null;
              leftRowIndex = 0;
            } else {
              leftRowIndex++;
            }
            leftBeginIndex = leftRowIndex;

            final boolean rightAtLast = rightRowIndex == rightBatches.getLast().numTuples() - 1;
            if (rightAtLast) {
              Preconditions.checkState(rightNotProcessed != null);
              rightBatches.clear();
              rightBatches.add(rightNotProcessed);
              rightNotProcessed = null;
              rightRowIndex = 0;
            } else {
              rightRowIndex++;
            }
            rightBeginIndex = rightRowIndex;

            joined = false;
          } else if (r1 == AdvanceResult.NOT_EQUAL && child2.eos() || r2 == AdvanceResult.NOT_EQUAL && child1.eos()
              || getLeft().eos() && getRight().eos()) {
            setEOS();
            break;
          } else if (r1 == AdvanceResult.NOT_ENOUGH_DATA || r2 == AdvanceResult.NOT_ENOUGH_DATA) {
            break;
          } else {
            throw new Exception("implement me");
          }
        }
      } else {
        if (compared > 0) {
          final boolean atLast = rightRowIndex == rightBatches.getLast().numTuples() - 1;
          if (atLast) {
            if (!getRight().eos() && rightNotProcessed == null) {
              TupleBatch tb = getRight().nextReady();
              if (tb != null) {
                rightNotProcessed = tb;
              }
            }

            if (rightNotProcessed != null) {
              rightRowIndex = 0;
              rightBatches.clear();
              rightBatches.add(rightNotProcessed);
              rightNotProcessed = null;
            } else if (getRight().eos()) {
              // System.err.println("Right set EOS");
              setEOS();
              break;
            } else {
              break;
            }
          } else {
            rightRowIndex++;
          }
          rightBeginIndex = rightRowIndex;
          System.out.println("Advance right to " + rightRowIndex);
        } else {
          final boolean atLast = leftRowIndex == leftBatches.getLast().numTuples() - 1;
          if (atLast) {
            if (!getLeft().eos() && leftNotProcessed == null) {
              TupleBatch tb = getLeft().fetchNextReady();
              if (tb != null) {
                leftNotProcessed = tb;
              }
            }

            if (leftNotProcessed != null) {
              leftRowIndex = 0;
              leftBatches.clear();
              leftBatches.add(leftNotProcessed);
              leftNotProcessed = null;
            } else if (getLeft().eos()) {
              // System.err.println("Left set EOS");
              setEOS();
              break;
            } else {
              break;
            }
          } else {
            leftRowIndex++;
          }
          leftBeginIndex = leftRowIndex;
          System.out.println("Advance left to " + leftRowIndex);
        }
      }
      nexttb = ans.popFilled();
    }

    if (eos()) {
      Preconditions.checkState(ans.numTuples() == 0 || nexttb == null);
      System.out.println("EOS " + getLeft().eos() + " " + getRight().eos());
      System.out.println("Indexes " + rightRowIndex + " " + leftRowIndex);
      System.out.println("Sizes " + rightBatches.size() + " " + leftBatches.size());
      Preconditions.checkState(leftNotProcessed == null);
      Preconditions.checkState(rightNotProcessed == null);

      nexttb = ans.popAny();
    }

    if (nexttb != null) {
      System.out.println("Size " + nexttb.numTuples());
    } else {
      System.out.println("Return null");
    }

    return nexttb;
  }

  /**
   * @return {@link AdvanceResult.OK} if we could advance
   * @throws Exception if any error occurs
   */
  protected AdvanceResult advanceLeft() throws Exception {
    final boolean atLast = leftRowIndex == leftBatches.getLast().numTuples() - 1;
    if (atLast) {
      // we might be able to get some information
      if (!getLeft().eos() && leftNotProcessed == null) {
        TupleBatch tb = getLeft().fetchNextReady();
        if (tb != null) {
          leftNotProcessed = tb;
        }
      }

      if (leftNotProcessed != null) {
        if (leftBatches.getLast().tupleCompare(leftCompareIndx, leftRowIndex, leftNotProcessed, leftCompareIndx, 0,
            ascending) == 0) {
          leftBatches.add(leftNotProcessed);
          leftNotProcessed = null;
          leftRowIndex = 0;
          joined = false;
          return AdvanceResult.OK;
        } else {
          return AdvanceResult.NOT_EQUAL;
        }
      } else {
        return AdvanceResult.NOT_ENOUGH_DATA;
      }
    } else if (leftBatches.getLast().tupleCompare(leftCompareIndx, leftRowIndex, leftRowIndex + 1, ascending) == 0) {
      leftRowIndex++;
      joined = false;
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
    final boolean atLast = rightRowIndex == rightBatches.getLast().numTuples() - 1;
    if (atLast) {
      // we might be able to get some information
      if (!getRight().eos() && rightNotProcessed == null) {
        TupleBatch tb = getRight().nextReady();
        if (tb != null) {
          rightNotProcessed = tb;
        }
      }

      if (rightNotProcessed != null) {
        if (rightBatches.getLast().tupleCompare(rightCompareIndx, rightRowIndex, rightNotProcessed, rightCompareIndx,
            0, ascending) == 0) {
          rightBatches.add(rightNotProcessed);
          rightNotProcessed = null;
          rightRowIndex = 0;
          joined = false;
          return AdvanceResult.OK;
        } else {
          return AdvanceResult.NOT_EQUAL;
        }
      } else {
        return AdvanceResult.NOT_ENOUGH_DATA;
      }
    } else if (rightBatches.getLast().tupleCompare(rightCompareIndx, rightRowIndex, rightRowIndex + 1, ascending) == 0) {
      rightRowIndex++;
      joined = false;
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
  protected void addAllToAns(final TupleBatch firstBatch, final LinkedList<TupleBatch> secondBatches,
      final int firstBatchRow, final int[] firstCompareIndx, final int secondBeginRow, final int secondEndRow) {

    // System.out.println("Join one at " + firstBatchRow + " with all of " + secondBeginRow + " to " + secondEndRow);

    int beginIndex = secondBeginRow;

    Iterator<TupleBatch> it = secondBatches.iterator();
    while (it.hasNext()) {
      TupleBatch tb = it.next();
      // in the last TB we only want to go till we hit the last processed tuple
      int endIndex = tb.numTuples();
      if (!it.hasNext()) {
        endIndex = secondEndRow + 1;
      }
      for (int i = beginIndex; i < endIndex; i++) {
        addToAns(firstBatch, firstBatchRow, tb, i);
      }
      beginIndex = 0;
    }
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    leftRowIndex = 0;
    rightRowIndex = 0;
    leftBeginIndex = 0;
    rightBeginIndex = 0;

    joined = false;

    leftNotProcessed = null;
    rightNotProcessed = null;

    leftBatches = new LinkedList<TupleBatch>();
    rightBatches = new LinkedList<TupleBatch>();

    ans = new TupleBatchBuffer(getSchema());
  }
}
