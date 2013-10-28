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
   * The buffer holding the results.
   */
  private transient TupleBatchBuffer ans;

  /** Which columns in the left child are to be output. */
  private final int[] leftAnswerColumns;
  /** Which columns in the right child are to be output. */
  private final int[] rightAnswerColumns;

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
    System.out.println("Add join tuple from " + leftRow + " and " + rightRow);
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
   * Note: If this operator is ready for EOS, this function will return true since EOS is a special EOI.
   * 
   * @return whether this operator is ready to set itself EOI
   */
  private boolean isEOIReady() {
    // we want or here because if one is finished, we are done
    if ((childrenEOI[0] || getLeft().eos()) || (childrenEOI[1] || getRight().eos())) {
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

    if (leftBatches.isEmpty() && !getLeft().eoi()) {
      TupleBatch tb = getLeft().fetchNextReady();
      if (tb != null) {
        leftBatches.add(tb);
      } else {
        return null;
      }
    }

    if (rightBatches.isEmpty() && !getRight().eos()) {
      TupleBatch tb = getRight().fetchNextReady();
      if (tb != null) {
        rightBatches.add(tb);
      } else {
        return null;
      }
    }

    while (nexttb == null) {
      System.out.println("==========");

      // Get data from children if possible (no eos) and necessary (we are at the end of the last TB)
      if (leftRowIndex == leftBatches.getLast().numTuples() - 1 && !getLeft().eos()) {
        TupleBatch tb = getLeft().fetchNextReady();
        if (tb != null) {
          leftBatches.add(tb);
          // TODO
        } else {
          System.out.println("Left is empty. eos: " + getLeft().eos());
          break;
        }
      }

      if (rightRowIndex == rightBatches.getLast().numTuples() - 1 && !getRight().eos()) {
        TupleBatch tb = getRight().fetchNextReady();
        if (tb != null) {
          rightBatches.add(tb);
          // TODO
        } else {
          System.out.println("Right is empty. eos: " + getRight().eos());
          break;
        }
      }

      System.out.println("Indexes " + leftBatches.getLast().getLong(0, leftRowIndex) + " "
          + rightBatches.getLast().getLong(0, rightRowIndex));

      final int compared =
          leftBatches.getLast().tupleCompare(leftCompareIndx, leftRowIndex, rightBatches.getLast(), rightCompareIndx,
              rightRowIndex, ascending);

      if (compared == 0) {
        System.out.println("Match");

        boolean advanceLeftFirst = rightRowIndex > leftRowIndex;

        boolean advanced = false;
        if (advanceLeftFirst) {
          advanced = advanceLeft();
          if (!advanced) {
            advanced = advanceRight();
          }
        } else {
          advanced = advanceRight();
          if (!advanced) {
            advanced = advanceLeft();
          }
        }

        if (!advanced) {
          // we merged all tuples with a certain value
          leftRowIndex++;
          rightRowIndex++;
          leftBeginIndex = leftRowIndex;
          rightBeginIndex = rightRowIndex;
          System.out.println("Advance left to " + leftRowIndex);
          System.out.println("Advance right to " + rightRowIndex);
          while (leftBatches.size() > 1) {
            leftBatches.removeFirst();
          }
          while (rightBatches.size() > 1) {
            rightBatches.removeFirst();
          }
        } else {
          System.out.println("We joined");
        }

      } else if (compared > 0) {
        rightRowIndex++;
        rightBeginIndex = rightRowIndex;
        System.out.println("Advance right to " + rightRowIndex);
      } else {
        leftRowIndex++;
        leftBeginIndex = leftRowIndex;
        System.out.println("Advance left to " + leftRowIndex);
      }

      nexttb = ans.popFilled();
    }

    if (isEOIReady()) {
      nexttb = ans.popAny();
      if (nexttb == null) {
        checkEOSAndEOI();
      }
    }

    return nexttb;
  }

  /**
   * @return true, if it could be advanced
   */
  protected boolean advanceLeft() {
    TupleBatch firstBatch = leftBatches.getLast();
    LinkedList<TupleBatch> secondBatches = rightBatches;
    Operator firstChild = getLeft();
    int firstBatchRow = leftRowIndex;
    int[] firstCompareIndx = leftCompareIndx;
    int secondBeginRow = rightBeginIndex;
    int secondEndRow = rightRowIndex;

    final int newLeftRowIndex =
        addAllToAns(firstBatch, secondBatches, firstChild, firstBatchRow, firstCompareIndx, secondBeginRow,
            secondEndRow);
    if (newLeftRowIndex != leftRowIndex) {
      leftRowIndex = newLeftRowIndex;
      System.out.println("Advance left to " + leftRowIndex);
      return true;
    }
    return false;
  }

  /**
   * @return true, if it could be advanced
   */
  protected boolean advanceRight() {
    TupleBatch firstBatch = rightBatches.getLast();
    LinkedList<TupleBatch> secondBatches = leftBatches;
    Operator firstChild = getRight();
    int firstBatchRow = rightRowIndex;
    int[] firstCompareIndx = rightCompareIndx;
    int secondBeginRow = leftBeginIndex;
    int secondEndRow = leftRowIndex;

    final int newRightRowIndex =
        addAllToAns(firstBatch, secondBatches, firstChild, firstBatchRow, firstCompareIndx, secondBeginRow,
            secondEndRow);
    if (newRightRowIndex != rightRowIndex) {
      rightRowIndex = newRightRowIndex;
      System.out.println("Advance right to " + rightRowIndex);
      return true;
    }
    return false;
  }

  /**
   * Joins tuple from first batch with all tuples from the second batch between start and end. This only joins, if the
   * row index can be advanced.
   * 
   * @param firstBatch the batch that has one tuple to be joined with n others
   * @param secondBatches the batches that have n tuples to joined with one
   * @param firstChild the first child
   * @param firstBatchRow the row in which we can find the tuple to join n other with
   * @param firstCompareIndx the compare index in the first TB. Used to determine whether the next tuple is equal to the
   *          one under the current index.
   * @param secondBeginRow the start of the n tuples (points into first TB in linked list)
   * @param secondEndRow the end of the n tuples (points into last TB in linked list)
   * @return new row index for first child
   */
  protected int addAllToAns(final TupleBatch firstBatch, final LinkedList<TupleBatch> secondBatches,
      final Operator firstChild, final int firstBatchRow, final int[] firstCompareIndx, final int secondBeginRow,
      final int secondEndRow) {

    System.out
        .println("Try to join one at " + firstBatchRow + " with all of " + secondBeginRow + " to " + secondEndRow);
    int newFirstBatchRow = firstBatchRow;

    if (firstBatchRow == firstBatch.numTuples() - 1) {
      System.out.println("At the end");
      return newFirstBatchRow;
    }

    if (!firstChild.eos()) {
      // if the next tuple is the same, we can advance and join
      int compare = firstBatch.tupleCompare(firstCompareIndx, firstBatchRow, firstBatchRow + 1, ascending);

      if (compare == 0) {
        System.out.println("Do join multiple.");
        newFirstBatchRow++;
        Iterator<TupleBatch> it = secondBatches.iterator();
        while (it.hasNext()) {
          TupleBatch tb = it.next();
          // in the last TB we only want to go till we hit the last processed tuple
          int endIndex = tb.numTuples();
          if (!it.hasNext()) {
            endIndex = secondEndRow + 1;
          }
          for (int i = secondBeginRow; i < endIndex; i++) {
            addToAns(firstBatch, firstBatchRow, secondBatches.getFirst(), i);
          }
        }
      }
    }

    return newFirstBatchRow;

  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    leftRowIndex = 0;
    rightRowIndex = 0;
    leftBeginIndex = 0;
    rightBeginIndex = 0;

    leftBatches = new LinkedList<TupleBatch>();
    rightBatches = new LinkedList<TupleBatch>();

    ans = new TupleBatchBuffer(getSchema());
  }
}
