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
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;
import edu.washington.escience.myria.util.MyriaArrayUtils;

/**
 * This is an implementation of unbalanced hash join. This operator only builds hash tables for its right child, thus
 * will begin to output tuples after right child EOS.
 *
 */
public final class RightHashJoin extends BinaryOperator {
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
   * A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code}
   */
  private transient IntObjectHashMap<IntArrayList> rightHashTableIndices;

  /**
   * The buffer holding the valid tuples from right.
   */
  private transient MutableTupleBuffer rightHashTable;
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

    /** serial version id. */
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

    @Override
    public void value(final int index) {
      if (TupleUtils.tupleEquals(
          inputTB, inputCmpColumns, row, joinAgainstHashTable, joinAgainstCmpColumns, index)) {
        addToAns(inputTB, row, joinAgainstHashTable, index);
      }
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
  public RightHashJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
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
  public RightHashJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final int[] answerColumns1,
      final int[] answerColumns2) {
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
  public RightHashJoin(
      final List<String> outputColumns,
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2,
      final int[] answerColumns1,
      final int[] answerColumns2) {
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
  public RightHashJoin(
      final List<String> outputColumns,
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2) {
    this(
        outputColumns,
        left,
        right,
        compareIndx1,
        compareIndx2,
        range(left.getSchema().numColumns()),
        range(right.getSchema().numColumns()));
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
      Preconditions.checkState(
          leftType == rightType,
          "column types do not match for join at index %s: left column type %s [%s] != right column type %s [%s]",
          i,
          leftIndex,
          leftType,
          rightIndex,
          rightType);
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
   */
  protected void addToAns(
      final TupleBatch cntTB, final int row, final MutableTupleBuffer hashTable, final int index) {
    List<? extends Column<?>> tbColumns = cntTB.getDataColumns();
    ReadableColumn[] hashTblColumns = hashTable.getColumns(index);
    int tupleIdx = hashTable.getTupleIndexInContainingTB(index);

    for (int i = 0; i < leftAnswerColumns.length; ++i) {
      ans.put(i, tbColumns.get(leftAnswerColumns[i]), row);
    }

    for (int i = 0; i < rightAnswerColumns.length; ++i) {
      ans.put(i + leftAnswerColumns.length, hashTblColumns[rightAnswerColumns[i]], tupleIdx);
    }
  }

  @Override
  protected void cleanup() throws DbException {
    rightHashTable = null;
    rightHashTableIndices = null;
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
    if ((childrenEOI[0] || getLeft().eos()) && (childrenEOI[1] || getRight().eos())) {
      return true;
    }
    return false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    /*
     * blocking mode will have the same logic
     */

    /* If any full tuple batches are ready, output them. */
    TupleBatch nexttb = ans.popAnyUsingTimeout();
    if (nexttb != null) {
      return nexttb;
    }

    final Operator right = getRight();

    /* Drain the right child. */
    while (!right.eos()) {
      TupleBatch rightTB = right.nextReady();
      if (rightTB == null) {
        /* The right child may have realized it's EOS now. If so, we must move onto left child to avoid livelock. */
        if (right.eos()) {
          break;
        }
        return null;
      }
      processRightChildTB(rightTB);
    }

    /* The right child is done, let's drain the left child. */
    final Operator left = getLeft();
    while (!left.eos()) {
      TupleBatch leftTB = left.nextReady();
      /*
       * Left tuple has no data, but we may need to pop partially-full existing batches if left reached EOI/EOS. Break
       * and check for termination.
       */
      if (leftTB == null) {
        break;
      }

      /* Process the data and add new results to ans. */
      processLeftChildTB(leftTB);

      nexttb = ans.popAnyUsingTimeout();
      if (nexttb != null) {
        return nexttb;
      }
      /*
       * We didn't time out or there is no data in ans, and there are no full tuple batches. Either way, check for more
       * data.
       */
    }

    if (isEOIReady()) {
      nexttb = ans.popAny();
    }

    return nexttb;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator right = getRight();

    rightHashTableIndices = new IntObjectHashMap<>();
    rightHashTable = new MutableTupleBuffer(right.getSchema());

    ans = new TupleBatchBuffer(getSchema());
    doJoin = new JoinProcedure();
  }

  /**
   * Process the tuples from left child.
   *
   * @param tb TupleBatch to be processed.
   */
  protected void processLeftChildTB(final TupleBatch tb) {
    doJoin.joinAgainstHashTable = rightHashTable;
    doJoin.inputCmpColumns = leftCompareIndx;
    doJoin.joinAgainstCmpColumns = rightCompareIndx;
    doJoin.inputTB = tb;

    for (int row = 0; row < tb.numTuples(); ++row) {
      final int cntHashCode = HashUtils.hashSubRow(tb, doJoin.inputCmpColumns, row);
      IntArrayList tuplesWithHashCode = rightHashTableIndices.get(cntHashCode);
      if (tuplesWithHashCode != null) {
        doJoin.row = row;
        tuplesWithHashCode.forEach(doJoin);
      }
    }
  }

  /**
   * Process the tuples from right child.
   *
   * @param tb TupleBatch to be processed.
   */
  protected void processRightChildTB(final TupleBatch tb) {

    for (int row = 0; row < tb.numTuples(); ++row) {
      final int cntHashCode = HashUtils.hashSubRow(tb, rightCompareIndx, row);
      // only build hash table on two sides if none of the children is EOS
      addToHashTable(tb, row, rightHashTable, rightHashTableIndices, cntHashCode);
    }
  }

  /**
   * @param tb the source TupleBatch
   * @param row the row number to get added to hash table
   * @param hashTable the target hash table
   * @param hashTable1IndicesLocal hash table 1 indices local
   * @param hashCode the hashCode of the tb.
   * */
  private void addToHashTable(
      final TupleBatch tb,
      final int row,
      final MutableTupleBuffer hashTable,
      final IntObjectHashMap<IntArrayList> hashTable1IndicesLocal,
      final int hashCode) {
    final int nextIndex = hashTable.numTuples();
    IntArrayList tupleIndicesList = hashTable1IndicesLocal.get(hashCode);
    if (tupleIndicesList == null) {
      tupleIndicesList = new IntArrayList(1);
      hashTable1IndicesLocal.put(hashCode, tupleIndicesList);
    }
    tupleIndicesList.add(nextIndex);
    List<? extends Column<?>> inputColumns = tb.getDataColumns();
    for (int column = 0; column < tb.numColumns(); column++) {
      hashTable.put(column, inputColumns.get(column), row);
    }
  }
}
