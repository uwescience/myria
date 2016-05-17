package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.gs.collections.api.block.procedure.primitive.IntProcedure;
import com.gs.collections.impl.list.mutable.primitive.IntArrayList;
import com.gs.collections.impl.map.mutable.primitive.IntObjectHashMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 */
public final class SymmetricHashCountingJoin extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The column indices for comparing of left child. */
  private final int[] leftCompareIndx;
  /** The column indices for comparing of right child. */
  private final int[] rightCompareIndx;
  /** A hash table for tuples from left child. {Hashcode -> List of tuple indices with the same hash code} */
  private transient IntObjectHashMap<IntArrayList> leftHashTableIndices;
  /** A hash table for tuples from right child. {Hashcode -> List of tuple indices with the same hash code} */
  private transient IntObjectHashMap<IntArrayList> rightHashTableIndices;
  /** The buffer holding the valid tuples from left. */
  private transient MutableTupleBuffer leftHashTable;
  /** The buffer holding the valid tuples from right. */
  private transient MutableTupleBuffer rightHashTable;
  /** How many times each key occurred from left. */
  private transient IntArrayList occuredTimesOnLeft;
  /** How many times each key occurred from right. */
  private transient IntArrayList occuredTimesOnRight;
  /** The number of join output tuples so far. */
  private long ans;
  /** The buffer for storing and returning answer. */
  private transient TupleBatchBuffer ansTBB;
  /** The name of the single column output from this operator. */
  private final String columnName;
  /**
   * Traverse through the list of tuples.
   */
  private transient CountingJoinProcedure doCountingJoin;

  /**
   * Whether this operator has returned answer or not.
   */
  private boolean hasReturnedAnswer = false;

  /**
   * Traverse through the list of tuples with the same hash code.
   */
  private final class CountingJoinProcedure implements IntProcedure {

    /** serial version id. */
    private static final long serialVersionUID = 1L;

    /**
     * Hash table.
     */
    private MutableTupleBuffer joinAgainstHashTable;

    /**
     * times of occure of a key.
     */
    private IntArrayList occuredTimesOnJoinAgainstChild;
    /**
     * Join columns in the input.
     */
    private int[] inputCmpColumns;

    /**
     * Join columns in the other table.
     */
    private int[] otherCmpColumns;

    /**
     * row index of the tuple.
     */
    private int row;

    /**
     * input TupleBatch.
     */
    private TupleBatch inputTB;

    @Override
    public void value(final int index) {
      if (TupleUtils.tupleEquals(
          inputTB, inputCmpColumns, row, joinAgainstHashTable, otherCmpColumns, index)) {
        ans += occuredTimesOnJoinAgainstChild.get(index);
      }
    }
  };

  /**
   * Construct a {@link SymmetricHashCountingJoin}.
   *
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public SymmetricHashCountingJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
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
  public SymmetricHashCountingJoin(
      final String outputColumnName,
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2) {
    super(left, right);
    leftCompareIndx = compareIndx1;
    rightCompareIndx = compareIndx2;
    columnName = Objects.requireNonNull(outputColumnName);
  }

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
  protected void cleanup() throws DbException {
    leftHashTable = null;
    rightHashTable = null;
    occuredTimesOnLeft = null;
    occuredTimesOnRight = null;
    leftHashTableIndices = null;
    rightHashTableIndices = null;
    ansTBB = null;
    ans = 0;
  }

  @Override
  public void checkEOSAndEOI() {
    final Operator left = getLeft();
    final Operator right = getRight();
    if (left.eos() && right.eos() && hasReturnedAnswer) {
      setEOS();
      return;
    }

    // at the time of eos, this operator will not return any data, so it can be safely set EOI to true
    if ((childrenEOI[0] || left.eos()) && (childrenEOI[1] || right.eos()) && hasReturnedAnswer) {
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

    /**
     * There is no distinction between synchronous EOI and asynchronous EOI
     *
     */
    final Operator left = getLeft();
    final Operator right = getRight();

    int numOfChildNoData = 0;
    while (numOfChildNoData < 2 && (!left.eos() || !right.eos())) {

      /*
       * If one of the children is already EOS, we need to set numOfChildNoData to 1 since "numOfChildNoData++" for this
       * child will not be called.
       */
      if (left.eos() || right.eos()) {
        numOfChildNoData = 1;
      } else {
        numOfChildNoData = 0;
      }

      /* process tuple from left child */
      if (!left.eos()) {
        TupleBatch leftTB = left.nextReady();
        if (leftTB != null) { // process the data that is pulled from left child
          processChildTB(leftTB, true);
        } else {
          /* if left eoi, consume it, check whether it will cause EOI of this operator */
          if (left.eoi()) {
            consumeChildEOI(true);
            /*
             * If this operator is ready to emit EOI ( reminder that it might need to clear buffer), break to EOI handle
             * part
             */
            if (isEOIReady()) {
              break;
            }
          }
          numOfChildNoData++;
        }
      }

      /* process tuple from right child */
      if (!right.eos()) {
        TupleBatch rightTB = right.nextReady();
        if (rightTB != null) { // process the data that is pulled from right child
          processChildTB(rightTB, false);
        } else {
          /* if right eoi, consume it, check whether it will cause EOI of this operator */
          if (right.eoi()) {
            consumeChildEOI(false);
            /*
             * If this operator is ready to emit EOI ( reminder that it might need to clear buffer), break to EOI handle
             * part
             */
            if (isEOIReady()) {
              break;
            }
          }
          numOfChildNoData++;
        }
      }
    }

    /*
     * If the operator is ready to EOI, just set EOI since EOI will not return any data. If the operator is ready to
     * EOS, return answer first, then at the next round set EOS
     */
    if (isEOIReady()) {
      if (left.eos() && right.eos() && (!hasReturnedAnswer)) {
        hasReturnedAnswer = true;
        ansTBB.putLong(0, ans);
        return ansTBB.popAny();
      }
    }
    return null;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    leftHashTableIndices = new IntObjectHashMap<>();
    rightHashTableIndices = new IntObjectHashMap<>();
    occuredTimesOnLeft = new IntArrayList();
    occuredTimesOnRight = new IntArrayList();
    leftHashTable = new MutableTupleBuffer(getLeft().getSchema().getSubSchema(leftCompareIndx));
    rightHashTable = new MutableTupleBuffer(getRight().getSchema().getSubSchema(rightCompareIndx));
    ans = 0;
    ansTBB = new TupleBatchBuffer(getSchema());
    doCountingJoin = new CountingJoinProcedure();
  }

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromLeft if the tb is from left.
   */
  protected void processChildTB(final TupleBatch tb, final boolean fromLeft) {

    final Operator left = getLeft();
    final Operator right = getRight();

    MutableTupleBuffer hashTable1Local = null;
    IntObjectHashMap<IntArrayList> hashTable1IndicesLocal = null;
    IntObjectHashMap<IntArrayList> hashTable2IndicesLocal = null;
    IntArrayList ownOccuredTimes = null;
    if (fromLeft) {
      hashTable1Local = leftHashTable;
      doCountingJoin.joinAgainstHashTable = rightHashTable;
      hashTable1IndicesLocal = leftHashTableIndices;
      hashTable2IndicesLocal = rightHashTableIndices;
      doCountingJoin.inputCmpColumns = leftCompareIndx;
      doCountingJoin.otherCmpColumns = rightCompareIndx;
      doCountingJoin.occuredTimesOnJoinAgainstChild = occuredTimesOnRight;
      ownOccuredTimes = occuredTimesOnLeft;
    } else {
      hashTable1Local = rightHashTable;
      doCountingJoin.joinAgainstHashTable = leftHashTable;
      hashTable1IndicesLocal = rightHashTableIndices;
      hashTable2IndicesLocal = leftHashTableIndices;
      doCountingJoin.inputCmpColumns = rightCompareIndx;
      doCountingJoin.otherCmpColumns = leftCompareIndx;
      doCountingJoin.occuredTimesOnJoinAgainstChild = occuredTimesOnLeft;
      ownOccuredTimes = occuredTimesOnRight;
    }
    doCountingJoin.inputTB = tb;

    if (left.eos() && !right.eos()) {
      /*
       * delete right child's hash table if the left child is EOS, since there will be no incoming tuples from right as
       * it will never be probed again.
       */
      rightHashTableIndices = null;
      rightHashTable = null;
    } else if (right.eos() && !left.eos()) {
      /*
       * delete left child's hash table if the right child is EOS, since there will be no incoming tuples from left as
       * it will never be probed again.
       */
      leftHashTableIndices = null;
      leftHashTable = null;
    }

    for (int row = 0; row < tb.numTuples(); ++row) {

      /*
       * update number of count of probing the other child's hash table.
       */
      final int cntHashCode = HashUtils.hashSubRow(tb, doCountingJoin.inputCmpColumns, row);
      IntArrayList tuplesWithHashCode = hashTable2IndicesLocal.get(cntHashCode);
      if (tuplesWithHashCode != null) {
        doCountingJoin.row = row;
        tuplesWithHashCode.forEach(doCountingJoin);
      }

      if (hashTable1Local != null) {
        // only build hash table on two sides if none of the children is EOS
        updateHashTableAndOccureTimes(
            tb,
            row,
            cntHashCode,
            hashTable1Local,
            hashTable1IndicesLocal,
            doCountingJoin.inputCmpColumns,
            ownOccuredTimes);
      }
    }
  }

  @Override
  protected Schema generateSchema() {
    final Schema leftSchema = getLeft().getSchema();
    final Schema rightSchema = getRight().getSchema();

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

    return Schema.of(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of(columnName));
  }

  /**
   * @param tb the source TupleBatch
   * @param row the row number of the to be processed tuple in the source TupleBatch
   * @param hashCode the hashCode of the to be processed tuple
   * @param hashTable the hash table to be updated
   * @param hashTableIndices the hash indices to be updated
   * @param compareColumns compareColumns of input tuple
   * @param occuredTimes occuredTimes array to be updated
   */
  private void updateHashTableAndOccureTimes(
      final TupleBatch tb,
      final int row,
      final int hashCode,
      final MutableTupleBuffer hashTable,
      final IntObjectHashMap<IntArrayList> hashTableIndices,
      final int[] compareColumns,
      final IntArrayList occuredTimes) {

    /* get the index of the tuple's hash code corresponding to */
    final int nextIndex = hashTable.numTuples();
    IntArrayList tupleIndicesList = hashTableIndices.get(hashCode);

    /* create one is there is no such a index yet (there is no tuple with the same hash code has been processed ) */
    if (tupleIndicesList == null) {
      tupleIndicesList = new IntArrayList(1);
      hashTableIndices.put(hashCode, tupleIndicesList);
    }

    Preconditions.checkArgument(hashTable.numColumns() == compareColumns.length);
    List<? extends Column<?>> inputColumns = tb.getDataColumns();

    /* find whether this tuple's comparing key has occured before. If it is, only update occurred times */
    boolean found = false;
    for (int i = 0; i < tupleIndicesList.size(); ++i) {
      int index = tupleIndicesList.get(i);
      if (TupleUtils.tupleEquals(tb, compareColumns, row, hashTable, index)) {
        occuredTimes.set(index, occuredTimes.get(index) + 1);
        found = true;
        break;
      }
    }

    if (!found) {
      tupleIndicesList.add(nextIndex);
      for (int column = 0; column < hashTable.numColumns(); ++column) {
        hashTable.put(column, inputColumns.get(compareColumns[column]), row);
      }
      occuredTimes.add(1);
    }
  }
}
