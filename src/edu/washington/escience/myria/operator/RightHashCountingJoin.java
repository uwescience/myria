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
 *
 * Counting join which will only build hash table of the right child.
 *
 */
public class RightHashCountingJoin extends BinaryOperator {
  /**
   * This is required for serialization.
   */
  private static final long serialVersionUID = 1L;

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
  private transient IntObjectHashMap<IntArrayList> hashTableIndices;

  /**
   * The buffer holding the valid tuples from right.
   */
  private transient MutableTupleBuffer hashTable;
  /**
   * How many times each key occurred from right.
   */
  private transient IntArrayList occurredTimes;
  /**
   * The buffer holding the results.
   */
  private transient long ans;

  /** The buffer for storing and returning answer. */
  private transient TupleBatchBuffer ansTBB;

  /** The name of the single column output from this operator. */
  private final String columnName;

  /**
   * Whether this operator has returned answer or not.
   * */
  private boolean hasReturnedAnswer = false;

  /**
   * Traverse through the list of tuples.
   * */
  private transient CountingJoinProcedure doCountingJoin;

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class CountingJoinProcedure implements IntProcedure {

    /** serial version id. */
    private static final long serialVersionUID = 1L;

    /**
     * Hash table.
     * */
    private MutableTupleBuffer joinAgainstHashTable;

    /**
     * times of occure of a key.
     * */
    private IntArrayList occuredTimesOnJoinAgainstChild;
    /**
     *
     * */
    private int[] inputCmpColumns;

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
      if (TupleUtils.tupleEquals(inputTB, inputCmpColumns, row, joinAgainstHashTable, index)) {
        ans += occuredTimesOnJoinAgainstChild.get(index);
      }
    }
  };

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
  public RightHashCountingJoin(
      final Operator left,
      final Operator right,
      final int[] compareIndx1,
      final int[] compareIndx2) {
    this("count", left, right, compareIndx1, compareIndx2);
  }

  /**
   * Construct a {@link RightHashCountingJoin} operator with output column name specified.
   *
   * @param outputColumnName the name of the column of the output table.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public RightHashCountingJoin(
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

  @Override
  protected void cleanup() throws DbException {
    hashTable = null;
    hashTableIndices = null;
    occurredTimes = null;
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

    // EOS could be used as an EOI
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

    /*
     * There is no distinction between blocking and non-blocking.
     */

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
    }

    /*
     * If the operator is ready to EOI, just set EOI since EOI will not return any data. If the operator is ready to
     * EOS, return answer first, then at the next round set EOS
     */
    if (isEOIReady()) {
      if (left.eos() && right.eos() && (!hasReturnedAnswer)) {
        ansTBB.putLong(0, ans);
        hasReturnedAnswer = true;
        return ansTBB.popAny();
      }
    }

    /* If not eos, return null since there is no tuple can be processed right now */
    return null;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator right = getRight();
    hashTableIndices = new IntObjectHashMap<>();
    hashTable = new MutableTupleBuffer(right.getSchema().getSubSchema(rightCompareIndx));
    occurredTimes = new IntArrayList();
    doCountingJoin = new CountingJoinProcedure();
    ans = 0;
    ansTBB = new TupleBatchBuffer(getSchema());
  }

  /**
   * Process tuples from right child: build up hash tables.
   *
   * @param tb the incoming TupleBatch.
   */
  protected void processRightChildTB(final TupleBatch tb) {
    for (int row = 0; row < tb.numTuples(); ++row) {
      final int cntHashCode = HashUtils.hashSubRow(tb, rightCompareIndx, row);
      // only build hash table on two sides if none of the children is EOS
      updateHashTableAndOccureTimes(
          tb, row, cntHashCode, hashTable, hashTableIndices, rightCompareIndx, occurredTimes);
    }
  }

  /**
   * Process tuples from the left child: do the actual count join.
   *
   * @param tb the incoming TupleBatch for processing join.
   */
  protected void processLeftChildTB(final TupleBatch tb) {

    doCountingJoin.inputCmpColumns = leftCompareIndx;
    doCountingJoin.inputTB = tb;
    doCountingJoin.occuredTimesOnJoinAgainstChild = occurredTimes;
    doCountingJoin.joinAgainstHashTable = hashTable;
    for (int row = 0; row < tb.numTuples(); ++row) {

      /*
       * update number of count of probing the other child's hash table.
       */
      final int cntHashCode = HashUtils.hashSubRow(tb, doCountingJoin.inputCmpColumns, row);
      IntArrayList tuplesWithHashCode = hashTableIndices.get(cntHashCode);
      if (tuplesWithHashCode != null) {
        doCountingJoin.row = row;
        tuplesWithHashCode.forEach(doCountingJoin);
      }
    }
  }

  /**
   * @param tb the source TupleBatch
   * @param row the row number of the to be processed tuple in the source TupleBatch
   * @param hashCode the hashCode of the to be processed tuple
   * @param hashTable the hash table to be updated
   * @param hashTableIndices the hash indices to be updated
   * @param compareColumns compareColumns of input tuple
   * @param occuredTimes occuredTimes array to be updated
   * */
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

    /* find whether this tuple's comparing key has occurred before. If it is, only update occurred times */
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

  @Override
  protected Schema generateSchema() {
    return Schema.of(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of(columnName));
  }
}
