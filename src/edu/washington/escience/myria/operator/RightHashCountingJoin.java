package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * 
 * Counting join which will only build hash table of the right child.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class RightHashCountingJoin extends BinaryOperator {
  /**
   * This is required for serialization.
   */
  private static final long serialVersionUID = 1L;

  /** The result schema. */
  private final Schema outputSchema;

  /**
   * The column indices for comparing of child 1.
   */
  private final int[] compareIndx1;
  /**
   * The column indices for comparing of child 2.
   */
  private final int[] compareIndx2;

  /**
   * A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code}
   */
  private transient TIntObjectHashMap<TIntArrayList> hashTableIndices;

  /**
   * The buffer holding the valid tuples from right.
   */
  private transient TupleBuffer hashTable;
  /**
   * How many times each key occurred from right.
   */
  private transient TIntArrayList occurredTimes;
  /**
   * The buffer holding the results.
   */
  private transient long ans;

  /** The buffer for storing and returning answer. */
  private transient TupleBatchBuffer ansTBB;

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
  public RightHashCountingJoin(final Operator left, final Operator right, final int[] compareIndx1, final int[] compareIndx2) {
    this(null, left, right, compareIndx1, compareIndx2);
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
  public RightHashCountingJoin(final String outputColumnName, final Operator left, final Operator right,
      final int[] compareIndx1, final int[] compareIndx2) {
    super(left, right);
    ImmutableList<Type> types = ImmutableList.of(Type.LONG_TYPE);
    ImmutableList<String> names;
    if (outputColumnName == null) {
      names = ImmutableList.of("count");
    } else {
      names = ImmutableList.of(outputColumnName);
    }
    outputSchema = Schema.of(types, names);
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
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

    checkEOSAndEOI();
    if (eos()) {
      ansTBB.put(0, ans);
      return ansTBB.popAny();
    }

    /* If not eos, return null since there is no tuple can be processed right now */
    return null;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator right = getRight();
    hashTableIndices = new TIntObjectHashMap<TIntArrayList>();
    hashTable = new TupleBuffer(right.getSchema().getSubSchema(compareIndx2));
    occurredTimes = new TIntArrayList();
    ans = 0;
    ansTBB = new TupleBatchBuffer(outputSchema);
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @param cntTuple a list representation of a tuple
   * @param compareIndx the comparing list of columns of cntTuple
   * @return true if equals.
   */
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBuffer hashTable, final int index,
      final int[] compareIndx) {
    if (compareIndx.length != hashTable.getSchema().numColumns()) {
      return false;
    }
    for (int i = 0; i < compareIndx.length; ++i) {
      if (!cntTuple.get(compareIndx[i]).equals(hashTable.get(i, index))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Process tuples from right child: build up hash tables.
   * 
   * @param tb the incoming TupleBatch.
   */
  protected void processRightChildTB(final TupleBatch tb) {
    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = hashTable.numTuples();
      final int cntHashCode = tb.hashCode(i, compareIndx2);

      // detect if there is a hash bucket for tb
      boolean found = false;
      if (hashTableIndices.get(cntHashCode) != null) {
        // if there is, find if there is tuple with same key, update if so
        TIntArrayList indexList = hashTableIndices.get(cntHashCode);
        for (int j = 0; j < indexList.size(); j++) {
          int index = indexList.get(j);
          if (tupleEquals(cntTuple, hashTable, index, compareIndx2)) {
            occurredTimes.set(index, occurredTimes.get(index) + 1);
            found = true;
            break;
          }
        }
      } else {
        // create a hash table bucket if there isn't one
        hashTableIndices.put(cntHashCode, new TIntArrayList());
      }

      if (!found) {
        // if this key didn't appear before, create the count for this key as 1
        hashTableIndices.get(cntHashCode).add(nextIndex);
        for (int j = 0; j < compareIndx2.length; ++j) {
          hashTable.put(j, cntTuple.get(compareIndx2[j]));
        }
        occurredTimes.add(1);
      }
    }
  }

  /**
   * Process tuples from the left child: do the actual count join.
   * 
   * @param tb the incoming TupleBatch for processing join.
   */
  protected void processLeftChildTB(final TupleBatch tb) {
    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }

      final int cntHashCode = tb.hashCode(i, compareIndx1);
      TIntArrayList indexList = hashTableIndices.get(cntHashCode);
      if (indexList != null) {
        for (int j = 0; j < indexList.size(); j++) {
          int index = indexList.get(j);
          if (tupleEquals(cntTuple, hashTable, index, compareIndx1)) {
            ans += occurredTimes.get(index);
          }
        }
      }
    }
  }

  @Override
  protected Schema generateSchema() {
    return outputSchema;
  }
}
