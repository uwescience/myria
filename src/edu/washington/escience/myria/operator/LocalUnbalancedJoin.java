package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * This is an implementation of unbalanced hash join. This operator only builds hash tables for its right child, thus
 * will begin to output tuples after right child EOS.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public final class LocalUnbalancedJoin extends BinaryOperator {

  /**
   * This is required for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * The names of the output columns.
   */
  private final ImmutableList<String> outputColumns;

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
   * The buffer holding the results.
   */
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
  public LocalUnbalancedJoin(final Operator left, final Operator right, final int[] compareIndx1,
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
  public LocalUnbalancedJoin(final Operator left, final Operator right, final int[] compareIndx1,
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
  public LocalUnbalancedJoin(final List<String> outputColumns, final Operator left, final Operator right,
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
   * @param outputColumns the names of the columns in the output schema. If null, the corresponding columns will be
   *          copied from the children.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalUnbalancedJoin(final List<String> outputColumns, final Operator left, final Operator right,
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

    for (int i : answerColumns1) {
      types.add(left.getSchema().getColumnType(i));
      names.add(left.getSchema().getColumnName(i));
    }

    for (int i : answerColumns2) {
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
   * @param cntTuple a list representation of a tuple
   * @param hashTable the buffer holding the tuples to join against
   * @param index the index of hashTable, which the cntTuple is to join with
   */
  protected void addToAns(final List<Object> cntTuple, final TupleBuffer hashTable, final int index) {
    for (int i = 0; i < answerColumns1.length; ++i) {
      ans.put(i, cntTuple.get(answerColumns1[i]));
    }
    for (int i = 0; i < answerColumns2.length; ++i) {
      ans.put(i + answerColumns1.length, hashTable.get(answerColumns2[i], index));
    }

  }

  @Override
  protected void cleanup() throws DbException {
    hashTable = null;
    ans = null;
  }

  @Override
  public void checkEOSAndEOI() {
    final Operator left = getLeft();
    final Operator right = getRight();

    /* If right didn't finish yet, we cannot be EOI or EOS. */
    if (!right.eos()) {
      return;
    }

    /* If left has reached EOS, we are EOS. */
    if (left.eos()) {
      setEOS();
      return;
    }

    /* If left has reached EOI, we are EOI. */
    if (left.eoi()) {
      setEOI(true);
      left.setEOI(false);
    }
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

    checkEOSAndEOI();
    if (eoi() || eos()) {
      nexttb = ans.popAny();
    }
    return nexttb;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator right = getRight();
    hashTableIndices = new TIntObjectHashMap<TIntArrayList>();
    hashTable = new TupleBuffer(right.getSchema());
    ans = new TupleBatchBuffer(getSchema());
  }

  /**
   * Check if a tuple in uniqueTuples equals to the comparing tuple (cntTuple).
   * 
   * @param hashTable the TupleBatchBuffer holding the tuples to compare against
   * @param index the index in the hashTable
   * @param cntTuple a list representation of a tuple
   * @param compareIndx1 the comparing list of columns of cntTuple
   * @param compareIndx2 the comparing list of columns of hashTable
   * @return true if equals.
   */
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBuffer hashTable, final int index,
      final int[] compareIndx1, final int[] compareIndx2) {
    if (compareIndx1.length != compareIndx2.length) {
      return false;
    }
    for (int i = 0; i < compareIndx1.length; ++i) {
      if (!cntTuple.get(compareIndx1[i]).equals(hashTable.get(compareIndx2[i], index))) {
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

      if (hashTableIndices.get(cntHashCode) == null) {
        hashTableIndices.put(cntHashCode, new TIntArrayList());
      }
      hashTableIndices.get(cntHashCode).add(nextIndex);
      for (int j = 0; j < tb.numColumns(); ++j) {
        hashTable.put(j, cntTuple.get(j));
      }
    }
  }

  /**
   * Process tuples from the left child: do the actual join.
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
          if (tupleEquals(cntTuple, hashTable, index, compareIndx1, compareIndx2)) {
            addToAns(cntTuple, hashTable, index);
          }
        }
      }
    }
  }
}
