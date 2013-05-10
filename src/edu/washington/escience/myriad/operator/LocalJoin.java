package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 * */
public final class LocalJoin extends Operator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The two children.
   * */
  private Operator child1, child2;

  /**
   * The result schema.
   * */
  private Schema outputSchema;
  /**
   * The column indices for comparing of child 1.
   * */
  private final int[] compareIndx1;
  /**
   * The column indices for comparing of child 2.
   * */
  private final int[] compareIndx2;
  /**
   * A hash table for tuples from child 1. {Hashcode -> List of tuple indices with the same hash code}
   * */
  private transient HashMap<Integer, List<Integer>> hashTable1Indices;
  /**
   * A hash table for tuples from child 2. {Hashcode -> List of tuple indices with the same hash code}
   * */
  private transient HashMap<Integer, List<Integer>> hashTable2Indices;
  /**
   * The buffer holding the valid tuples from child1.
   * */
  private transient TupleBatchBuffer hashTable1;
  /**
   * The buffer holding the valid tuples from child2.
   * */
  private transient TupleBatchBuffer hashTable2;
  /**
   * The buffer holding the results.
   * */
  private transient TupleBatchBuffer ans;
  /** Which columns in the left child are to be output. */
  private final int[] answerColumns1;
  /** Which columns in the right child are to be output. */
  private final int[] answerColumns2;

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   * 
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names from the children.
   */
  public LocalJoin(final Operator child1, final Operator child2, final int[] compareIndx1, final int[] compareIndx2) {
    this(Schema.merge(child1.getSchema(), child2.getSchema()), child1, child2, compareIndx1, compareIndx2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalJoin(final Operator child1, final Operator child2, final int[] compareIndx1, final int[] compareIndx2,
      final int[] answerColumns1, final int[] answerColumns2) {
    this(mergeFilter(child1, child2, answerColumns1, answerColumns2), child1, child2, compareIndx1, compareIndx2,
        answerColumns1, answerColumns2);
  }

  /**
   * Construct an EquiJoin operator. It returns the specified columns from both children when the corresponding columns
   * in compareIndx1 and compareIndx2 match.
   * 
   * @param outputSchema the Schema of the output table.
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @param answerColumns1 the columns of the left child to be returned. Order matters.
   * @param answerColumns2 the columns of the right child to be returned. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  private LocalJoin(final Schema outputSchema, final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
    this.outputSchema = outputSchema;
    this.child1 = child1;
    this.child2 = child2;
    this.compareIndx1 = compareIndx1;
    this.compareIndx2 = compareIndx2;
    this.answerColumns1 = answerColumns1;
    this.answerColumns2 = answerColumns2;
  }

  /**
   * Construct an EquiJoin operator. It returns all columns from both children when the corresponding columns in
   * compareIndx1 and compareIndx2 match.
   * 
   * @param outputSchema the Schema of the output table.
   * @param child1 the left child.
   * @param child2 the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  private LocalJoin(final Schema outputSchema, final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2) {
    this(outputSchema, child1, child2, compareIndx1, compareIndx2, range(child1.getSchema().numColumns()), range(child2
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

  /**
   * Helper function to generate the proper output schema merging two parts of two schemas.
   * 
   * @param child1 the left child.
   * @param child2 the right child.
   * @param answerColumns1 the selected columns of the left schema.
   * @param answerColumns2 the selected columns of the right schema.
   * @return a schema that contains the chosen columns of the left and right schema.
   */
  private static Schema mergeFilter(final Operator child1, final Operator child2, final int[] answerColumns1,
      final int[] answerColumns2) {
    if (child1 == null || child2 == null) {
      return null;
    }
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    for (int i : answerColumns1) {
      types.add(child1.getSchema().getColumnType(i));
      names.add(child1.getSchema().getColumnName(i));
    }
    for (int i : answerColumns2) {
      types.add(child2.getSchema().getColumnType(i));
      names.add(child2.getSchema().getColumnName(i));
    }

    return new Schema(types, names);
  }

  /**
   * @param cntTuple a list representation of a tuple
   * @param hashTable the buffer holding the tuples to join against
   * @param index the index of hashTable, which the cntTuple is to join with
   * @param fromChild1 if the tuple is from child 1
   */
  protected void addToAns(final List<Object> cntTuple, final TupleBatchBuffer hashTable, final int index,
      final boolean fromChild1) {
    if (fromChild1) {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, cntTuple.get(answerColumns1[i]));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, hashTable.get(answerColumns2[i], index));
      }
    } else {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, hashTable.get(answerColumns1[i], index));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, cntTuple.get(answerColumns2[i]));
      }
    }
  }

  @Override
  protected void cleanup() throws DbException {
    hashTable1 = null;
    hashTable2 = null;
    ans = null;
  }

  @Override
  protected TupleBatch fetchNext() throws DbException, InterruptedException {
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasNewTuple = false;
      TupleBatch tb = child1.next();
      if (tb != null) {
        hasNewTuple = true;
        processChildTB(tb, true);
      } else {
        if (child1.eoi()) {
          child1.setEOI(false);
          childrenEOI[0] = true;
        }
      }
      tb = child2.next();
      if (tb != null) {
        hasNewTuple = true;
        processChildTB(tb, false);
      } else {
        if (child2.eoi()) {
          child2.setEOI(false);
          childrenEOI[1] = true;
        }
      }
      nexttb = ans.popFilled();
      if (nexttb != null) {
        return nexttb;
      }
      if (!hasNewTuple) {
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

    if (child1.eos() && child2.eos()) {
      setEOS();
      return;
    }

    // EOS could be used as an EOI
    if ((childrenEOI[0] || child1.eos()) && (childrenEOI[1] || child2.eos())) {
      setEOI(true);
      Arrays.fill(childrenEOI, false);
    }
  }

  /**
   * Recording the EOI status of the children.
   * */
  private final boolean[] childrenEOI = new boolean[2];

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    }

    if (eoi()) {
      return ans.popAny();
    }

    TupleBatch child1TB = null;
    TupleBatch child2TB = null;

    int numEOS = 0;
    if (child1.eos()) {
      numEOS = 1;
    }
    if (child2.eos()) {
      numEOS += 1;
    }
    int numNoData = numEOS;

    while (numEOS < 2 && numNoData < 2) {
      child1TB = null;
      child2TB = null;
      if (!child1.eos()) {
        child1TB = child1.nextReady();
        if (child1TB != null) { // data
          processChildTB(child1TB, true);
          nexttb = ans.popFilled();
          if (nexttb != null) {
            return nexttb;
          }
        } else {
          // eoi or eos or no data
          if (child1.eoi()) {
            child1.setEOI(false);
            childrenEOI[0] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (child1.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }
      if (!child2.eos()) {
        child2TB = child2.nextReady();
        if (child2TB != null) {
          processChildTB(child2TB, false);
          nexttb = ans.popFilled();
          if (nexttb != null) {
            return nexttb;
          }
        } else {
          if (child2.eoi()) {
            child2.setEOI(false);
            childrenEOI[1] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (child2.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }
    }

    checkEOSAndEOI();
    if (eoi() || eos()) {
      nexttb = ans.popAny();
    }
    return nexttb;
  }

  @Override
  public Operator[] getChildren() {
    return new Operator[] { child1, child2 };
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    hashTable1Indices = new HashMap<Integer, List<Integer>>();
    hashTable2Indices = new HashMap<Integer, List<Integer>>();
    hashTable1 = new TupleBatchBuffer(child1.getSchema());
    hashTable2 = new TupleBatchBuffer(child2.getSchema());
    ans = new TupleBatchBuffer(outputSchema);
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
   * */
  private boolean tupleEquals(final List<Object> cntTuple, final TupleBatchBuffer hashTable, final int index,
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
   * @param tb the incoming TupleBatch for processing join.
   * @param fromChild1 if the tb is from child1.
   * */
  protected void processChildTB(final TupleBatch tb, final boolean fromChild1) {

    TupleBatchBuffer hashTable1Local = hashTable1;
    TupleBatchBuffer hashTable2Local = hashTable2;
    HashMap<Integer, List<Integer>> hashTable1IndicesLocal = hashTable1Indices;
    HashMap<Integer, List<Integer>> hashTable2IndicesLocal = hashTable2Indices;
    int[] compareIndx1Local = compareIndx1;
    int[] compareIndx2Local = compareIndx2;
    if (!fromChild1) {
      hashTable1Local = hashTable2;
      hashTable2Local = hashTable1;
      hashTable1IndicesLocal = hashTable2Indices;
      hashTable2IndicesLocal = hashTable1Indices;
      compareIndx1Local = compareIndx2;
      compareIndx2Local = compareIndx1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int nextIndex = hashTable1Local.numTuples();
      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<Integer> indexList = hashTable2IndicesLocal.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable2Local, index, compareIndx1Local, compareIndx2Local)) {
            addToAns(cntTuple, hashTable2Local, index, fromChild1);
          }
        }
      }
      if (hashTable1IndicesLocal.get(cntHashCode) == null) {
        hashTable1IndicesLocal.put(cntHashCode, new ArrayList<Integer>());
      }
      hashTable1IndicesLocal.get(cntHashCode).add(nextIndex);
      for (int j = 0; j < tb.numColumns(); ++j) {
        hashTable1Local.put(j, cntTuple.get(j));
      }
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
    outputSchema = mergeFilter(child1, child2, answerColumns1, answerColumns2);
  }
}
