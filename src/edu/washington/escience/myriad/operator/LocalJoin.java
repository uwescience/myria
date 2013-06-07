package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.MyriaConstants;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.Worker.QueryExecutionMode;

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
   * A hash table for tuples from left child.
   * */
  private transient HashMap<Integer, List<List<Object>>> leftHashTable;
  /**
   * A hash table for tuples from right child.
   * */
  private transient HashMap<Integer, List<List<Object>>> rightHashTable;
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
    Preconditions.checkArgument(compareIndx1.length == compareIndx2.length);
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
   * @param leftTuple a list representation of the left tuple
   * @param rightTuple a list representation of the right tuple
   * @param fromLeft if the tuple starts from the left
   */
  protected void addToAns(final List<Object> leftTuple, final List<Object> rightTuple, final boolean fromLeft) {
    if (fromLeft) {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, leftTuple.get(answerColumns1[i]));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, rightTuple.get(answerColumns2[i]));
      }
    } else {
      for (int i = 0; i < answerColumns1.length; ++i) {
        ans.put(i, rightTuple.get(answerColumns1[i]));
      }
      for (int i = 0; i < answerColumns2.length; ++i) {
        ans.put(i + answerColumns1.length, leftTuple.get(answerColumns2[i]));
      }
    }
  }

  @Override
  protected void cleanup() throws DbException {
    leftHashTable = null;
    rightHashTable = null;
    ans = null;
  }

  /**
   * In blocking mode, asynchronous EOI semantic may make system hang. Only synchronous EOI semantic works.
   * 
   * @return result TB.
   * @throws DbException if any error occurs.
   * */
  private TupleBatch fetchNextReadySynchronousEOI() throws DbException {
    TupleBatch nexttb = ans.popFilled();
    while (nexttb == null) {
      boolean hasnewtuple = false;
      if (!child1.eos() && !childrenEOI[0]) {
        TupleBatch tb = child1.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, true);
        } else {
          if (child1.eoi()) {
            child1.setEOI(false);
            childrenEOI[0] = true;
          }
        }
      }
      if (!child2.eos() && !childrenEOI[1]) {
        TupleBatch tb = child2.nextReady();
        if (tb != null) {
          hasnewtuple = true;
          processChildTB(tb, false);
        } else {
          if (child2.eoi()) {
            child2.setEOI(false);
            childrenEOI[1] = true;
          }
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
    if (!nonBlocking) {
      return fetchNextReadySynchronousEOI();
    }
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
    int numNoData = 0;

    while (numEOS < 2 && numNoData < 2) {

      numEOS = 0;
      if (child1.eos()) {
        numEOS += 1;
      }
      if (child2.eos()) {
        numEOS += 1;
      }
      numNoData = numEOS;

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
    Preconditions.checkArgument(numEOS <= 2);
    Preconditions.checkArgument(numNoData <= 2);

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
    leftHashTable = new HashMap<Integer, List<List<Object>>>();
    rightHashTable = new HashMap<Integer, List<List<Object>>>();
    ans = new TupleBatchBuffer(outputSchema);
    QueryExecutionMode qem = (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE);
    nonBlocking = qem == QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * The query execution mode is nonBlocking.
   * */
  private transient boolean nonBlocking = true;

  /**
   * Check if left and right tuples match.
   * 
   * @param leftTuple the left tuple
   * @param rightTuple the right tuple
   * @param compareIndx1 the comparing list of columns of cntTuple
   * @param compareIndx2 the comparing list of columns of hashTable
   * @return true if equals.
   * */
  private boolean tupleEquals(final List<Object> leftTuple, final List<Object> rightTuple, final int[] compareIndx1,
      final int[] compareIndx2) {
    for (int i = 0; i < compareIndx1.length; ++i) {
      if (!leftTuple.get(compareIndx1[i]).equals(rightTuple.get(compareIndx2[i]))) {
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

    HashMap<Integer, List<List<Object>>> hashTable1Local = leftHashTable;
    HashMap<Integer, List<List<Object>>> hashTable2Local = rightHashTable;
    int[] compareIndx1Local = compareIndx1;
    int[] compareIndx2Local = compareIndx2;
    if (!fromChild1) {
      hashTable1Local = rightHashTable;
      hashTable2Local = leftHashTable;
      compareIndx1Local = compareIndx2;
      compareIndx2Local = compareIndx1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {
      final List<Object> cntTuple = new ArrayList<Object>();
      for (int j = 0; j < tb.numColumns(); ++j) {
        cntTuple.add(tb.getObject(j, i));
      }
      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<List<Object>> tupleList = hashTable2Local.get(cntHashCode);
      if (tupleList != null) {
        for (final List<Object> tuple : tupleList) {
          if (tupleEquals(cntTuple, tuple, compareIndx1Local, compareIndx2Local)) {
            addToAns(cntTuple, tuple, fromChild1);
          }
        }
      }

      if (hashTable1Local.containsKey(cntHashCode)) {
        tupleList = hashTable1Local.get(cntHashCode);
      } else {
        tupleList = new ArrayList<List<Object>>();
        hashTable1Local.put(cntHashCode, tupleList);
      }
      tupleList.add(cntTuple);
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    child1 = children[0];
    child2 = children[1];
    outputSchema = mergeFilter(child1, child2, answerColumns1, answerColumns2);
  }
}
