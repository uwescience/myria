package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.TupleBuffer;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.parallel.QueryExecutionMode;
import edu.washington.escience.myria.parallel.TaskResourceManager;

/**
 * 
 * Local counting join which will only build hash table of the right child.
 * 
 * @author Shumo Chu <chushumo@cs.washington.edu>
 * 
 */
public class LocalUnbalancedCountingJoin extends BinaryOperator {
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
  private transient HashMap<Integer, List<Integer>> hashTableIndices;

  /**
   * The buffer holding the valid tuples from right.
   */
  private transient TupleBuffer hashTable;
  /**
   * How many times each key occurred from right.
   */
  private transient List<Integer> occurredTimes;
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
  public LocalUnbalancedCountingJoin(final Operator left, final Operator right, final int[] compareIndx1,
      final int[] compareIndx2) {
    this(null, left, right, compareIndx1, compareIndx2);
  }

  /**
   * Construct a LocalCountingJoin operator with schema specified.
   * 
   * @param outputColumnName the name of the column of the output table.
   * @param left the left child.
   * @param right the right child.
   * @param compareIndx1 the columns of the left child to be compared with the right. Order matters.
   * @param compareIndx2 the columns of the right child to be compared with the left. Order matters.
   * @throw IllegalArgumentException if there are duplicated column names in <tt>outputSchema</tt>, or if
   *        <tt>outputSchema</tt> does not have the correct number of columns and column types.
   */
  public LocalUnbalancedCountingJoin(final String outputColumnName, final Operator left, final Operator right,
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
    if (!nonBlocking) {
      ;
      // to be implemented
    }

    if (eoi()) {
      ansTBB.put(0, ans);
      return ansTBB.popAny();
    }

    final Operator left = getLeft();
    final Operator right = getRight();
    TupleBatch leftTB = null;
    TupleBatch rightTB = null;
    int numEOS = 0;
    int numNoData = 0;

    while (numEOS < 2 && numNoData < 2) {

      numEOS = 0;
      if (left.eos()) {
        numEOS += 1;
      }
      if (right.eos()) {
        numEOS += 1;
      }
      numNoData = numEOS;

      leftTB = null;
      rightTB = null;
      if (!right.eos()) {
        rightTB = right.nextReady();
        if (rightTB != null) {
          processRightChildTB(rightTB);
        } else {
          if (right.eoi()) {
            right.setEOI(false);
            childrenEOI[1] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (right.eos()) {
            numEOS++;
          } else {
            numNoData++;
          }
        }
      }

      if (right.eos() && !left.eos()) {
        leftTB = left.nextReady();
        if (leftTB != null) { // data
          processLeftChildTB(leftTB);
        } else {
          // eoi or eos or no data
          if (left.eoi()) {
            left.setEOI(false);
            childrenEOI[0] = true;
            checkEOSAndEOI();
            if (eoi()) {
              break;
            }
          } else if (left.eos()) {
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
      ansTBB.put(0, ans);
      return ansTBB.popAny();
    }
    return null;
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    final Operator right = getRight();
    hashTableIndices = new HashMap<Integer, List<Integer>>();
    hashTable = new TupleBuffer(right.getSchema().getSubSchema(compareIndx2));
    occurredTimes = new ArrayList<Integer>();
    ans = 0;
    ansTBB = new TupleBatchBuffer(outputSchema);
    TaskResourceManager qem = (TaskResourceManager) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_TASK_RESOURCE_MANAGER);
    nonBlocking = qem.getExecutionMode() == QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * The query execution mode is nonBlocking.
   */
  private transient boolean nonBlocking = true;

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
        List<Integer> indexList = hashTableIndices.get(cntHashCode);
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable, index, compareIndx2)) {
            occurredTimes.set(index, occurredTimes.get(index) + 1);
            found = true;
            break;
          }
        }
      } else {
        // create a hash table bucket if there isn't one
        hashTableIndices.put(cntHashCode, new ArrayList<Integer>());
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
      List<Integer> indexList = hashTableIndices.get(cntHashCode);
      if (indexList != null) {
        for (final int index : indexList) {
          if (tupleEquals(cntTuple, hashTable, index, compareIndx1)) {
            ans += occurredTimes.get(index);
          }
        }
      }
    }
  }
}
