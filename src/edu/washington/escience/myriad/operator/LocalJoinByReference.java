package edu.washington.escience.myriad.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
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
import edu.washington.escience.myriad.util.MyriaUtils;

/**
 * This is an implementation of hash equal join. The same as in DupElim, this implementation does not keep the
 * references to the incoming TupleBatches in order to get better memory performance.
 * */
public final class LocalJoinByReference extends Operator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Data structure for keeping references to a tuple in a TupleBatch.
   * */
  private static final class IndexedTuple {
    /**
     * The row index.
     * */
    private final int index;
    /**
     * The source data tuple batch.
     * */
    private final TupleBatch tupleBatch;

    /**
     * Factory.
     * 
     * @param tb the tuple batch
     * @param index the row of the tuple in the tuple batch
     * @return the IndexedTuple object
     */
    public static IndexedTuple of(final TupleBatch tb, final int index) {
      return new IndexedTuple(tb, index);
    }

    /**
     * The private constructor.
     * 
     * @param tb the tuple batch
     * @param index the row of the tuple in the tuple batch
     * */
    private IndexedTuple(final TupleBatch tb, final int index) {
      tupleBatch = tb;
      this.index = index;
    }

    /**
     * @return the index
     */
    public int getIndex() {
      return index;
    }

    /**
     * @return the tb
     */
    public TupleBatch getTupleBatch() {
      return tupleBatch;
    }
  }

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
  private transient IntObjectOpenHashMap<List<IndexedTuple>> leftHashTable;
  /**
   * A hash table for tuples from right child.
   * */
  private transient IntObjectOpenHashMap<List<IndexedTuple>> rightHashTable;
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
  public LocalJoinByReference(final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2) {
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
  public LocalJoinByReference(final Operator child1, final Operator child2, final int[] compareIndx1,
      final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
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
  private LocalJoinByReference(final Schema outputSchema, final Operator child1, final Operator child2,
      final int[] compareIndx1, final int[] compareIndx2, final int[] answerColumns1, final int[] answerColumns2) {
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
  private LocalJoinByReference(final Schema outputSchema, final Operator child1, final Operator child2,
      final int[] compareIndx1, final int[] compareIndx2) {
    this(outputSchema, child1, child2, compareIndx1, compareIndx2, MyriaUtils.range(child1.getSchema().numColumns()),
        MyriaUtils.range(child2.getSchema().numColumns()));
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
   * Adds a tuple to the answer.
   * 
   * @param leftTb the tuple batch that contains the left tuple
   * @param leftIdx the index of the left tuple in the leftTb
   * @param rightTb the tuple batch that contains the right tuple
   * @param rightIdx the index of the right tuple in the rightTb
   * @param fromLeft if the tuple starts from the left
   */
  protected void addToAns(final TupleBatch leftTb, final int leftIdx, final TupleBatch rightTb, final int rightIdx,
      final boolean fromLeft) {
    if (fromLeft) {
      ans.put(leftTb, leftIdx, answerColumns1, rightTb, rightIdx, answerColumns2);
    } else {
      ans.put(rightTb, rightIdx, answerColumns1, leftTb, leftIdx, answerColumns2);
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
          nexttb = ans.popAnyUsingTimeout();
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
          nexttb = ans.popAnyUsingTimeout();
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
    leftHashTable = new IntObjectOpenHashMap<List<IndexedTuple>>();
    rightHashTable = new IntObjectOpenHashMap<List<IndexedTuple>>();
    ans = new TupleBatchBuffer(outputSchema);
    QueryExecutionMode qem = (QueryExecutionMode) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_EXECUTION_MODE);
    nonBlocking = qem == QueryExecutionMode.NON_BLOCKING;
  }

  /**
   * The query execution mode is nonBlocking.
   * */
  private transient boolean nonBlocking = true;

  /**
   * @param tb the incoming TupleBatch for processing join.
   * @param fromChild1 if the tb is from child1.
   * */
  protected void processChildTB(final TupleBatch tb, final boolean fromChild1) {

    IntObjectOpenHashMap<List<IndexedTuple>> hashTable1Local = leftHashTable;
    IntObjectOpenHashMap<List<IndexedTuple>> hashTable2Local = rightHashTable;
    int[] compareIndx1Local = compareIndx1;
    int[] compareIndx2Local = compareIndx2;
    if (!fromChild1) {
      hashTable1Local = rightHashTable;
      hashTable2Local = leftHashTable;
      compareIndx1Local = compareIndx2;
      compareIndx2Local = compareIndx1;
    }

    for (int i = 0; i < tb.numTuples(); ++i) {
      final int cntHashCode = tb.hashCode(i, compareIndx1Local);
      List<IndexedTuple> tupleList = hashTable2Local.get(cntHashCode);
      if (tupleList != null) {
        for (final IndexedTuple tuple : tupleList) {
          if (tb.tupleMatches(i, compareIndx1Local, tuple.getTupleBatch(), tuple.getIndex(), compareIndx2Local)) {
            addToAns(tb, i, tuple.getTupleBatch(), tuple.getIndex(), fromChild1);
          }
        }
      }

      if (hashTable1Local.containsKey(cntHashCode)) {
        tupleList = hashTable1Local.get(cntHashCode);
      } else {
        tupleList = new ArrayList<IndexedTuple>();
        hashTable1Local.put(cntHashCode, tupleList);
      }
      tupleList.add(IndexedTuple.of(tb, i));
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    Preconditions.checkNotNull(children, "LocalJoin.setChildren called with null argument.");
    Preconditions.checkArgument(children.length == 2, "LocalJoin must have exactly 2 children.");
    child1 = Objects.requireNonNull(children[0], "LocalJoin.setChildren called with null left child.");
    child2 = Objects.requireNonNull(children[1], "LocalJoin.setChildren called with null right child.");
    outputSchema = mergeFilter(child1, child2, answerColumns1, answerColumns2);
  }
}
