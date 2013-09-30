package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.SimplePredicate.Op;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;

/**
 * Merges the sorted output of a set of operators.
 * */
public final class Merge extends NAryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** True if the tuples coming from children are in ascending order. */
  private final boolean[] ascending;

  /** Indexes of columns that are sorted. */
  private final int[] sortedColumns;

  /**
   * Fairly get data from children.
   * */
  private transient int childIdxToGet;

  /**
   * Contains a tuple batch for each child.
   */
  private transient ArrayList<LinkedList<ArrayList<Object>>> tupleBuffer =
      new ArrayList<LinkedList<ArrayList<Object>>>();;

  /**
   * The buffer holding the results.
   */
  private transient TupleBatchBuffer ans;

  /**
   * @param children the children to be merged.
   * @param ascending tuple coming from children are in ascending order
   * @param sortedColumns the columns that are sorted in the input
   * 
   * */
  public Merge(final Operator[] children, final int[] sortedColumns, final boolean[] ascending) {
    this.ascending = ascending;
    this.sortedColumns = sortedColumns;
    setChildren(children);
  }

  @Override
  protected void cleanup() throws DbException {
    tupleBuffer.clear();
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    }

    int numEOS = 0;

    while (nexttb == null) {
      // System.out.println("Size of ans " + ans.numTuples());

      // index of the next tuple that is added to ans
      int nextIndex = -1;

      List<Object> nextTuple = null;

      numEOS = 0;

      for (int numMerge = 0; numMerge < numChildren(); numMerge++) {
        childIdxToGet = (childIdxToGet + 1) % numChildren();
        // System.out.println("Buffer " + childIdxToGet + " " + tupleBuffer.get(childIdxToGet).size());

        // if (tupleBuffer.get(childIdxToGet).size() > 0) {
        // System.out.println(tupleBuffer.get(childIdxToGet).getFirst());
        // }

        Operator child = children[childIdxToGet];

        if (child.eos()) {
          numEOS++;
        }

        LinkedList<ArrayList<Object>> tuples = tupleBuffer.get(childIdxToGet);

        if (tuples.size() == 0 && !child.eos()) {
          TupleBatch tb = child.fetchNextReady();
          if (tb == null) {
            // System.out.println("Break because child cannot deliver " + childIdxToGet);
            nextIndex = -1;
            nextTuple = null;
            break;
          } else {
            importTuplesIntoBuffer(childIdxToGet, tb);
          }
        }

        if (tuples.size() == 0) {
          continue;
        }

        ArrayList<Object> currentTuple = tuples.getFirst();

        if (nextTuple != null) {
          for (int j = 0; j < sortedColumns.length; j++) {
            int columnIndex = sortedColumns[j];

            Object valueInTuple = currentTuple.get(columnIndex);
            Object operand = nextTuple.get(columnIndex);
            Type columnType = getSchema().getColumnType(j);
            Op operator = Op.GREATER_THAN;
            if (ascending[j]) {
              operator = Op.LESS_THAN;
            }
            // System.out.println("Operator: " + operator);
            boolean isSmallest = columnType.compareObjects(operator, valueInTuple, operand);

            if (isSmallest) {
              nextTuple = currentTuple;
              nextIndex = childIdxToGet;
            }
          }
        } else {
          nextTuple = currentTuple;
          nextIndex = childIdxToGet;
        }
      }

      if (nextIndex >= 0) {
        // System.out.println("Take " + nextIndex);
        addToAns(tupleBuffer.get(nextIndex).removeFirst());
      }

      nexttb = ans.popFilled();
      if (nexttb != null) {
        // System.out.println("Size of next" + nexttb.numTuples());
        break;
      }

      // if all buffers are empty, there is no point in
      // continuing to fill ans
      boolean allBuffersEmpty = true;
      for (int i = 0; i < numChildren(); i++) {
        if (tupleBuffer.get(i).size() > 0) {
          allBuffersEmpty = false;
          break;
        }
      }
      if (allBuffersEmpty) {
        // System.out.println("All buffers empty");
        break;
      }
    }

    if (nexttb == null && ans.numTuples() > 0) {
      nexttb = ans.popAny();
    }

    if (numEOS == numChildren()) {
      setEOS();
      Preconditions.checkArgument(nexttb != null);
      Preconditions.checkArgument(ans.numTuples() == 0);
    }

    // System.out.println("Tuple buffer " + nexttb);
    return nexttb;
  }

  /**
   * Imports all the tuples form a column stored tuple batch into tuple store.
   * 
   * @param childIndex which child buffer should be filled
   * @param tb the tuple buffer to be imported
   */
  private void importTuplesIntoBuffer(final int childIndex, final TupleBatch tb) {
    for (int i1 = 0; i1 < tb.numTuples(); i1++) {
      final ArrayList<Object> tuple = new ArrayList<Object>();
      tuple.ensureCapacity(tb.numColumns());
      for (int j = 0; j < tb.numColumns(); j++) {
        tuple.add(tb.getObject(j, i1));
      }
      tupleBuffer.get(childIndex).add(tuple);
    }
  }

  /**
   * Append a tuple to ans.
   * 
   * @param cntTuple a tuple as list of objects.
   */
  private void addToAns(final List<Object> cntTuple) {
    for (int i = 0; i < getSchema().numColumns(); ++i) {
      ans.put(i, cntTuple.get(i));
    }
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    ans = new TupleBatchBuffer(getSchema());
    childIdxToGet = 0;

    for (int i = 0; i < numChildren(); i++) {
      tupleBuffer.add(new LinkedList<ArrayList<Object>>());
    }
  }

  @Override
  public void setChildren(final Operator[] children) {
    Objects.requireNonNull(children);
    Preconditions.checkArgument(children.length > 0);
    for (Operator op : children) {
      Preconditions.checkArgument(op.getSchema().equals(children[0].getSchema()));
    }
    this.children = children;
  }
}
