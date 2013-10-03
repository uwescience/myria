package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

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
   * Contains a tuple batch for each child.
   */
  private transient ArrayList<TupleBatch> childBatches = new ArrayList<TupleBatch>();

  /**
   * Index that points to the current location in the tupleBuffer of all children.
   */
  private transient ArrayList<Integer> pointerIntoChildBatches = new ArrayList<Integer>();

  /**
   * The buffer holding the results.
   */
  private transient TupleBatchBuffer ans;

  /**
   * Use a heap to find the smallest tuple (actually just the child, where the pointer points to the smallest tuple).
   */
  private transient Queue<Integer> heap;

  /**
   * Comparator for tuples in tuple batch.
   * 
   * @author dominik
   * 
   */
  class TupleComparator implements Comparator<Integer> {
    @Override
    public int compare(final Integer left, final Integer right) {
      // System.out.println("compare " + left + " " + right);
      Integer leftPointer = pointerIntoChildBatches.get(left);
      Integer rightPointer = pointerIntoChildBatches.get(right);
      TupleBatch leftTb = childBatches.get(left);
      TupleBatch rightTb = childBatches.get(right);
      Preconditions.checkArgument(leftTb.numTuples() > leftPointer);
      Preconditions.checkArgument(rightTb.numTuples() > rightPointer);
      for (int columnIndex = 0; columnIndex < sortedColumns.length; columnIndex++) {
        Type columnType = getSchema().getColumnType(columnIndex);
        Op op;
        if (ascending[columnIndex]) {
          op = Op.GREATER_THAN;
        } else {
          op = Op.LESS_THAN;
        }
        switch (columnType) {
          case INT_TYPE:
            if (Type.compare(op, leftTb.getInt(columnIndex, leftPointer), rightTb.getInt(columnIndex, rightPointer))) {
              return 1;
            }
            break;

          case FLOAT_TYPE:
            if (Type
                .compare(op, leftTb.getFloat(columnIndex, leftPointer), rightTb.getFloat(columnIndex, rightPointer))) {
              return 1;
            }
            break;

          case LONG_TYPE:
            if (Type.compare(op, leftTb.getLong(columnIndex, leftPointer), rightTb.getLong(columnIndex, rightPointer))) {
              return 1;
            }
            break;

          case DOUBLE_TYPE:
            if (Type.compare(op, leftTb.getDouble(columnIndex, leftPointer), rightTb.getDouble(columnIndex,
                rightPointer))) {
              return 1;
            }
            break;

          case BOOLEAN_TYPE:
            if (Type.compare(op, leftTb.getBoolean(columnIndex, leftPointer), rightTb.getBoolean(columnIndex,
                rightPointer))) {
              return 1;
            }
            break;

          case STRING_TYPE:
            if (Type.compare(op, leftTb.getString(columnIndex, leftPointer), rightTb.getString(columnIndex,
                rightPointer))) {
              return 1;
            }
            break;

          case DATETIME_TYPE:
            if (Type.compare(op, leftTb.getDateTime(columnIndex, leftPointer), rightTb.getDateTime(columnIndex,
                rightPointer))) {
              return 1;
            }
            break;

          default:
            throw new UnsupportedOperationException("Unknown type " + columnType);
        }
      }
      return -1;
    }
  }

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
    childBatches.clear();
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

      numEOS = 0;

      // TODO: might break if children never deliver anything

      // fill the buffers, if possible and necessary
      boolean notEnoughData = false;
      for (int childId = 0; childId < numChildren(); childId++) {
        Operator child = children[childId];
        if (child.eos()) {
          numEOS++;
          continue;
        }
        if (childBatches.get(childId) == null) {
          TupleBatch tb = child.fetchNextReady();
          if (tb != null) {
            childBatches.set(childId, tb);
            pointerIntoChildBatches.set(childId, 0);
            // this reads the index after we refilled the batch or
            // when the batches are initially loaded
            heap.add(childId);
          } else {
            notEnoughData = true;
            break;
          }
        }
      }

      if (notEnoughData) {
        break;
      }

      // System.out.println("Size of heap " + heap.size() + " num EOS:" + numEOS + " heap " + heap);
      for (int i = 0; i < children.length; i++) {
        if (!children[i].eos() && (childBatches.get(i) != null)) {
          // System.out.print(childBatches.get(i).getObject(0, pointerIntoChildBatches.get(i)) + " ");
        }
      }
      // System.out.println();

      Integer smallestTb = heap.poll();
      // System.out.println("Smallest " + smallestTb);
      if (smallestTb != null) {
        Integer positionInSmallestTb = pointerIntoChildBatches.get(smallestTb);
        ans.put(childBatches.get(smallestTb), positionInSmallestTb);

        // reset tb or increase position pointer
        if (positionInSmallestTb == childBatches.get(smallestTb).numTuples() - 1) {
          pointerIntoChildBatches.set(smallestTb, -1);
          childBatches.set(smallestTb, null);
        } else {
          pointerIntoChildBatches.set(smallestTb, positionInSmallestTb + 1);
          // we either re-add the index to the child here or when we fetched more data
          // we cannot re-add it here if the data is not present
          heap.add(smallestTb);
        }
        nexttb = ans.popFilled();
      }

      if (numEOS == numChildren()) {
        setEOS();
        Preconditions.checkArgument(heap.size() == 0);
        // Preconditions.checkArgument(nexttb != null);
        Preconditions.checkArgument(ans.numTuples() == 0);
        break;
      }
    }

    if (nexttb == null && ans.numTuples() > 0) {
      nexttb = ans.popAny();
    }

    // System.out.println("Tuple buffer " + nexttb);
    return nexttb;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    ans = new TupleBatchBuffer(getSchema());
    for (int i = 0; i < numChildren(); i++) {
      childBatches.add(null);
    }

    Comparator<Integer> comparator = new TupleComparator();
    heap = new PriorityQueue<Integer>(numChildren(), comparator);
  }

  @Override
  public void setChildren(final Operator[] children) {
    Objects.requireNonNull(children);
    Preconditions.checkArgument(children.length > 0);
    for (Operator op : children) {
      Preconditions.checkArgument(op.getSchema().equals(children[0].getSchema()));
      pointerIntoChildBatches.add(-1);
    }
    this.children = children;
  }
}
