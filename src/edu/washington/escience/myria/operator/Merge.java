package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
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
  private boolean[] ascending;

  /** Indexes of columns that are sorted. */
  private int[] sortedColumns;

  /**
   * Contains a tuple batch for each child.
   * 
   * Contains null if there is no batch.
   */
  private transient ArrayList<TupleBatch> childBatches = new ArrayList<TupleBatch>();

  /**
   * Index that points to the current location in the tupleBuffer of all children.
   * 
   * -1 indicates an invalid pointer.
   */
  private transient ArrayList<Integer> pointerIntoChildBatches = new ArrayList<Integer>();

  /**
   * The buffer holding the results.
   */
  private transient TupleBatchBuffer ans;

  /**
   * Use a heap to find the smallest tuple (actually just the index in {@link #childBatches} to a child, where the
   * pointer in {@link #pointerIntoChildBatches} points to the smallest tuple).
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
      Integer leftPointer = pointerIntoChildBatches.get(left);
      Integer rightPointer = pointerIntoChildBatches.get(right);
      TupleBatch leftTb = childBatches.get(left);
      TupleBatch rightTb = childBatches.get(right);
      Preconditions.checkArgument(leftTb.numTuples() > leftPointer);
      Preconditions.checkArgument(rightTb.numTuples() > rightPointer);
      for (int columnIndex = 0; columnIndex < sortedColumns.length; columnIndex++) {
        Type columnType = getSchema().getColumnType(columnIndex);
        int compared, factor;
        if (ascending[columnIndex]) {
          factor = 1;
        } else {
          factor = -1;
        }
        switch (columnType) {
          case INT_TYPE:
            compared =
                Type.compareRaw(leftTb.getInt(columnIndex, leftPointer), rightTb.getInt(columnIndex, rightPointer));
            break;

          case FLOAT_TYPE:
            compared =
                Type.compareRaw(leftTb.getFloat(columnIndex, leftPointer), rightTb.getFloat(columnIndex, rightPointer));
            break;

          case LONG_TYPE:
            compared =
                Type.compareRaw(leftTb.getLong(columnIndex, leftPointer), rightTb.getLong(columnIndex, rightPointer));
            break;

          case DOUBLE_TYPE:
            compared =
                Type.compareRaw(leftTb.getDouble(columnIndex, leftPointer), rightTb
                    .getDouble(columnIndex, rightPointer));
            break;

          case BOOLEAN_TYPE:
            compared =
                Type.compareRaw(leftTb.getBoolean(columnIndex, leftPointer), rightTb.getBoolean(columnIndex,
                    rightPointer));
            break;

          case STRING_TYPE:
            compared =
                Type.compareRaw(leftTb.getString(columnIndex, leftPointer), rightTb
                    .getString(columnIndex, rightPointer));
            break;

          case DATETIME_TYPE:
            compared =
                Type.compareRaw(leftTb.getDateTime(columnIndex, leftPointer), rightTb.getDateTime(columnIndex,
                    rightPointer));
            break;

          default:
            throw new UnsupportedOperationException("Unknown type " + columnType);
        }
        if (compared != 0) {
          return compared * factor;
        }
      }
      return 0;
    }
  }

  /**
   * @param children the children to be merged.
   * @param sortedColumns the indexes of columns that tuples are ordered by in the input
   * @param ascending true for each column that is ordered ascending
   * 
   * */
  public Merge(final Operator[] children, final int[] sortedColumns, final boolean[] ascending) {
    super(children);

    setSortedColumns(sortedColumns, ascending);
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
      numEOS = 0;

      // fill the buffers, if possible and necessary
      boolean notEnoughData = false;
      for (int childIdx = 0; childIdx < getNumChildren(); childIdx++) {
        Operator child = getChild(childIdx);
        if (child.eos()) {
          numEOS++;
          continue;
        }
        if (childBatches.get(childIdx) == null) {
          TupleBatch tb = child.fetchNextReady();

          // After fetching from a child, it might be EOS.
          // If we don't catch this case here but return null,
          // this method might not be called again.
          if (child.eos()) {
            numEOS++;
            continue;
          }
          if (tb != null) {
            childBatches.set(childIdx, tb);
            pointerIntoChildBatches.set(childIdx, 0);

            // add the index after we refilled the batch or
            // when the batches are initially loaded
            heap.add(childIdx);
          } else {
            notEnoughData = true;
            break;
          }
        }
      }

      if (notEnoughData) {
        // break if this is EOS to ensure that we don't return
        // null and have data in buffer which would mean that this
        // method is never called again.
        if (numEOS == getNumChildren()) {
          break;
        }
        return null;
      }

      Integer smallestTb = heap.poll();
      if (smallestTb != null) {
        Integer positionInSmallestTb = pointerIntoChildBatches.get(smallestTb);
        ans.put(childBatches.get(smallestTb), positionInSmallestTb);

        // reset tb or advance position pointer
        if (positionInSmallestTb == childBatches.get(smallestTb).numTuples() - 1) {
          pointerIntoChildBatches.set(smallestTb, -1);
          childBatches.set(smallestTb, null);
        } else {
          pointerIntoChildBatches.set(smallestTb, positionInSmallestTb + 1);
          // We either re-add the index to the child here or when we fetch more data.
          // We cannot re-add it here because we don't know whether there will be data or not.
          heap.add(smallestTb);
        }
        nexttb = ans.popFilled();
      }

      if (numEOS == getNumChildren()) {
        setEOS();
        Preconditions.checkArgument(heap.size() == 0);
        Preconditions.checkArgument(nexttb == null);
        break;
      }
    }

    if (nexttb == null && ans.numTuples() > 0) {
      nexttb = ans.popAny();
    }

    return nexttb;
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    Objects.requireNonNull(getChildren());
    Preconditions.checkArgument(getNumChildren() > 0);

    Preconditions.checkArgument(ascending.length == sortedColumns.length);

    ans = new TupleBatchBuffer(getSchema());
    for (Operator child : getChildren()) {
      Preconditions.checkNotNull(child);
      Preconditions.checkArgument(getSchema().equals(child.getSchema()));

      childBatches.add(null);
      pointerIntoChildBatches.add(-1);
    }

    Comparator<Integer> comparator = new TupleComparator();
    heap = new PriorityQueue<Integer>(getNumChildren(), comparator);
  }

  /**
   * Define how the tuples are sorted in the input and how they should be sorted in the output.
   * 
   * @param sortedColumns the indexes of columns that tuples are ordered by in the input
   * @param ascending true for each column that is ordered ascending
   */
  void setSortedColumns(final int[] sortedColumns, final boolean[] ascending) {
    this.sortedColumns = sortedColumns;
    this.ascending = ascending;
  }
}
