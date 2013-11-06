package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.Type;

/**
 * Orders tuples in memory.
 * 
 * Note: not efficient so only use it in tests
 */
public final class InMemoryOrderBy extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Which columns to sort the tuples by.
   */
  private final int[] sortColumns;

  /**
   * True for each column that should be sorted ascending.
   */
  private final boolean[] ascending;

  /**
   * Buffers tuples until they are all returned.
   */
  private TupleBatchBuffer ans;

  /**
   * Tuple data stored as columns until it is sorted.
   */
  private ArrayList<ArrayList<Object>> columns;

  /**
   * @param child the source of the tuples.
   */
  public InMemoryOrderBy(final Operator child) {
    this(child, null, null);
  }

  /**
   * @param child the source of the tuples.
   * @param sortColumns the columns that should be ordered by
   * @param ascending true for each column that should be sorted ascending
   */
  public InMemoryOrderBy(final Operator child, final int[] sortColumns, final boolean[] ascending) {
    super(child);
    this.sortColumns = sortColumns;
    this.ascending = ascending;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    Preconditions.checkArgument(sortColumns.length == ascending.length);
    ans = new TupleBatchBuffer(getSchema());

    columns = new ArrayList<ArrayList<Object>>();
    for (int i = 0; i < getSchema().numColumns(); i++) {
      columns.add(new ArrayList<Object>());
    }
  };

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    } else if (ans.numTuples() > 0) {
      return ans.popAny();
    }

    if (columns.get(0).size() > 0) {
      setEOS();
      return null;
    }

    while (!getChild().eos()) {
      TupleBatch tb = getChild().nextReady();
      if (tb != null) {
        for (int columnIdx = 0; columnIdx < tb.numColumns(); columnIdx++) {
          ArrayList<Object> column = columns.get(columnIdx);
          for (int rowIdx = 0; rowIdx < tb.numTuples(); rowIdx++) {
            column.add(tb.getObject(columnIdx, rowIdx));
          }
        }
      } else if (!getChild().eos()) {
        return null;
      }
    }

    Preconditions.checkState(getChild().eos());

    sort();

    final int numTuples = columns.get(0).size();
    for (int rowIdx = 0; rowIdx < numTuples; rowIdx++) {
      for (int columnIdx = 0; columnIdx < getSchema().numColumns(); columnIdx++) {
        ans.put(columnIdx, columns.get(columnIdx).get(rowIdx));
      }
    }

    nexttb = ans.popFilled();
    if (nexttb == null && ans.numTuples() > 0) {
      return ans.popAny();
    }
    return nexttb;
  }

  /**
   * Comparator for tuples made up of columns.
   */
  class TupleComparator implements Comparator<Integer> {
    @Override
    public int compare(final Integer rowIdx, final Integer otherRowIdx) {
      for (int columnIdx = 0; columnIdx < sortColumns.length; columnIdx++) {
        int compared = 0;
        ArrayList<Object> column = columns.get(columnIdx);
        switch (getSchema().getColumnType(columnIdx)) {
          case INT_TYPE:
            compared = Type.compareRaw((int) column.get(rowIdx), (int) column.get(otherRowIdx));
            break;
          case FLOAT_TYPE:
            compared = Type.compareRaw((float) column.get(rowIdx), (float) column.get(otherRowIdx));
            break;
          case LONG_TYPE:
            compared = Type.compareRaw((long) column.get(rowIdx), (long) column.get(otherRowIdx));
            break;
          case DOUBLE_TYPE:
            compared = Type.compareRaw((double) column.get(rowIdx), (double) column.get(otherRowIdx));
            break;
          case BOOLEAN_TYPE:
            compared = Type.compareRaw((boolean) column.get(rowIdx), (boolean) column.get(otherRowIdx));
            break;
          case STRING_TYPE:
            compared = Type.compareRaw((String) column.get(rowIdx), (String) column.get(otherRowIdx));
            break;
          case DATETIME_TYPE:
            compared = Type.compareRaw((DateTime) column.get(rowIdx), (DateTime) column.get(otherRowIdx));
            break;
        }
        if (compared != 0) {
          if (ascending[columnIdx]) {
            return compared;
          } else {
            return -compared;
          }
        }
      }
      return 0;
    }
  }

  /**
   * Sorts the tuples. First, we get an array of indexes by which we sort the data. Then we actually reorder the
   * columns.
   */
  public void sort() {
    final int numTuples = columns.get(0).size();
    ArrayList<Integer> indexes = new ArrayList<>();
    indexes.ensureCapacity(numTuples);
    for (int i = 0; i < numTuples; i++) {
      indexes.add(i);
    }

    TupleComparator comparator = new TupleComparator();
    Collections.sort(indexes, comparator);

    // very inefficient out of place sort
    ArrayList<ArrayList<Object>> newColumns = new ArrayList<ArrayList<Object>>();
    for (int columnIdx = 0; columnIdx < getSchema().numColumns(); columnIdx++) {
      ArrayList<Object> column = new ArrayList<Object>();
      column.ensureCapacity(numTuples);
      for (int rowIdx = 0; rowIdx < numTuples; rowIdx++) {
        column.add(new Object());
      }
      for (int rowIdx = 0; rowIdx < numTuples; rowIdx++) {
        column.set(rowIdx, columns.get(columnIdx).get(indexes.get(rowIdx)));
      }
      newColumns.add(column);
    }
    columns = newColumns;
  }

  @Override
  protected Schema generateSchema() {
    Operator child = getChild();
    if (child == null) {
      return null;
    }
    return child.getSchema();
  }
}