package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.ReadableColumn;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

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
  private MutableTupleBuffer table;

  /**
   * @param child the source of the tuples.
   */
  public InMemoryOrderBy(final Operator child) {
    this(child, null, null);
  }

  /**
   * A list of indexes into columns that defines the sort order.
   */
  private ArrayList<Integer> indexes;

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
    table = new MutableTupleBuffer(getSchema());
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch nexttb = ans.popFilled();
    if (nexttb != null) {
      return nexttb;
    } else if (ans.numTuples() > 0) {
      return ans.popAny();
    }

    if (table.numTuples() > 0 && getChild().eos()) {
      setEOS();
      return null;
    }

    while (!getChild().eos()) {
      TupleBatch tb = getChild().nextReady();
      if (tb != null) {
        for (int row = 0; row < tb.numTuples(); ++row) {
          List<? extends Column<?>> inputColumns = tb.getDataColumns();
          for (int column = 0; column < tb.numColumns(); ++column) {
            table.put(column, inputColumns.get(column), row);
          }
        }
      } else if (!getChild().eos()) {
        return null;
      }
    }

    Preconditions.checkState(getChild().eos());

    sort();

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
      int i = 0;
      for (int columnIdx : sortColumns) {
        int compared = 0;
        switch (getSchema().getColumnType(columnIdx)) {
          case INT_TYPE:
            compared =
                Type.compareRaw(
                    table.getInt(columnIdx, rowIdx), table.getInt(columnIdx, otherRowIdx));
            break;
          case FLOAT_TYPE:
            compared =
                Type.compareRaw(
                    table.getFloat(columnIdx, rowIdx), table.getFloat(columnIdx, otherRowIdx));
            break;
          case LONG_TYPE:
            compared =
                Type.compareRaw(
                    table.getLong(columnIdx, rowIdx), table.getLong(columnIdx, otherRowIdx));
            break;
          case DOUBLE_TYPE:
            compared =
                Type.compareRaw(
                    table.getDouble(columnIdx, rowIdx), table.getDouble(columnIdx, otherRowIdx));
            break;
          case BOOLEAN_TYPE:
            compared =
                Type.compareRaw(
                    table.getBoolean(columnIdx, rowIdx), table.getBoolean(columnIdx, otherRowIdx));
            break;
          case STRING_TYPE:
            compared =
                Type.compareRaw(
                    table.getString(columnIdx, rowIdx), table.getString(columnIdx, otherRowIdx));
            break;
          case DATETIME_TYPE:
            compared =
                Type.compareRaw(
                    table.getDateTime(columnIdx, rowIdx),
                    table.getDateTime(columnIdx, otherRowIdx));
            break;
        }
        if (compared != 0) {
          if (ascending[i]) {
            return compared;
          } else {
            return -compared;
          }
        }
        i++;
      }
      return 0;
    }
  }

  /**
   * Sorts the tuples. First, we get an array of indexes by which we sort the data. Then we actually reorder the rows.
   */
  public void sort() {
    final int numTuples = table.numTuples();
    indexes = new ArrayList<>();
    indexes.ensureCapacity(numTuples);
    for (int i = 0; i < numTuples; i++) {
      indexes.add(i);
    }

    TupleComparator comparator = new TupleComparator();
    Collections.sort(indexes, comparator);

    for (int rowIdx = 0; rowIdx < numTuples; rowIdx++) {
      for (int columnIdx = 0; columnIdx < getSchema().numColumns(); columnIdx++) {
        int sourceRow = indexes.get(rowIdx);
        int tupleIdx = table.getTupleIndexInContainingTB(sourceRow);
        ReadableColumn hashTblColumn = table.getColumns(sourceRow)[columnIdx];
        ans.put(columnIdx, hashTblColumn, tupleIdx);
      }
    }
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
