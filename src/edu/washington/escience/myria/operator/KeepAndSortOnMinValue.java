package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.impl.list.mutable.primitive.IntArrayList;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.MutableTupleBuffer;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Keeps min value. It adds newly met unique tuples into a buffer so that the source TupleBatches are not referenced.
 * This implementation reduces memory consumption.
 * */
public final class KeepAndSortOnMinValue extends StreamingState {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for this class.
   * */
  static final Logger LOGGER = LoggerFactory.getLogger(KeepAndSortOnMinValue.class);

  /**
   * Indices to unique tuples.
   * */
  private transient IntObjectHashMap<IntArrayList> uniqueTupleIndices;

  /**
   * The buffer for storing unique tuples.
   * */
  private transient MutableTupleBuffer uniqueTuples = null;

  /** column indices of the key. */
  private final int[] keyColIndices;
  /** column indices of the key as a set. */
  private final Set<Integer> keyColIndicesSet;
  /** column indices of the value. */
  private final int[] valueColIndices;

  /**
   *
   * @param keyColIndices column indices of the key
   * @param valueColIndices column indices of the value
   */
  public KeepAndSortOnMinValue(final int[] keyColIndices, final int[] valueColIndices) {
    this.keyColIndices = Arrays.copyOf(keyColIndices, keyColIndices.length);
    keyColIndicesSet = ImmutableSet.copyOf(Ints.asList(keyColIndices));
    this.valueColIndices = Arrays.copyOf(valueColIndices, valueColIndices.length);
  }

  @Override
  public void cleanup() {
    uniqueTuples = null;
    uniqueTupleIndices = null;
  }

  /**
   * Check if a tuple in uniqueTuples should be replaced by a given tuple
   *
   * @param index the row index of the tuple in uniqueTuples
   * @param columns the columns of the given tuple
   * @param row the row index of the given tuple
   * @return true if should be replaced by
   * */
  private boolean shouldReplace(
      final int index, final List<? extends Column<?>> columns, final int row) {
    for (int valueColIndex : valueColIndices) {
      Column<?> column = columns.get(valueColIndex);
      switch (column.getType()) {
        case INT_TYPE:
          {
            int t1 = column.getInt(row);
            int t2 = uniqueTuples.getInt(valueColIndex, index);
            if (t1 < t2) {
              return true;
            }
            if (t1 > t2) {
              return false;
            }
            break;
          }
        case LONG_TYPE:
          {
            long t1 = column.getLong(row);
            long t2 = uniqueTuples.getLong(valueColIndex, index);
            if (t1 < t2) {
              return true;
            }
            if (t1 > t2) {
              return false;
            }
            break;
          }
        case FLOAT_TYPE:
          {
            float t1 = column.getFloat(row);
            float t2 = uniqueTuples.getFloat(valueColIndex, index);
            if (t1 < t2) {
              return true;
            }
            if (t1 > t2) {
              return false;
            }
            break;
          }
        case DOUBLE_TYPE:
          {
            double t1 = column.getDouble(row);
            double t2 = uniqueTuples.getDouble(valueColIndex, index);
            if (t1 < t2) {
              return true;
            }
            if (t1 > t2) {
              return false;
            }
            break;
          }
        default:
          throw new IllegalStateException(
              "type " + column.getType() + " is not supported in KeepMinValue.replace()");
      }
    }
    return false;
  }

  /**
   * Do duplicate elimination for tb.
   *
   * @param tb the TupleBatch for performing DupElim.
   * @return the duplicate eliminated TB.
   * */
  protected TupleBatch keepMinValue(final TupleBatch tb) {
    final int numTuples = tb.numTuples();
    if (numTuples <= 0) {
      return tb;
    }
    doReplace.inputTB = tb;
    final List<? extends Column<?>> columns = tb.getDataColumns();
    final BitSet toRemove = new BitSet(numTuples);
    for (int i = 0; i < numTuples; ++i) {
      final int nextIndex = uniqueTuples.numTuples();
      final int cntHashCode = HashUtils.hashSubRow(tb, keyColIndices, i);
      IntArrayList tupleIndexList = uniqueTupleIndices.get(cntHashCode);
      doReplace.unique = true;
      if (tupleIndexList == null) {
        tupleIndexList = new IntArrayList();
        tupleIndexList.add(nextIndex);
        uniqueTupleIndices.put(cntHashCode, tupleIndexList);
      } else {
        doReplace.replaced = false;
        doReplace.sourceRow = i;
        tupleIndexList.forEach(doReplace);
        if (!doReplace.unique && !doReplace.replaced) {
          toRemove.set(i);
        }
      }
      if (doReplace.unique) {
        for (int j = 0; j < tb.numColumns(); ++j) {
          uniqueTuples.put(j, columns.get(j), i);
        }
        tupleIndexList.add(nextIndex);
      }
    }
    return tb.filterOut(toRemove);
  }

  @Override
  public Schema getSchema() {
    return getOp().getInputSchema();
  }

  @Override
  public void init(final ImmutableMap<String, Object> execEnvVars) {
    uniqueTupleIndices = new IntObjectHashMap<IntArrayList>();
    uniqueTuples = new MutableTupleBuffer(getSchema());
    doReplace = new ReplaceProcedure();
  }

  @Override
  public TupleBatch update(final TupleBatch tb) {
    TupleBatch newtb = keepMinValue(tb);
    if (newtb.numTuples() > 0 || newtb.isEOI()) {
      return newtb;
    }
    return null;
  }

  @Override
  public List<TupleBatch> exportState() {
    MutableTupleBuffer tmp = uniqueTuples.clone();
    sortOn(tmp, valueColIndices);
    return tmp.getAll();
  }

  /**
   * Traverse through the list of tuples and replace old values.
   * */
  private transient ReplaceProcedure doReplace;

  /**
   * Traverse through the list of tuples with the same hash code.
   * */
  private final class ReplaceProcedure implements IntProcedure {

    /** serial version id. */
    private static final long serialVersionUID = 1L;

    /** row index of the tuple. */
    private int sourceRow;

    /** input TupleBatch. */
    private TupleBatch inputTB;

    /** if found a replacement. */
    private boolean replaced;

    /** if the given tuple doesn't exist. */
    private boolean unique;

    @Override
    public void value(final int destRow) {
      if (TupleUtils.tupleEquals(
          inputTB, keyColIndices, sourceRow, uniqueTuples, keyColIndices, destRow)) {
        unique = false;
        if (shouldReplace(destRow, inputTB.getDataColumns(), sourceRow)) {
          for (int i = 0; i < uniqueTuples.numColumns(); ++i) {
            if (!keyColIndicesSet.contains(i)) {
              // replace the whole tuple except key columns.
              uniqueTuples.replace(i, destRow, inputTB.getDataColumns().get(i), sourceRow);
            }
          }
          replaced = true;
        }
      }
    }
  };

  /**
   * sort the given TukpleBuffer on a column.
   *
   * @param tuples tuples
   * @param col column index
   */
  private void sortOn(final MutableTupleBuffer tuples, final int[] col) {
    quicksort(tuples, col, 0, tuples.numTuples() - 1);
  }

  /**
   * quick sort on column col, tuple with smaller values are put in the front.
   *
   * @param tuples tuples
   * @param col the column index
   * @param low lower bound
   * @param high upper bound
   */
  private void quicksort(
      final MutableTupleBuffer tuples, final int[] col, final int low, final int high) {
    int i = low, j = high;
    int pivot = low + (high - low) / 2;

    while (i <= j) {
      while (compare(tuples, col, i, pivot) < 0) {
        i++;
      }
      while (compare(tuples, col, j, pivot) > 0) {
        j--;
      }
      if (i <= j) {
        if (i != j) {
          if (i == pivot) {
            pivot = j;
          } else if (j == pivot) {
            pivot = i;
          }
          for (int c = 0; c < tuples.numColumns(); ++c) {
            tuples.swap(c, i, j);
          }
        }
        i++;
        j--;
      }
    }
    if (low < j) {
      quicksort(tuples, col, low, j);
    }
    if (i < high) {
      quicksort(tuples, col, i, high);
    }
  }

  /**
   * compare a value in a column with pivot.
   *
   * @param tuples tuples
   * @param columns the column indices
   * @param row row index to compare with
   * @param pivot the index of the pivot value
   * @return if the value is smaller than (-1), equal to (0) or bigger than (1) pivot
   */
  public int compare(
      final MutableTupleBuffer tuples, final int[] columns, final int row, final int pivot) {
    for (int column : columns) {
      Type t = getSchema().getColumnType(column);
      switch (t) {
        case LONG_TYPE:
          {
            long t1 = tuples.getLong(column, row);
            long t2 = tuples.getLong(column, pivot);
            if (t1 < t2) {
              return -1;
            }
            if (t1 > t2) {
              return 1;
            }
            break;
          }
        case INT_TYPE:
          {
            int t1 = tuples.getInt(column, row);
            int t2 = tuples.getInt(column, pivot);
            if (t1 < t2) {
              return -1;
            }
            if (t1 > t2) {
              return 1;
            }
            break;
          }
        case FLOAT_TYPE:
          {
            float t1 = tuples.getFloat(column, row);
            float t2 = tuples.getFloat(column, pivot);
            if (t1 < t2) {
              return -1;
            }
            if (t1 > t2) {
              return 1;
            }
            break;
          }
        case DOUBLE_TYPE:
          {
            double t1 = tuples.getDouble(column, row);
            double t2 = tuples.getDouble(column, pivot);
            if (t1 < t2) {
              return -1;
            }
            if (t1 > t2) {
              return 1;
            }
            break;
          }
        default:
          throw new IllegalStateException("type " + t + " is not supported");
      }
    }
    return 0;
  }

  @Override
  public int numTuples() {
    return uniqueTuples.numTuples();
  }

  @Override
  public StreamingState duplicate() {
    return new KeepAndSortOnMinValue(keyColIndices, valueColIndices);
  }
}
