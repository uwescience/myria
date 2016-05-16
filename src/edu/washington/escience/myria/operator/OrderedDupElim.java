package edu.washington.escience.myria.operator;

import java.util.BitSet;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleUtils;

/**
 * A Duplicate Elimination Operator that works on ordered input.
 */
public final class OrderedDupElim extends UnaryOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The tuple batch containing the last emitted tuple. */
  private TupleBatch lastTupleBatch = null;
  /** The row of the last emitted tuple in the last emitted tuple batch. */
  private int lastTupleRow;

  /**
   * The order in which columns of the input tuple are scanned. So named because we want to scan in the reverse order
   * that columns were sorted to find differences as quickly as possible.
   */
  private int[] invSortColumns;

  /**
   * A duplicate elimination operator that works on ordered input. This constructor assumes that the child columns were
   * sorted in the same order in which they were input.
   *
   * @param child the source of the tuples.
   */
  public OrderedDupElim(final Operator child) {
    this(child, null);
  }

  /**
   * A duplicate elimination operator that works on ordered input. If present, <code>sortColumns</code> specifies the
   * order in which the columns of the input data were sorted, and will look for differences from the last column to the
   * first.
   *
   * @param child the source of the tuples.
   * @param sortColumns the order in which the columns of the input tuples are sorted.
   */
  public OrderedDupElim(final Operator child, @Nullable final int[] sortColumns) {
    super(child);
    invSortColumns = reverseArrayOrNull(sortColumns);
  }

  /**
   * Utility function to reverse an int array.
   *
   * @param input the array to be flipped, which may be null.
   * @return a copy of input with the elements reversed, or null.
   */
  private int[] reverseArrayOrNull(@Nullable final int[] input) {
    if (input == null) {
      return null;
    }
    int[] output = new int[input.length];
    for (int i = 0; i < input.length; ++i) {
      output[i] = input[input.length - i - 1];
    }
    return output;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws Exception {
    Schema schema = Objects.requireNonNull(getSchema());
    /* Assume the columns are sorted in order by default. */
    if (invSortColumns == null) {
      int numColumns = schema.numColumns();
      invSortColumns = new int[numColumns];
      for (int i = 0; i < numColumns; ++i) {
        invSortColumns[i] = numColumns - i - 1;
      }
    }
  };

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    TupleBatch tb = getChild().nextReady();
    if (tb == null) {
      return null;
    }

    BitSet output = new BitSet(tb.numTuples());
    for (int row = 0; row < tb.numTuples(); ++row) {
      if (lastTupleBatch == null
          || !TupleUtils.tupleEquals(
              tb, invSortColumns, row, lastTupleBatch, invSortColumns, lastTupleRow)) {
        output.set(row);
        lastTupleBatch = tb;
        lastTupleRow = row;
      }
    }

    return tb.filter(output);
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
