package edu.washington.escience.myria.operator.network.partition;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 *
 * return a fixed integer.
 *
 */
public final class FixValuePartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * the fixed value this partition function returns.
   */
  private final int value;

  /**
   * @param value the fix value this partition function returns.
   *
   * */
  public FixValuePartitionFunction(final int value) {
    super(1);
    this.value = value;
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      result[i] = value;
    }
    return result;
  }
}
