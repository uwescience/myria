package edu.washington.escience.myria.operator.network.partition;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Implementation of a PartitionFunction that use the trivial identity hash.
 * (i.e. a --> a) The attribute to hash on must be an INT column and should
 * represent a workerID
 */
public final class IdentityHashPartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The index of the partition field. */
  @JsonProperty private final int index;

  /**
   * @param index
   *          the index of the partition field.
   */
  @JsonCreator
  public IdentityHashPartitionFunction(
      @JsonProperty(value = "index", required = true) final Integer index) {
    super(null);
    this.index = Objects.requireNonNull(index, "missing property index");
    Preconditions.checkArgument(
        this.index >= 0, "IdentityHash field index cannot take negative value %s", this.index);
  }

  /**
   * @return the index
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param tb
   *          data.
   * @return partitions.
   * */
  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    Preconditions.checkArgument(
        tb.getSchema().getColumnType(index) == Type.INT_TYPE,
        "IdentityHash index column must be of type INT");
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      // Offset by -1 because WorkerIDs are 1-indexed.
      result[i] = tb.getInt(index, i) - 1;
    }
    return result;
  }
}
