package edu.washington.escience.myria.operator.network.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Implementation of a PartitionFunction that decides based on the raw values of an integer field.
 */
public final class RawValuePartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The index of the partition field. */
  @JsonProperty
  private final int index;

  /**
   * @param index the index of the partition field.
   */
  @JsonCreator
  public RawValuePartitionFunction(
      @JsonProperty(value = "index", required = true) final Integer index) {
    super(null);
    this.index = java.util.Objects.requireNonNull(index, "missing property index");
    Preconditions.checkArgument(this.index >= 0,
        "RawValue field index cannot take negative value %s", this.index);
  }

  /**
   * @return the index
   */
  public int getIndex() {
    return index;
  }

  /**
   * @param tb data.
   * @return partitions.
   * */
  @Override
  public int[] partition(final TupleBatch tb) {
    Preconditions.checkArgument(tb.getSchema().getColumnType(index) == Type.INT_TYPE,
        "RawValue index column must be of type INT");
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      // Offset by -1 because WorkerIDs are 1-indexed.
      result[i] = tb.getInt(index, i) - 1;
    }
    return result;
  }
}
