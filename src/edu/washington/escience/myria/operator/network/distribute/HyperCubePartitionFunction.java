package edu.washington.escience.myria.operator.network.distribute;

import java.util.BitSet;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.HashUtils;

/**
 * Multiple field multiple dimension hash partition function for HyperCubeJoin.
 */
public final class HyperCubePartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @JsonProperty private final int[] hashedColumns;
  @JsonProperty private final int[] mappedHCDimensions;
  @JsonProperty private final int[] hyperCubeDimensions;

  /**
   * @param hyperCubeDimensions the sizes of each dimension of the hypercube.
   * @param hashedColumns which fields are hashed.
   * @param mappedHCDimensions mapped hypercube dimensions of hashed columns.
   */
  public HyperCubePartitionFunction(
      final int[] hyperCubeDimensions, final int[] hashedColumns, final int[] mappedHCDimensions) {
    super();
    this.hashedColumns = hashedColumns;
    this.hyperCubeDimensions = hyperCubeDimensions;
    this.mappedHCDimensions = mappedHCDimensions;
  }

  @Override
  public TupleBatch[] partition(final TupleBatch tb) {
    BitSet[] partitions = new BitSet[numPartitions()];
    for (int i = 0; i < tb.numTuples(); i++) {
      int p = 0;
      for (int j = 0; j < hashedColumns.length; j++) {
        p +=
            Math.floorMod(
                HashUtils.hashSubRow(tb, new int[] {hashedColumns[j]}, i, mappedHCDimensions[j]),
                hyperCubeDimensions[mappedHCDimensions[j]]);
        if (p != hashedColumns.length - 1) {
          p *= hyperCubeDimensions[mappedHCDimensions[j]];
        }
      }
      partitions[p].set(i);
    }
    TupleBatch[] tbs = new TupleBatch[numPartitions()];
    for (int i = 0; i < tbs.length; ++i) {
      tbs[i] = tb.filter(partitions[i]);
    }
    return tbs;
  }
}
