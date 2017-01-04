package edu.washington.escience.myria.operator.network.distribute;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Distribute function for HyperCube Shuffle. */
public final class HyperCubeDistributeFunction extends DistributeFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * @param hyperCubeDimensions the sizes of each dimension of the hypercube.
   * @param hashedColumns which fields are hashed.
   * @param mappedHCDimensions mapped hypercube dimensions of hashed columns.
   * @param cellPartition mapping from cells to destinations.
   */
  @JsonCreator
  public HyperCubeDistributeFunction(
      @JsonProperty("hyperCubeDimensions") final int[] hyperCubeDimensions,
      @JsonProperty("hashedColumns") final int[] hashedColumns,
      @JsonProperty("mappedHCDimensions") final int[] mappedHCDimensions,
      @JsonProperty("cellPartition") final List<List<Integer>> cellPartition) {
    super(new HyperCubePartitionFunction(hyperCubeDimensions, hashedColumns, mappedHCDimensions));
    partitionToDestination = cellPartition;
  }

  /** @return all destinations */
  public List<Integer> getAllDestinations() {
    List<Integer> values = new ArrayList<Integer>();
    for (List<Integer> l : partitionToDestination) {
      values.addAll(l);
    }
    return values;
  }

  @Override
  public void setDestinations(int numWorker, int numOperatorId) {
    partitionFunction.setNumPartitions(partitionToDestination.size());
    /* TODO: should this be the same as the product of the sizes of all dimensions? */
  }
}
