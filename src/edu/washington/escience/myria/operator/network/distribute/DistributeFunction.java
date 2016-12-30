package edu.washington.escience.myria.operator.network.distribute;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.storage.TupleBatch;

/**
 * A dataset is distributed by two steps: First, using a partition function to generate a partition for each tuple;
 * Second, mapping each partition to a set of destinations. A destination corresponds to an output channel ID
 * corresponding to a (worker ID, operator ID) pair.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
  @Type(value = BroadcastDistributeFunction.class, name = "Broadcast"),
  @Type(value = HyperCubeDistributeFunction.class, name = "HyperCube"),
  @Type(value = HashDistributeFunction.class, name = "Hash"),
  @Type(value = RoundRobinDistributeFunction.class, name = "RoundRobin"),
  @Type(value = IdentityDistributeFunction.class, name = "Identity")
})
public abstract class DistributeFunction implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The partition function. */
  protected PartitionFunction partitionFunction;

  /** The mapping from partitions to destinations. */
  protected List<List<Integer>> partitionToDestination;

  /** number of destinations. */
  protected int numDestinations;

  /**
   * @param partitionFunction partition function.
   */
  public DistributeFunction(final PartitionFunction partitionFunction) {
    this.partitionFunction = partitionFunction;
  }

  /**
   * @param data the input data
   * @return a list of tuple batch lists, each represents output data of one destination.
   */
  public List<List<TupleBatch>> distribute(@Nonnull final TupleBatch data) {
    List<List<TupleBatch>> result = new ArrayList<List<TupleBatch>>();
    if (data.isEOI()) {
      for (int i = 0; i < numDestinations; ++i) {
        result.add(Lists.newArrayList(data));
      }
    } else {
      for (int i = 0; i < numDestinations; ++i) {
        result.add(new ArrayList<TupleBatch>());
      }
      TupleBatch[] tbs = partitionFunction.partition(data);
      for (int i = 0; i < tbs.length; ++i) {
        for (int channelIdx : partitionToDestination.get(i)) {
          result.get(channelIdx).add(tbs[i]);
        }
      }
    }
    return result;
  }

  /**
   * @param numWorker the number of workers to distribute on
   * @param numOperatorId the number of involved operator IDs
   */
  public abstract void setNumDestinations(final int numWorker, final int numOperatorId);
}
