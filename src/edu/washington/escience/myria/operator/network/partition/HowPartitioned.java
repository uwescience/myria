package edu.washington.escience.myria.operator.network.partition;

import java.util.Set;

import javax.annotation.Nullable;

import jersey.repackaged.com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;

/**
 * How a dataset is partitioned. There are two fields being recorded for now, the partition function and the sequence of
 * worker IDs. More information can be added in the future if needed.
 */
public class HowPartitioned {

  /** The partition function which was used to partition the dataset. Null means unknown. */
  @JsonProperty private PartitionFunction pf = null;
  /** The sequence of workers that the dataset is partitioned on. Null means unknown. */
  @JsonProperty private ImmutableSet<Integer> workers = null;

  /**
   * @param pf the partition function.
   * @param workers the sequence of workers.
   */
  public HowPartitioned(@Nullable final PartitionFunction pf, @Nullable final int[] workers) {
    this.pf = pf;
    if (workers != null) {
      this.workers = ImmutableSet.copyOf(Ints.asList(workers));
    }
  }

  /**
   * Static function to create a HowPartitioned object.
   *
   * @param pf the partition function.
   * @param workers the sequence of workers. *
   * @return a new HowPartitioned reference to the specified relation.
   */
  @JsonCreator
  public static HowPartitioned of(
      @JsonProperty("pf") final PartitionFunction pf,
      @JsonProperty("workers") final int[] workers) {
    return new HowPartitioned(pf, workers);
  }

  /** @return the partition function. */
  public PartitionFunction getPf() {
    return pf;
  }

  /** @return the workers. */
  public Set<Integer> getWorkers() {
    return workers;
  }
}
