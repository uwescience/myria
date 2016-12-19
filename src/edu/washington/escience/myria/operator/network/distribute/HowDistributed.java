package edu.washington.escience.myria.operator.network.distribute;

import java.util.List;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

/** How is a dataset distributed. */
public class HowDistributed {

  /** The distribute function used to distirbute the dataset. Null means unknown. */
  @JsonProperty private DistributeFunction df = null;
  /** The sequence of workers that the dataset is partitioned on. Null means unknown. */
  @JsonProperty private ImmutableList<Integer> workers = null;

  /** @param df the distribute function.
   * @param workers the sequence of workers. */
  public HowDistributed(@Nullable final DistributeFunction df, @Nullable final int[] workers) {
    this.df = df;
    if (workers != null) {
      this.workers = ImmutableList.copyOf(Ints.asList(workers));
    }
  }

  /** Static function to create a HowPartitioned object.
   *
   * @param df the distribute function.
   * @param workers the sequence of workers. *
   * @return a new HowPartitioned reference to the specified relation. */
  @JsonCreator
  public static HowDistributed of(
      @JsonProperty("df") final DistributeFunction df,
      @JsonProperty("workers") final int[] workers) {
    return new HowDistributed(df, workers);
  }

  /** @return the distribute function. */
  public DistributeFunction getDf() {
    return df;
  }

  /** @return the workers. */
  public List<Integer> getWorkers() {
    return workers;
  }
}
