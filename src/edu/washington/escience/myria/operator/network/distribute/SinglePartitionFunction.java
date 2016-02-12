package edu.washington.escience.myria.operator.network.distribute;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonCreator;

import edu.washington.escience.myria.storage.TupleBatch;

/** return a fixed integer. */
public final class SinglePartitionFunction extends PartitionFunction {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   */
  @JsonCreator
  public SinglePartitionFunction() {
    setNumPartitions(1);
  }

  @Override
  public TupleBatch[] partition(@Nonnull final TupleBatch tb) {
    return new TupleBatch[] {tb};
  }
}
