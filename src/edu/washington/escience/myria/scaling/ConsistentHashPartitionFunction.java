/**
 * 
 */
package edu.washington.escience.myria.scaling;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.operator.network.partition.PartitionFunction;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * @author valmeida
 * 
 */
public class ConsistentHashPartitionFunction extends PartitionFunction {

  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsistentHashPartitionFunction.class);

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /**
   * The number of replicas on the consistent hash structure. TODO valmeida change to non-fixed value.
   */
  private static final int CONSISTENT_HASH_REPLICATION = 1;

  /** The consistent hash structure. */
  private final ConsistentHash consistentHash;

  /**
   * The index of the partition field.
   */
  @JsonProperty
  private final int index;

  /**
   * The constructor.
   * 
   * @param numPartitions the number of partitions
   * @param index the index of the partition field.
   */
  @JsonCreator
  public ConsistentHashPartitionFunction(@Nullable @JsonProperty("numPartitions") final Integer numPartitions,
      @JsonProperty(value = "index", required = true) final Integer index) {
    super(numPartitions);
    /* TODO(dhalperi) once Jackson actually implements support for required, remove these checks. */
    this.index = Objects.requireNonNull(index, "missing property index");
    Preconditions.checkArgument(this.index >= 0, "SingleFieldHash field index cannot take negative value %s",
        this.index);

    ConsistentHash.initialize(CONSISTENT_HASH_REPLICATION);
    consistentHash = ConsistentHash.getInstance();
    for (int i = 0; i < numPartitions; i++) {
      consistentHash.addWorker(i);
    }
    LOGGER.info("Consistent hashing: " + consistentHash.toString());
  }

  @Override
  public int[] partition(@Nonnull final TupleBatch tb) {
    IntColumnBuilder builder = new IntColumnBuilder();
    final int[] result = new int[tb.numTuples()];
    for (int i = 0; i < result.length; i++) {
      int p = tb.hashCode(i, index);
      builder.appendInt(p);
      result[i] = consistentHash.addHashCode(p);
    }
    tb.addColumn("hash_code", builder.build());
    LOGGER.info("partition tb: " + tb.toString());
    return result;
  }
}
