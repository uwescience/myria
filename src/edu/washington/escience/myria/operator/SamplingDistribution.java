package edu.washington.escience.myria.operator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;

public class SamplingDistribution extends UnaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The output schema. */
  private static final Schema SCHEMA = Schema.of(
      ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE),
      ImmutableList.of("WorkerID", "PartitionSize", "SampleSize"));

  /** Total number of tuples to sample. */
  private final int sampleSize;

  /** True if the sampling is WithoutReplacement. WithReplacement otherwise. */
  private final boolean isWithoutReplacement;

  /** Value to seed the random generator with. Null if no specified seed value. */
  protected Long randomSeed;

  public SamplingDistribution(final int sampleSize,
      final boolean isWithoutReplacement, final Operator child) {
    this(sampleSize, isWithoutReplacement, child, null);
  }

  public SamplingDistribution(final int sampleSize,
      final boolean isWithoutReplacement, final Operator child, Long randomSeed) {
    super(child);
    this.sampleSize = sampleSize;
    this.isWithoutReplacement = isWithoutReplacement;
    this.randomSeed = randomSeed;
    Preconditions.checkState(sampleSize >= 0,
        "Sample size cannot be negative: %s", sampleSize);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (getChild().eos()) {
      return null;
    }

    // Distribution of the tuples across the workers.
    // Value at index i == # of tuples on worker i.
    ArrayList<Integer> tupleCounts = new ArrayList<Integer>();

    // Total number of tuples across all workers.
    int totalTupleCount = 0;

    // Drain out all the workerID and partitionSize info.
    while (!getChild().eos()) {
      TupleBatch tb = getChild().nextReady();
      if (tb == null) {
        continue;
      }
      Type col0Type = tb.getSchema().getColumnType(0);
      Type col1Type = tb.getSchema().getColumnType(1);
      for (int i = 0; i < tb.numTuples(); i++) {
        int workerID;
        if (col0Type == Type.INT_TYPE) {
          workerID = tb.getInt(0, i);
        } else if (col0Type == Type.LONG_TYPE) {
          workerID = (int) tb.getLong(0, i);
        } else {
          throw new DbException("WorkerID must be of type INT or LONG");
        }
        Preconditions.checkState(workerID > 0, "WorkerID must be > 0");
        // Ensure the future tupleCounts.set(workerID, -) call will work.
        for (int j = tupleCounts.size(); j < workerID; j++) {
          tupleCounts.add(0);
        }

        int partitionSize;
        if (col1Type == Type.INT_TYPE) {
          partitionSize = tb.getInt(1, i);
        } else if (col1Type == Type.LONG_TYPE) {
          partitionSize = (int) tb.getLong(1, i);
        } else {
          throw new DbException("PartitionSize must be of type INT or LONG");
        }
        Preconditions.checkState(partitionSize >= 0,
            "Worker cannot have a negative PartitionSize: %s", sampleSize);
        tupleCounts.set(workerID - 1, partitionSize);
        totalTupleCount += partitionSize;
      }
    }
    Preconditions.checkState(sampleSize <= totalTupleCount,
        "Cannot extract %s samples from a population of size %s", sampleSize,
        totalTupleCount);

    // Generate a random distribution across the workers.
    int[] sampleCounts;
    if (isWithoutReplacement) {
      sampleCounts = withoutReplacementDistribution(tupleCounts, sampleSize);
    } else {
      sampleCounts = withReplacementDistribution(tupleCounts, sampleSize);
    }

    // Build and return a TupleBatch with the distribution.
    IntColumnBuilder wIdCol = new IntColumnBuilder();
    IntColumnBuilder tupCountCol = new IntColumnBuilder();
    IntColumnBuilder sampCountCol = new IntColumnBuilder();
    for (int i = 0; i < tupleCounts.size(); i++) {
      wIdCol.appendInt(i + 1);
      tupCountCol.appendInt(tupleCounts.get(i));
      sampCountCol.appendInt(sampleCounts[i]);
    }
    ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
    columns.add(wIdCol.build(), tupCountCol.build(), sampCountCol.build());
    return new TupleBatch(SCHEMA, columns.build());
  }

  private int[] withReplacementDistribution(List<Integer> tupleCounts,
      int sampleSize) {
    int[] distribution = new int[tupleCounts.size()];
    Random rand = getRandom();
    int totalTupleCount = 0;
    for (int val : tupleCounts)
      totalTupleCount += val;

    for (int i = 0; i < sampleSize; i++) {
      int sampleTupleIdx = rand.nextInt(totalTupleCount);
      // Assign this tuple to the workerID that holds this sampleTupleIdx.
      int tupleOffset = 0;
      for (int j = 0; j < tupleCounts.size(); j++) {
        if (sampleTupleIdx < tupleCounts.get(j) + tupleOffset) {
          distribution[j] += 1;
          break;
        }
        tupleOffset += tupleCounts.get(j);
      }
    }
    return distribution;
  }

  private int[] withoutReplacementDistribution(List<Integer> tupleCounts,
      int sampleSize) {
    int[] distribution = new int[tupleCounts.size()];
    Random rand = getRandom();
    int totalTupleCount = 0;
    for (int val : tupleCounts)
      totalTupleCount += val;
    List<Integer> logicalTupleCounts = new ArrayList<Integer>(tupleCounts);

    for (int i = 0; i < sampleSize; i++) {
      int sampleTupleIdx = rand.nextInt(totalTupleCount - i);
      // Assign this tuple to the workerID that holds this sampleTupleIdx.
      int tupleOffset = 0;
      for (int j = 0; j < logicalTupleCounts.size(); j++) {
        if (sampleTupleIdx < logicalTupleCounts.get(j) + tupleOffset) {
          distribution[j] += 1;
          // Cannot sample the same tuple, so pretend it doesn't exist anymore.
          logicalTupleCounts.set(j, logicalTupleCounts.get(j) - 1);
          break;
        }
        tupleOffset += logicalTupleCounts.get(j);
      }
    }
    return distribution;
  }

  private Random getRandom() {
    Random rand;
    if (randomSeed != null) {
      rand = new Random(randomSeed);
    } else {
      rand = new Random();
    }
    return rand;
  }

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

}
