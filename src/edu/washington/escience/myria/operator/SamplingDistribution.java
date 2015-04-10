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

  public SamplingDistribution(final int sampleSize,
      final boolean isWithoutReplacement, final Operator child) {
    super(child);
    this.sampleSize = sampleSize;
    this.isWithoutReplacement = isWithoutReplacement;
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {r
    if (getChild().eos()) {
      return null;
    }

    // Distribution of the tuples across the workers.
    // Value at index i == # of tuples on mapped worker.
    List<Integer> tupleCounts = new ArrayList<Integer>();

    // Maps indices in tupleCounts to the actual WorkerID association.
    // Value at index i == actual workerID of tupleCount.
    List<Integer> workerMapping = new ArrayList<Integer>();

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
        workerMapping.add(workerID);

        int partitionSize;
        if (col1Type == Type.INT_TYPE) {
          partitionSize = tb.getInt(1, i);
        } else if (col1Type == Type.LONG_TYPE) {
          partitionSize = (int) tb.getLong(1, i);
        } else {
          throw new DbException("PartitionSize must be of type INT or LONG");
        }
        tupleCounts.add(partitionSize);
        totalTupleCount += partitionSize;
        System.out.println("workerID: " + workerID + ", partitionSize: "
            + partitionSize);
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
    for (int i = 0; i < workerMapping.size(); i++) {
      wIdCol.appendInt(workerMapping.get(i));
      tupCountCol.appendInt(tupleCounts.get(i));
      sampCountCol.appendInt(sampleCounts[i]);
    }
    ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
    columns.add(wIdCol.build(), tupCountCol.build(), sampCountCol.build());
    return new TupleBatch(SCHEMA, columns.build());
  }

  private static int[] withReplacementDistribution(List<Integer> tupleCounts,
      int sampleSize) {
    int[] distribution = new int[tupleCounts.size()];
    Random rand = new Random();
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

  private static int[] withoutReplacementDistribution(
      List<Integer> tupleCounts, int sampleSize) {
    int[] distribution = new int[tupleCounts.size()];
    Random rand = new Random();
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

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

}
