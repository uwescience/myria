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
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.SamplingType;

public class SamplingDistribution extends UnaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The output schema. */
  private static final Schema SCHEMA = Schema.of(ImmutableList.of(
      Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE),
      ImmutableList.of("WorkerID", "StreamSize", "SampleSize", "SampleType"));

  /** Total number of tuples to sample. */
  private int sampleSize;

  /** True if using a percentage instead of a specific tuple count. */
  private boolean isPercentageSample = false;

  /** Percentage of total tuples to sample. */
  private float samplePercentage;

  /** The type of sampling to perform. */
  private final SamplingType sampleType;

  /** Random generator used for creating the distribution. */
  private Random rand;

  private SamplingDistribution(Operator child, SamplingType sampleType,
      Long randomSeed) {
    super(child);
    this.sampleType = sampleType;
    this.rand = new Random();
    if (randomSeed != null) {
      this.rand.setSeed(randomSeed);
    }
  }

  /**
   * Instantiate a SamplingDistribution operator using a specific sample size.
   * 
   * @param sampleSize
   *          total samples to create a distribution for.
   * @param sampleType
   *          the type of sampling distribution to create
   * @param child
   *          extracts (WorkerID, PartitionSize, StreamSize) information from
   *          this child.
   * @param randomSeed
   *          value to seed the random generator with. null if no specified seed
   */
  public SamplingDistribution(Operator child, int sampleSize,
      SamplingType sampleType, Long randomSeed) {
    this(child, sampleType, randomSeed);
    this.sampleSize = sampleSize;
    Preconditions.checkState(this.sampleSize >= 0,
        "Sample Size must be >= 0: %s", this.sampleSize);
  }

  /**
   * Instantiate a SamplingDistribution operator using a percentage of total
   * tuples.
   *
   * @param samplePercentage
   *          percentage of total samples to create a distribution for.
   * @param sampleType
   *          the type of sampling distribution to create
   * @param child
   *          extracts (WorkerID, PartitionSize, StreamSize) information from
   *          this child.
   * @param randomSeed
   *          value to seed the random generator with. null if no specified seed
   */
  public SamplingDistribution(Operator child, float samplePercentage,
      SamplingType sampleType, Long randomSeed) {
    this(child, sampleType, randomSeed);
    this.isPercentageSample = true;
    this.samplePercentage = samplePercentage;
    Preconditions.checkState(samplePercentage >= 0 && samplePercentage <= 100,
        "Sample Percentage must be >= 0 && <= 100: %s", samplePercentage);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    if (getChild().eos()) {
      return null;
    }

    // Distribution of the tuples across the workers.
    // Value at index i == # of tuples on worker i.
    ArrayList<Integer> tupleCounts = new ArrayList<Integer>();

    // Distribution of the actual stream size across the workers.
    // May be different from tupleCounts if worker i pre-sampled the data.
    // Value at index i == # of tuples in stream on worker i.
    ArrayList<Integer> streamCounts = new ArrayList<Integer>();

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
      boolean hasStreamSize = false;
      Type col2Type = null;
      if (tb.getSchema().numColumns() > 2) {
        hasStreamSize = true;
        col2Type = tb.getSchema().getColumnType(2);
      }
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
        // Ensure the future .set(workerID, -) calls will work.
        for (int j = tupleCounts.size(); j < workerID; j++) {
          tupleCounts.add(0);
          streamCounts.add(0);
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
            "Worker cannot have a negative PartitionSize: %s", partitionSize);
        tupleCounts.set(workerID - 1, partitionSize);
        totalTupleCount += partitionSize;

        int streamSize = partitionSize;
        if (hasStreamSize) {
          if (col2Type == Type.INT_TYPE) {
            streamSize = tb.getInt(2, i);
          } else if (col2Type == Type.LONG_TYPE) {
            streamSize = (int) tb.getLong(2, i);
          } else {
            throw new DbException("StreamSize must be of type INT or LONG");
          }
          Preconditions.checkState(partitionSize >= 0,
              "Worker cannot have a negative StreamSize: %d", streamSize);
        }
        streamCounts.set(workerID - 1, streamSize);
      }
    }
    // Convert samplePct to sampleSize if using a percentage sample.
    if (isPercentageSample) {
      sampleSize = Math.round(totalTupleCount * (samplePercentage / 100));
    }
    Preconditions.checkState(sampleSize >= 0 && sampleSize <= totalTupleCount,
        "Cannot extract %s samples from a population of size %s", sampleSize,
        totalTupleCount);

    // Generate a random distribution across the workers.
    int[] sampleCounts;
    if (sampleType == SamplingType.WR) {
      sampleCounts = withReplacementDistribution(tupleCounts, sampleSize);
    } else if (sampleType == SamplingType.WoR) {
      sampleCounts = withoutReplacementDistribution(tupleCounts, sampleSize);
    } else {
      throw new DbException("Invalid sampleType: " + sampleType);
    }

    // Build and return a TupleBatch with the distribution.
    IntColumnBuilder wIdCol = new IntColumnBuilder();
    IntColumnBuilder streamSizeCol = new IntColumnBuilder();
    IntColumnBuilder sampCountCol = new IntColumnBuilder();
    StringColumnBuilder sampTypeCol = new StringColumnBuilder();
    for (int i = 0; i < streamCounts.size(); i++) {
      wIdCol.appendInt(i + 1);
      streamSizeCol.appendInt(streamCounts.get(i));
      sampCountCol.appendInt(sampleCounts[i]);
      sampTypeCol.appendString(sampleType.name());
    }
    ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
    columns.add(wIdCol.build(), streamSizeCol.build(), sampCountCol.build(),
        sampTypeCol.build());
    return new TupleBatch(SCHEMA, columns.build());
  }

  /**
   * Creates a WithReplacement distribution across the workers.
   * 
   * @param tupleCounts
   *          list of how many tuples each worker has.
   * @param sampleSize
   *          total number of samples to distribute across the workers.
   * @return array representing the distribution across the workers.
   */
  private int[] withReplacementDistribution(List<Integer> tupleCounts,
      int sampleSize) {
    int[] distribution = new int[tupleCounts.size()];
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

  /**
   * Creates a WithoutReplacement distribution across the workers.
   *
   * @param tupleCounts
   *          list of how many tuples each worker has.
   * @param sampleSize
   *          total number of samples to distribute across the workers.
   * @return array representing the distribution across the workers.
   */
  private int[] withoutReplacementDistribution(List<Integer> tupleCounts,
      int sampleSize) {
    int[] distribution = new int[tupleCounts.size()];
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

  /**
   * Returns the sample size of this operator. If operator was created using a
   * samplePercentage, this value will be 0 until after fetchNextReady.
   */
  public int getSampleSize() {
    return sampleSize;
  }

  /** Returns whether this operator is using a percentage sample. */
  public boolean isPercentageSample() {
    return isPercentageSample;
  }

  /**
   * Returns the percentage of total tuples that this operator will distribute.
   * Will be 0 if the operator was created using a specific sampleSize.
   */
  public float getSamplePercentage() {
    return samplePercentage;
  }

  /** Returns the type of sampling distribution that this operator will create. */
  public SamplingType getSampleType() {
    return sampleType;
  }

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

}
