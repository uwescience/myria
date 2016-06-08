package edu.washington.escience.myria.operator;

import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.column.builder.ColumnBuilder;
import edu.washington.escience.myria.column.builder.ColumnFactory;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.SamplingType;

/**
 * Given the sizes of each worker, computes a distribution of how much each
 * worker should sample.
 */
public class SamplingDistribution extends UnaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** The output schema. */
  private static final Schema SCHEMA =
      Schema.ofFields(
          "WorkerID",
          Type.INT_TYPE,
          "StreamSize",
          Type.INT_TYPE,
          "SampleSize",
          Type.INT_TYPE,
          "SampleType",
          Type.STRING_TYPE);

  /** Total number of tuples to sample. */
  private int sampleSize = 0;

  /** True if using a percentage instead of a specific tuple count. */
  private boolean isPercentageSample = false;

  /** Percentage of total tuples to sample. */
  private float samplePercentage;

  /** The type of sampling to perform. */
  private final SamplingType sampleType;

  /** Random generator used for creating the distribution. */
  private Random rand;

  /** Maps (worker_i) --> (sampling info for worker_i) */
  SortedMap<Integer, WorkerInfo> workerInfo = new TreeMap<>();

  /** Total number of tuples across all workers. */
  int totalTupleCount = 0;

  private SamplingDistribution(Operator child, SamplingType sampleType, Long randomSeed) {
    super(child);
    this.sampleType = sampleType;
    rand = new Random();
    if (randomSeed != null) {
      rand.setSeed(randomSeed);
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
  public SamplingDistribution(
      Operator child, int sampleSize, SamplingType sampleType, Long randomSeed) {
    this(child, sampleType, randomSeed);
    Preconditions.checkArgument(sampleSize >= 0, "Sample Size must be >= 0: %s", sampleSize);
    this.sampleSize = sampleSize;
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
  public SamplingDistribution(
      Operator child, float samplePercentage, SamplingType sampleType, Long randomSeed) {
    this(child, sampleType, randomSeed);
    this.isPercentageSample = true;
    this.samplePercentage = samplePercentage;
    Preconditions.checkArgument(
        samplePercentage >= 0 && samplePercentage <= 100,
        "Sample Percentage must be >= 0 && <= 100: %s",
        samplePercentage);
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    // Drain out all the worker info.
    while (!getChild().eos()) {
      TupleBatch tb = getChild().nextReady();
      if (tb == null) {
        if (getChild().eos()) {
          break;
        }
        return null;
      }
      extractWorkerInfo(tb);
    }
    getChild().close();

    // Convert samplePct to sampleSize if using a percentage sample.
    if (isPercentageSample) {
      sampleSize = Math.round(totalTupleCount * (samplePercentage / 100));
    }
    Preconditions.checkState(
        sampleSize >= 0 && sampleSize <= totalTupleCount,
        "Cannot extract %s samples from a population of size %s",
        sampleSize,
        totalTupleCount);

    // Generate a sampling distribution across the workers.
    if (sampleType == SamplingType.WithReplacement) {
      withReplacementDistribution(workerInfo, totalTupleCount, sampleSize);
    } else if (sampleType == SamplingType.WithoutReplacement) {
      withoutReplacementDistribution(workerInfo, totalTupleCount, sampleSize);
    } else {
      throw new DbException("Invalid sampleType: " + sampleType);
    }

    // Build and return a TupleBatch with the distribution.
    // Assumes that the sampling information can fit into one tuple batch.
    List<ColumnBuilder<?>> colBuilders = ColumnFactory.allocateColumns(SCHEMA);
    for (Map.Entry<Integer, WorkerInfo> iWorker : workerInfo.entrySet()) {
      colBuilders.get(0).appendInt(iWorker.getKey());
      colBuilders.get(1).appendInt(iWorker.getValue().actualTupleCount);
      colBuilders.get(2).appendInt(iWorker.getValue().sampleSize);
      colBuilders.get(3).appendString(sampleType.name());
    }
    ImmutableList.Builder<Column<?>> columns = ImmutableList.builder();
    for (ColumnBuilder<?> cb : colBuilders) {
      columns.add(cb.build());
    }
    setEOS();
    return new TupleBatch(SCHEMA, columns.build());
  }

  /** Helper function to extract worker information from a tuple batch. */
  private void extractWorkerInfo(TupleBatch tb) throws DbException {
    Type col0Type = tb.getSchema().getColumnType(0);
    Type col1Type = tb.getSchema().getColumnType(1);
    boolean hasActualTupleCount = false;
    Type col2Type = null;
    if (tb.getSchema().numColumns() > 2) {
      hasActualTupleCount = true;
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
      Preconditions.checkState(!workerInfo.containsKey(workerID), "Duplicate WorkerIDs");

      int tupleCount;
      if (col1Type == Type.INT_TYPE) {
        tupleCount = tb.getInt(1, i);
      } else if (col1Type == Type.LONG_TYPE) {
        tupleCount = (int) tb.getLong(1, i);
      } else {
        throw new DbException("TupleCount must be of type INT or LONG");
      }
      Preconditions.checkState(
          tupleCount >= 0, "Worker cannot have a negative TupleCount: %s", tupleCount);

      int actualTupleCount = tupleCount;
      if (hasActualTupleCount) {
        if (col2Type == Type.INT_TYPE) {
          actualTupleCount = tb.getInt(2, i);
        } else if (col2Type == Type.LONG_TYPE) {
          actualTupleCount = (int) tb.getLong(2, i);
        } else {
          throw new DbException("ActualTupleCount must be of type INT or LONG");
        }
        Preconditions.checkState(
            tupleCount >= 0,
            "Worker cannot have a negative ActualTupleCount: %d",
            actualTupleCount);
      }

      WorkerInfo wInfo = new WorkerInfo(tupleCount, actualTupleCount);
      workerInfo.put(workerID, wInfo);
      totalTupleCount += tupleCount;
    }
  }

  /**
   * Creates a WithReplacement distribution across the workers.
   *
   * @param workerInfo
   *          reference to the workerInfo to modify.
   * @param totalTupleCount
   *          total # of tuples across all workers.
   * @param sampleSize
   *          total # of samples to distribute across the workers.
   */
  private void withReplacementDistribution(
      SortedMap<Integer, WorkerInfo> workerInfo, int totalTupleCount, int sampleSize) {
    for (int i = 0; i < sampleSize; i++) {
      int sampleTupleIdx = rand.nextInt(totalTupleCount);
      // Assign this tuple to the workerID that holds this sampleTupleIdx.
      int tupleOffset = 0;
      for (Map.Entry<Integer, WorkerInfo> iWorker : workerInfo.entrySet()) {
        WorkerInfo wInfo = iWorker.getValue();
        if (sampleTupleIdx < wInfo.tupleCount + tupleOffset) {
          wInfo.sampleSize += 1;
          break;
        }
        tupleOffset += wInfo.tupleCount;
      }
    }
  }

  /**
   * Creates a WithoutReplacement distribution across the workers.
   *
   * @param workerInfo
   *          reference to the workerInfo to modify.
   * @param totalTupleCount
   *          total # of tuples across all workers.
   * @param sampleSize
   *          total # of samples to distribute across the workers.
   */
  private void withoutReplacementDistribution(
      SortedMap<Integer, WorkerInfo> workerInfo, int totalTupleCount, int sampleSize) {
    SortedMap<Integer, Integer> logicalTupleCounts = new TreeMap<>();
    for (Map.Entry<Integer, WorkerInfo> wInfo : workerInfo.entrySet()) {
      logicalTupleCounts.put(wInfo.getKey(), wInfo.getValue().tupleCount);
    }

    for (int i = 0; i < sampleSize; i++) {
      int sampleTupleIdx = rand.nextInt(totalTupleCount - i);
      // Assign this tuple to the workerID that holds this sampleTupleIdx.
      int tupleOffset = 0;
      for (Map.Entry<Integer, WorkerInfo> iWorker : workerInfo.entrySet()) {
        int wID = iWorker.getKey();
        WorkerInfo wInfo = iWorker.getValue();
        if (sampleTupleIdx < logicalTupleCounts.get(wID) + tupleOffset) {
          wInfo.sampleSize += 1;
          // Cannot sample the same tuple, so pretend it doesn't exist anymore.
          logicalTupleCounts.put(wID, logicalTupleCounts.get(wID) - 1);
          break;
        }
        tupleOffset += logicalTupleCounts.get(wID);
      }
    }
  }

  /**
   * Returns the sample size of this operator. If operator was created using a
   * samplePercentage, this value will be 0 until after fetchNextReady.
   */
  public int getSampleSize() {
    return sampleSize;
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

  @Override
  public void cleanup() {
    workerInfo = null;
  }

  /** Encapsulates sampling information about a worker. */
  private class WorkerInfo {
    /** # of tuples that this worker owns. */
    int tupleCount;

    /**
     * Actual # of tuples that the worker has stored. May be different than
     * tupleCount if the worker pre-sampled the data.
     **/
    int actualTupleCount;

    /** # of tuples that the distribution assigned to this worker. */
    int sampleSize = 0;

    WorkerInfo(int tupleCount, int actualTupleCount) {
      this.tupleCount = tupleCount;
      this.actualTupleCount = actualTupleCount;
    }
  }
}
