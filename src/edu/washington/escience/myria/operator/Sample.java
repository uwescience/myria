package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class Sample extends BinaryOperator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** Total number of tuples to expect from the right operator. */
  private int streamSize;

  /** Number of tuples to sample from the right operator. */
  private int sampleSize;

  /** Random generator used for index selection. */
  private Random rand;

  /** True if the sampling is WithoutReplacement. WithReplacement otherwise. */
  private final boolean isWithoutReplacement;

  /** True if operator has extracted sampling info. */
  private boolean computedSamplingInfo = false;

  /** Buffer for tuples that will be returned. */
  private TupleBatchBuffer ans;

  /** Global count of the tuples seen so far. */
  private int tupleNum = 0;

  /** Sorted array of tuple indices that will be taken as samples. */
  private int[] samples;

  /** Current index of the samples array. */
  private int curSampIdx = 0;

  /**
   * Instantiate a Sample operator using sampling info from the left operator
   * and the stream from the right operator.
   *
   * @param left
   *          inputs a (WorkerID, StreamSize, SampleSize) tuple.
   * @param right
   *          tuples that will be sampled from.
   * @param isWithoutReplacement
   *          true if the sampling will be done Without Replacement.
   * @param randomSeed
   *          value to seed the random generator with. null if no specified seed
   */
  public Sample(final Operator left, final Operator right,
      boolean isWithoutReplacement, Long randomSeed) {
    super(left, right);
    this.isWithoutReplacement = isWithoutReplacement;
    this.rand = new Random();
    if (randomSeed != null) {
      this.rand.setSeed(randomSeed);
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // Extract sampling info from left operator.
    if (!computedSamplingInfo) {
      TupleBatch tb = getLeft().nextReady();
      if (tb == null)
        return null;
      extractSamplingInfo(tb);
      getLeft().close();

      // Cannot sampleWoR more tuples than there are.
      if (isWithoutReplacement) {
        Preconditions.checkState(sampleSize <= streamSize,
            "Cannot SampleWoR %s tuples from a population of size %s",
            sampleSize, streamSize);
      }

      // Generate target indices to accept as samples.
      if (isWithoutReplacement) {
        samples = generateIndicesWoR(streamSize, sampleSize);
      } else {
        samples = generateIndicesWR(streamSize, sampleSize);
      }

      computedSamplingInfo = true;
    }

    // Return a ready tuple batch if possible.
    TupleBatch nexttb = ans.popAny();
    if (nexttb != null) {
      return nexttb;
    }
    // Check if there's nothing left to sample.
    if (curSampIdx >= samples.length) {
      getRight().close();
      setEOS();
      return null;
    }
    Operator right = getRight();
    for (TupleBatch tb = right.nextReady(); tb != null; tb = right.nextReady()) {
      if (curSampIdx >= samples.length) { // done sampling
        break;
      }
      if (samples[curSampIdx] > tupleNum + tb.numTuples()) {
        // nextIndex is not in this batch. Continue with next batch.
        tupleNum += tb.numTuples();
        continue;
      }
      while (curSampIdx < samples.length
          && samples[curSampIdx] < tupleNum + tb.numTuples()) {
        ans.put(tb, samples[curSampIdx] - tupleNum);
        curSampIdx++;
      }
      tupleNum += tb.numTuples();
      if (ans.hasFilledTB()) {
        return ans.popFilled();
      }
    }
    return ans.popAny();
  }

  /** Helper function to extract sampling information from a TupleBatch. */
  private void extractSamplingInfo(TupleBatch tb) throws Exception {
    Preconditions.checkArgument(tb != null);

    int workerID;
    Type col0Type = tb.getSchema().getColumnType(0);
    if (col0Type == Type.INT_TYPE) {
      workerID = tb.getInt(0, 0);
    } else if (col0Type == Type.LONG_TYPE) {
      workerID = (int) tb.getLong(0, 0);
    } else {
      throw new DbException("workerID column must be of type INT or LONG");
    }
    Preconditions.checkState(workerID == getNodeID(),
        "Invalid WorkerID in samplingInfo. Expected %s, but received %s",
        getNodeID(), workerID);

    Type col1Type = tb.getSchema().getColumnType(1);
    if (col1Type == Type.INT_TYPE) {
      streamSize = tb.getInt(1, 0);
    } else if (col1Type == Type.LONG_TYPE) {
      streamSize = (int) tb.getLong(1, 0);
    } else {
      throw new DbException("streamSize column must be of type INT or LONG");
    }
    Preconditions.checkState(streamSize >= 0, "streamSize cannot be negative");

    Type col2Type = tb.getSchema().getColumnType(2);
    if (col2Type == Type.INT_TYPE) {
      sampleSize = tb.getInt(2, 0);
    } else if (col2Type == Type.LONG_TYPE) {
      sampleSize = (int) tb.getLong(2, 0);
    } else {
      throw new DbException("sampleSize column must be of type INT or LONG");
    }
    Preconditions.checkState(sampleSize >= 0, "sampleSize cannot be negative");
  }

  /**
   * Generates a sorted array of random numbers to be taken as samples.
   *
   * @param populationSize
   *          size of the population that will be sampled from.
   * @param sampleSize
   *          number of samples to draw from the population.
   * @return a sorted array of indices.
   */
  private int[] generateIndicesWR(int populationSize, int sampleSize) {
    int[] indices = new int[sampleSize];
    for (int i = 0; i < sampleSize; i++) {
      indices[i] = rand.nextInt(populationSize);
    }
    Arrays.sort(indices);
    return indices;
  }

  /**
   * Generates a sorted array of unique random numbers to be taken as samples.
   *
   * @param populationSize
   *          size of the population that will be sampled from.
   * @param sampleSize
   *          number of samples to draw from the population.
   * @return a sorted array of indices.
   */
  private int[] generateIndicesWoR(int populationSize, int sampleSize) {
    Set<Integer> indices = new HashSet<Integer>(sampleSize);
    for (int i = populationSize - sampleSize; i < populationSize; i++) {
      int idx = rand.nextInt(i + 1);
      if (indices.contains(idx)) {
        indices.add(i);
      } else {
        indices.add(idx);
      }
    }
    int[] indicesArr = new int[indices.size()];
    int i = 0;
    for (Integer val : indices) {
      indicesArr[i] = val;
      i++;
    }
    Arrays.sort(indicesArr);
    return indicesArr;
  }

  @Override
  public Schema generateSchema() {
    Operator right = getRight();
    if (right == null) {
      return null;
    }
    return right.getSchema();
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) {
    ans = new TupleBatchBuffer(getSchema());
  }

  @Override
  public void cleanup() {
    ans = null;
  }

}
