package edu.washington.escience.myria.operator;

import java.util.Arrays;
import java.util.Random;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

public class SampleWR extends Sample {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** True if operator has extracted sampling info. */
  private boolean computedSamplingInfo = false;

  /** Buffer for tuples that will be returned. */
  private TupleBatchBuffer ans;

  /** Current TupleID being processed. */
  private int tupleNum = 0;

  /** List of of indices that will be taken as samples. */
  private int[] samples;

  /** Current index of the samples array. */
  private int curSampIdx = 0;

  public SampleWR(final Operator left, final Operator right) {
    super(left, right, null);
  }

  public SampleWR(final Operator left, final Operator right, Long randomSeed) {
    super(left, right, randomSeed);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // Extract sampling info from left operator.
    if (!computedSamplingInfo) {
      TupleBatch tb = getLeft().nextReady();
      if (tb == null)
        return null;
      extractSamplingInfo(tb);

      // Generate target indices to accept as samples.
      samples = generateTargetSampleIndices(streamSize, sampleSize);

      getLeft().close();
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

  /**
   * Generates a sorted sequence of random indices that will be chosen to sample
   * from.
   * 
   * @param populationSize
   *          size of the population that will be sampled from.
   * @param sampleSize
   *          number of samples to draw from the population.
   * @return a sorted array of indices.
   */
  private int[] generateTargetSampleIndices(int populationSize, int sampleSize) {
    Random rand = getRandom();
    int[] indices = new int[sampleSize];
    for (int i = 0; i < sampleSize; i++) {
      indices[i] = rand.nextInt(populationSize);
    }
    Arrays.sort(indices);
    return indices;
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
