package edu.washington.escience.myria.operator;

import java.util.BitSet;
import java.util.Random;

import com.google.common.base.Preconditions;
import edu.washington.escience.myria.storage.TupleBatch;

public class SampleWoR extends Sample {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /** True if operator has extracted sampling info. */
  private boolean computedSamplingInfo = false;

  /** Current number of samples that have been selected. */
  private int samplesAcquired = 0;

  /** Current number of tuples that have been processed. */
  private int tuplesProcessed = 0;

  public SampleWoR(final Operator left, final Operator right) {
    super(left, right);
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    // Extract sampling info from left operator.
    if (!computedSamplingInfo) {
      TupleBatch tb = getLeft().nextReady();
      if (tb == null)
        return null;
      extractSamplingInfo(tb);

      // Cannot sampleWoR more tuples than there are.
      Preconditions.checkState(sampleSize <= populationSize,
          "Cannot SampleWoR %s tuples from a population of size %s",
          sampleSize, populationSize);

      getLeft().close();
      computedSamplingInfo = true;
    }

    // SampleWoR from the right operator.
    Random rand = new Random();
    Operator right = getRight();
    for (TupleBatch tb = right.nextReady(); tb != null; tb = right.nextReady()) {
      BitSet toKeep = new BitSet(tb.numTuples());
      for (int i = 0; i < tb.numTuples(); i++) {
        int k = sampleSize - samplesAcquired;
        int n = populationSize - tuplesProcessed;
        // Each tuple has a k/n probability of getting sampled.
        if (k > rand.nextInt(n)) {
          toKeep.set(i);
          samplesAcquired++;
        }
        tuplesProcessed++;
      }
      if (!toKeep.isEmpty()) {
        return tb.filter(toKeep);
      }
    }
    return null;
  }

}
