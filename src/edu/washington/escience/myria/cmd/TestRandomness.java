package edu.washington.escience.myria.cmd;

import java.util.Random;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public final class TestRandomness {

  /** Disable construction. */
  private TestRandomness() {
  }

  /** The number of bins into which numbers are hashed. */
  private static final int NUM_BINS = 48;
  /** The number of random numbers to hash. */
  private static final int TRIALS = 1000 * 100;
  /** The seed for the hash function. */
  private static final int MAGIC_HASHCODE = 243;
  /** The hash function itself. */
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32(MAGIC_HASHCODE);

  /**
   * @param args arguments.
   * @throws Exception exception.
   */
  public static void main(final String[] args) throws Exception {
    Random randomizer = new Random(System.currentTimeMillis());
    int[] bins = new int[NUM_BINS];
    for (int i = 0; i < TRIALS; ++i) {
      int binId = HASH_FUNCTION.newHasher().putInt(randomizer.nextInt()).hash().asInt() % bins.length;
      if (binId < 0) {
        binId += bins.length;
      }
      bins[binId]++;
    }

    double max = 0;
    double min = Integer.MAX_VALUE;
    for (int binCount : bins) {
      max = Math.max(max, (double) binCount * NUM_BINS / TRIALS);
      min = Math.min(min, (double) binCount * NUM_BINS / TRIALS);
    }
    System.err.println("max skew on hash values is " + max);
    System.err.println("min skew on hash values is " + min);
  }
}
