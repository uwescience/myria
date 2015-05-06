package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * Tests the SamplingDistribution operator by verifying the results of various
 * scenarios.
 */
public class SamplingDistributionTest {

  final long RANDOM_SEED = 42;

  final Schema inputSchema = Schema.ofFields("WorkerID", Type.INT_TYPE,
      "PartitionSize", Type.INT_TYPE);
  final Schema expectedResultSchema = Schema.ofFields("WorkerID", Type.INT_TYPE,
          "StreamSize", Type.INT_TYPE, "SampleSize", Type.INT_TYPE);

  TupleBatchBuffer input;
  SamplingDistribution sampOp;

  @Before
  public void setup() {
    // (WorkerID, PartitionSize)
    input = new TupleBatchBuffer(inputSchema);
    input.putInt(0, 1);
    input.putInt(1, 300);
    input.putInt(0, 2);
    input.putInt(1, 200);
    input.putInt(0, 3);
    input.putInt(1, 400);
    input.putInt(0, 4);
    input.putInt(1, 100);
  }

  /** Sample size 0. */
  @Test
  public void testSampleWRSizeZero() throws DbException {
    int sampleSize = 0;
    boolean isWithoutReplacement = false;
    final int[][] expectedResults = { { 1, 300, 0 }, { 2, 200, 0 },
        { 3, 400, 0 }, { 4, 100, 0 } };
    verifyExpectedResults(sampleSize, isWithoutReplacement, expectedResults);
  }

  @Test
  public void testSampleWoRSizeZero() throws DbException {
    int sampleSize = 0;
    boolean isWithoutReplacement = true;
    final int[][] expectedResults = { { 1, 300, 0 }, { 2, 200, 0 },
        { 3, 400, 0 }, { 4, 100, 0 } };
    verifyExpectedResults(sampleSize, isWithoutReplacement, expectedResults);
  }

  /** Sample size 1. */
  @Test
  public void testSampleWRSizeOne() throws DbException {
    int sampleSize = 1;
    boolean isWithoutReplacement = false;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  @Test
  public void testSampleWoRSizeOne() throws DbException {
    int sampleSize = 1;
    boolean isWithoutReplacement = true;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  /** Sample size 50. */
  @Test
  public void testSampleWRSizeFifty() throws DbException {
    int sampleSize = 50;
    boolean isWithoutReplacement = false;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  @Test
  public void testSampleWoRSizeFifty() throws DbException {
    int sampleSize = 50;
    boolean isWithoutReplacement = true;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  /** Sample all but one tuple. */
  @Test
  public void testSampleWoRSizeAllButOne() throws DbException {
    int sampleSize = 999;
    boolean isWithoutReplacement = true;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  @Test
  public void testSampleWRSizeAllButOne() throws DbException {
    int sampleSize = 999;
    boolean isWithoutReplacement = false;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  /** SamplingWoR the entire population == return all. */
  @Test
  public void testSampleWoRSizeMax() throws DbException {
    int sampleSize = 1000;
    boolean isWithoutReplacement = true;
    final int[][] expectedResults = { { 1, 300, 300 }, { 2, 200, 200 },
        { 3, 400, 400 }, { 4, 100, 100 } };
    verifyExpectedResults(sampleSize, isWithoutReplacement, expectedResults);
  }

  /** SamplingWR the entire population. */
  @Test
  public void testSampleWRSizeMax() throws DbException {
    int sampleSize = 1000;
    boolean isWithoutReplacement = false;
    verifyPossibleDistribution(sampleSize, isWithoutReplacement);
  }

  /** Cannot sample more than total size. */
  @Test(expected = IllegalStateException.class)
  public void testSampleWoRSizeTooMany() throws DbException {
    int sampleSize = 1001;
    boolean isWithoutReplacement = true;
    drainOperator(sampleSize, isWithoutReplacement);
  }

  @Test(expected = IllegalStateException.class)
  public void testSampleWRSizeTooMany() throws DbException {
    int sampleSize = 1001;
    boolean isWithoutReplacement = false;
    drainOperator(sampleSize, isWithoutReplacement);
  }

  /** Cannot sample a negative number of samples. */
  @Test(expected = IllegalStateException.class)
  public void testSampleWoRSizeNegative() throws DbException {
    int sampleSize = -1;
    boolean isWithoutReplacement = true;
    drainOperator(sampleSize, isWithoutReplacement);
  }

  @Test(expected = IllegalStateException.class)
  public void testSampleWRSizeNegative() throws DbException {
    int sampleSize = -1;
    boolean isWithoutReplacement = false;
    drainOperator(sampleSize, isWithoutReplacement);
  }

  /** Worker cannot report a negative partition size. */
  @Test(expected = IllegalStateException.class)
  public void testSampleWoRWorkerNegative() throws DbException {
    int sampleSize = 50;
    boolean isWithoutReplacement = true;
    input.putInt(0, 5);
    input.putInt(1, -1);
    drainOperator(sampleSize, isWithoutReplacement);
  }

  @Test(expected = IllegalStateException.class)
  public void testSampleWRWorkerNegative() throws DbException {
    int sampleSize = 50;
    boolean isWithoutReplacement = false;
    input.putInt(0, 5);
    input.putInt(1, -1);
    drainOperator(sampleSize, isWithoutReplacement);
  }

  @After
  public void cleanup() throws DbException {
    if (sampOp != null && sampOp.isOpen()) {
      sampOp.close();
    }
  }

  /** Compare output results compared to some known expectedResults. */
  private void verifyExpectedResults(int sampleSize,
      boolean isWithoutReplacement, int[][] expectedResults) throws DbException {
    sampOp = new SamplingDistribution(sampleSize, isWithoutReplacement,
        new TupleSource(input), RANDOM_SEED);
    sampOp.open(TestEnvVars.get());

    int rowIdx = 0;
    while (!sampOp.eos()) {
      TupleBatch result = sampOp.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());
        for (int i = 0; i < result.numTuples(); ++i, ++rowIdx) {
          assertEquals(expectedResults[rowIdx][0], result.getInt(0, i));
          assertEquals(expectedResults[rowIdx][1], result.getInt(1, i));
          assertEquals(expectedResults[rowIdx][2], result.getInt(2, i));
        }
      }
    }
    assertEquals(expectedResults.length, rowIdx);
  }

  /**
   * Tests the actual distribution against what could be possible. Note: doesn't
   * test if it is statistically random.
   */
  private void verifyPossibleDistribution(int sampleSize,
      boolean isWithoutReplacement) throws DbException {
    sampOp = new SamplingDistribution(sampleSize, isWithoutReplacement,
        new TupleSource(input), RANDOM_SEED);
    sampOp.open(TestEnvVars.get());

    int rowIdx = 0;
    int computedSampleSize = 0;
    while (!sampOp.eos()) {
      TupleBatch result = sampOp.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());
        for (int i = 0; i < result.numTuples(); ++i, ++rowIdx) {
          assert (result.getInt(2, i) >= 0 && result.getInt(2, i) <= sampleSize);
          if (isWithoutReplacement) {
            // SampleWoR cannot sample more than worker's population size.
            assert (result.getInt(2, i) <= result.getInt(1, i));
          }
          computedSampleSize += result.getInt(2, i);
        }
      }
    }
    assertEquals(input.numTuples(), rowIdx);
    assertEquals(sampleSize, computedSampleSize);
  }

  /** Run through all results without doing anything. */
  private void drainOperator(int sampleSize, boolean isWithoutReplacement)
      throws DbException {
    sampOp = new SamplingDistribution(sampleSize, isWithoutReplacement,
        new TupleSource(input), RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    while (!sampOp.eos()) {
      sampOp.nextReady();
    }
  }
}
