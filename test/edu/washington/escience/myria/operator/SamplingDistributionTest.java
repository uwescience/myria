package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import edu.washington.escience.myria.util.SamplingType;
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

  final Schema inputSchema =
      Schema.ofFields("WorkerID", Type.INT_TYPE, "PartitionSize", Type.INT_TYPE);
  final Schema expectedResultSchema =
      Schema.ofFields(
          "WorkerID",
          Type.INT_TYPE,
          "StreamSize",
          Type.INT_TYPE,
          "SampleSize",
          Type.INT_TYPE,
          "SampleType",
          Type.STRING_TYPE);

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
    SamplingType sampleType = SamplingType.WithReplacement;
    final int[][] expectedResults = {{1, 300, 0}, {2, 200, 0}, {3, 400, 0}, {4, 100, 0}};
    verifyExpectedResults(sampleSize, sampleType, expectedResults);
  }

  @Test
  public void testSampleWoRSizeZero() throws DbException {
    int sampleSize = 0;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    final int[][] expectedResults = {{1, 300, 0}, {2, 200, 0}, {3, 400, 0}, {4, 100, 0}};
    verifyExpectedResults(sampleSize, sampleType, expectedResults);
  }

  /** Sample size 0%. */
  @Test
  public void testSampleWRPctZero() throws DbException {
    float samplePct = 0;
    SamplingType sampleType = SamplingType.WithReplacement;
    final int[][] expectedResults = {{1, 300, 0}, {2, 200, 0}, {3, 400, 0}, {4, 100, 0}};
    verifyExpectedResults(samplePct, sampleType, expectedResults);
  }

  @Test
  public void testSampleWoRPctZero() throws DbException {
    float samplePct = 0;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    final int[][] expectedResults = {{1, 300, 0}, {2, 200, 0}, {3, 400, 0}, {4, 100, 0}};
    verifyExpectedResults(samplePct, sampleType, expectedResults);
  }

  /** Sample size 1. */
  @Test
  public void testSampleWRSizeOne() throws DbException {
    int sampleSize = 1;
    SamplingType sampleType = SamplingType.WithReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  @Test
  public void testSampleWoRSizeOne() throws DbException {
    int sampleSize = 1;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  /** Sample size 50. */
  @Test
  public void testSampleWRSizeFifty() throws DbException {
    int sampleSize = 50;
    SamplingType sampleType = SamplingType.WithReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  @Test
  public void testSampleWoRSizeFifty() throws DbException {
    int sampleSize = 50;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  /** Sample size 50%. */
  @Test
  public void testSampleWRPctFifty() throws DbException {
    float samplePct = 50;
    SamplingType sampleType = SamplingType.WithReplacement;
    verifyPossibleDistribution(samplePct, sampleType);
  }

  @Test
  public void testSampleWoRPctFifty() throws DbException {
    float samplePct = 50;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    verifyPossibleDistribution(samplePct, sampleType);
  }

  /** Sample all but one tuple. */
  @Test
  public void testSampleWoRSizeAllButOne() throws DbException {
    int sampleSize = 999;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  @Test
  public void testSampleWRSizeAllButOne() throws DbException {
    int sampleSize = 999;
    SamplingType sampleType = SamplingType.WithReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  /** SamplingWoR the entire population == return all. */
  @Test
  public void testSampleWoRSizeMax() throws DbException {
    int sampleSize = 1000;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    final int[][] expectedResults = {{1, 300, 300}, {2, 200, 200}, {3, 400, 400}, {4, 100, 100}};
    verifyExpectedResults(sampleSize, sampleType, expectedResults);
  }

  @Test
  public void testSampleWoRPctMax() throws DbException {
    float samplePct = 100;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    final int[][] expectedResults = {{1, 300, 300}, {2, 200, 200}, {3, 400, 400}, {4, 100, 100}};
    verifyExpectedResults(samplePct, sampleType, expectedResults);
  }

  /** SamplingWR the entire population. */
  @Test
  public void testSampleWRSizeMax() throws DbException {
    int sampleSize = 1000;
    SamplingType sampleType = SamplingType.WithReplacement;
    verifyPossibleDistribution(sampleSize, sampleType);
  }

  @Test
  public void testSampleWRPctMax() throws DbException {
    float samplePct = 100;
    SamplingType sampleType = SamplingType.WithReplacement;
    verifyPossibleDistribution(samplePct, sampleType);
  }

  /** Cannot sample more than total size. */
  @Test(expected = IllegalStateException.class)
  public void testSampleWoRSizeTooMany() throws DbException {
    int sampleSize = 1001;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    drainOperator(sampleSize, sampleType);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSampleWoRPctTooMany() throws DbException {
    float samplePct = 100.1f;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    drainOperator(samplePct, sampleType);
  }

  @Test(expected = IllegalStateException.class)
  public void testSampleWRSizeTooMany() throws DbException {
    int sampleSize = 1001;
    SamplingType sampleType = SamplingType.WithReplacement;
    drainOperator(sampleSize, sampleType);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSampleWRPctTooMany() throws DbException {
    float samplePct = 100.1f;
    SamplingType sampleType = SamplingType.WithReplacement;
    drainOperator(samplePct, sampleType);
  }

  /** Cannot sample a negative number of samples. */
  @Test(expected = IllegalArgumentException.class)
  public void testSampleWoRSizeNegative() throws DbException {
    int sampleSize = -1;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    drainOperator(sampleSize, sampleType);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSampleWoRPctNegative() throws DbException {
    float samplePct = -0.01f;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    drainOperator(samplePct, sampleType);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSampleWRSizeNegative() throws DbException {
    int sampleSize = -1;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    drainOperator(sampleSize, sampleType);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSampleWRPctNegative() throws DbException {
    float samplePct = -0.01f;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    drainOperator(samplePct, sampleType);
  }

  /** Worker cannot report a negative partition size. */
  @Test(expected = IllegalStateException.class)
  public void testSampleWoRWorkerNegative() throws DbException {
    int sampleSize = 50;
    SamplingType sampleType = SamplingType.WithoutReplacement;
    input.putInt(0, 5);
    input.putInt(1, -1);
    drainOperator(sampleSize, sampleType);
  }

  @Test(expected = IllegalStateException.class)
  public void testSampleWRWorkerNegative() throws DbException {
    int sampleSize = 50;
    SamplingType sampleType = SamplingType.WithReplacement;
    input.putInt(0, 5);
    input.putInt(1, -1);
    drainOperator(sampleSize, sampleType);
  }

  @After
  public void cleanup() throws DbException {
    if (sampOp != null && sampOp.isOpen()) {
      sampOp.close();
    }
  }

  /** Compare output results compared to some known expectedResults. */
  private void verifyExpectedResults(SamplingDistribution sampOp, int[][] expectedResults)
      throws DbException {
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

  private void verifyExpectedResults(
      int sampleSize, SamplingType sampleType, int[][] expectedResults) throws DbException {
    sampOp = new SamplingDistribution(new TupleSource(input), sampleSize, sampleType, RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    verifyExpectedResults(sampOp, expectedResults);
  }

  private void verifyExpectedResults(
      float samplePct, SamplingType sampleType, int[][] expectedResults) throws DbException {
    sampOp = new SamplingDistribution(new TupleSource(input), samplePct, sampleType, RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    verifyExpectedResults(sampOp, expectedResults);
  }

  /**
   * Tests the actual distribution against what could be possible. Note: doesn't
   * test if it is statistically random.
   */
  private void verifyPossibleDistribution(SamplingDistribution sampOp) throws DbException {
    int rowIdx = 0;
    int computedSampleSize = 0;
    while (!sampOp.eos()) {
      TupleBatch result = sampOp.nextReady();
      if (result != null) {
        assertEquals(expectedResultSchema, result.getSchema());
        for (int i = 0; i < result.numTuples(); ++i, ++rowIdx) {
          assertTrue(result.getInt(2, i) >= 0 && result.getInt(2, i) <= sampOp.getSampleSize());
          if (sampOp.getSampleType() == SamplingType.WithoutReplacement) {
            // SampleWoR cannot sample more than worker's population size.
            assertTrue(result.getInt(2, i) <= result.getInt(1, i));
          }
          computedSampleSize += result.getInt(2, i);
        }
      }
    }
    assertEquals(input.numTuples(), rowIdx);
    assertEquals(sampOp.getSampleSize(), computedSampleSize);
  }

  private void verifyPossibleDistribution(int sampleSize, SamplingType sampleType)
      throws DbException {
    sampOp = new SamplingDistribution(new TupleSource(input), sampleSize, sampleType, RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    verifyPossibleDistribution(sampOp);
  }

  private void verifyPossibleDistribution(float samplePct, SamplingType sampleType)
      throws DbException {
    sampOp = new SamplingDistribution(new TupleSource(input), samplePct, sampleType, RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    verifyPossibleDistribution(sampOp);
  }

  /** Run through all results without doing anything. */
  private void drainOperator(int sampleSize, SamplingType sampleType) throws DbException {
    sampOp = new SamplingDistribution(new TupleSource(input), sampleSize, sampleType, RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    while (!sampOp.eos()) {
      sampOp.nextReady();
    }
  }

  private void drainOperator(float samplePct, SamplingType sampleType) throws DbException {
    sampOp = new SamplingDistribution(new TupleSource(input), samplePct, sampleType, RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    while (!sampOp.eos()) {
      sampOp.nextReady();
    }
  }
}
