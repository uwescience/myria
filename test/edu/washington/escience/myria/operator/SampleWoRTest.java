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
 * Tests SampleWoR by verifying the results of various scenarios.
 */
public class SampleWoRTest {

  final long RANDOM_SEED = 42;
  final int[] INPUT_VALS = {0, 1, 2, 3, 4, 5};

  final Schema LEFT_SCHEMA =
      Schema.ofFields(
          "WorkerID",
          Type.INT_TYPE,
          "PartitionSize",
          Type.INT_TYPE,
          "SampleSize",
          Type.INT_TYPE,
          "SampleType",
          Type.STRING_TYPE);
  final Schema RIGHT_SCHEMA = Schema.ofFields(Type.INT_TYPE, "SomeValue");
  final Schema OUTPUT_SCHEMA = RIGHT_SCHEMA;

  TupleBatchBuffer leftInput;
  TupleBatchBuffer rightInput;
  Sample sampOp;

  @Before
  public void setup() {
    leftInput = new TupleBatchBuffer(LEFT_SCHEMA);
    leftInput.putInt(0, -1); // WorkerID for testing
    rightInput = new TupleBatchBuffer(RIGHT_SCHEMA);
    for (int val : INPUT_VALS) {
      rightInput.putInt(0, val);
    }
  }

  /** Sample size 0. */
  @Test
  public void testSampleSizeZero() throws DbException {
    int partitionSize = INPUT_VALS.length;
    int sampleSize = 0;
    verifyExpectedResults(partitionSize, sampleSize);
  }

  /** Sample size 1. */
  @Test
  public void testSampleSizeOne() throws DbException {
    int partitionSize = INPUT_VALS.length;
    int sampleSize = 1;
    verifyExpectedResults(partitionSize, sampleSize);
  }

  /** Sample size 50%. */
  @Test
  public void testSampleSizeHalf() throws DbException {
    int partitionSize = INPUT_VALS.length;
    int sampleSize = INPUT_VALS.length / 2;
    verifyExpectedResults(partitionSize, sampleSize);
  }

  /** Sample size all. */
  @Test
  public void testSampleSizeAll() throws DbException {
    int partitionSize = INPUT_VALS.length;
    int sampleSize = INPUT_VALS.length;
    verifyExpectedResults(partitionSize, sampleSize);
  }

  /** Sample size greater than partition size. */
  @Test(expected = IllegalStateException.class)
  public void testSampleSizeTooMany() throws DbException {
    int partitionSize = INPUT_VALS.length;
    int sampleSize = INPUT_VALS.length + 1;
    drainOperator(partitionSize, sampleSize);
  }

  /** Cannot have a negative sample size. */
  @Test(expected = IllegalStateException.class)
  public void testSampleSizeNegative() throws DbException {
    int partitionSize = INPUT_VALS.length;
    int sampleSize = -1;
    drainOperator(partitionSize, sampleSize);
  }

  /** Cannot have a negative partition size. */
  @Test(expected = IllegalStateException.class)
  public void testSamplePartitionNegative() throws DbException {
    int partitionSize = -1;
    int sampleSize = 3;
    drainOperator(partitionSize, sampleSize);
  }

  @After
  public void cleanup() throws DbException {
    if (sampOp != null && sampOp.isOpen()) {
      sampOp.close();
    }
  }

  /**
   * Tests whether the output could be a valid distribution. Note: doesn't
   * currently test for statistical randomness.
   */
  private void verifyExpectedResults(int partitionSize, int sampleSize) throws DbException {
    leftInput.putInt(1, partitionSize);
    leftInput.putInt(2, sampleSize);
    leftInput.putString(3, "WithoutReplacement");
    sampOp = new Sample(new TupleSource(leftInput), new TupleSource(rightInput), RANDOM_SEED);
    sampOp.open(TestEnvVars.get());

    int rowIdx = 0;
    while (!sampOp.eos()) {
      TupleBatch result = sampOp.nextReady();
      if (result != null) {
        assertEquals(OUTPUT_SCHEMA, result.getSchema());
        rowIdx += result.numTuples();
      }
    }
    assertEquals(sampleSize, rowIdx);
  }

  /** Run through all results without doing anything. */
  private void drainOperator(int partitionSize, int sampleSize) throws DbException {
    leftInput.putInt(1, partitionSize);
    leftInput.putInt(2, sampleSize);
    leftInput.putString(3, "WithoutReplacement");
    sampOp = new Sample(new TupleSource(leftInput), new TupleSource(rightInput), RANDOM_SEED);
    sampOp.open(TestEnvVars.get());
    while (!sampOp.eos()) {
      sampOp.nextReady();
    }
  }
}
