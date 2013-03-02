package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.operator.apply.Apply;
import edu.washington.escience.myriad.operator.apply.ConstantMultiplicationIFunction;
import edu.washington.escience.myriad.operator.apply.IFunction;
import edu.washington.escience.myriad.operator.apply.SqrtIFunction;

public class ApplyTest {

  private static final int RANDOM_LIMIT = 1000;
  private int numTuples;
  private int multiplicationFactor;

  @Before
  public void setUp() throws Exception {
    Random rand = new Random();
    // numTuples = rand.nextInt(RANDOM_LIMIT);
    numTuples = 10000000;
    // multiplicationFactor = rand.nextInt(RANDOM_LIMIT);
    multiplicationFactor = 2;
  }

  @Test
  public void testApplySqrt() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE),
        ImmutableList.of("a"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < numTuples; i++) {
      tbb.put(0, (long) Math.pow(i, 2));
    }
    Apply apply = new Apply(new TupleSource(tbb), new int[] { 0 },
        new IFunction[] { new SqrtIFunction() });
    apply.open();
    TupleBatch result;
    int resultSize = 0;
    while ((result = apply.next()) != null) {
      assertEquals(2, result.getSchema().numFields());
      assertEquals(Type.DOUBLE_TYPE, result.getSchema().getFieldType(1));
      for (int i = 0; i < result.numTuples(); i++) {
        assertEquals(i + resultSize, result.getDouble(1, i), 0.0000001);
      }
      resultSize += result.numTuples();
    }
    assertEquals(numTuples, resultSize);
    apply.close();
  }

  @Test
  public void testApplyMultiFunctions() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE),
        ImmutableList.of("a"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < numTuples; i++) {
      tbb.put(0, (long) Math.pow(i, 2));
    }
    Apply apply = new Apply(new TupleSource(tbb), new int[] { 0, 0 },
        new IFunction[] { new SqrtIFunction(),
            new ConstantMultiplicationIFunction(multiplicationFactor) });
    apply.open();
    TupleBatch result;
    int resultSize = 0;
    while ((result = apply.next()) != null) {
      assertEquals(3, result.getSchema().numFields());
      assertEquals(Type.DOUBLE_TYPE, result.getSchema().getFieldType(1));
      assertEquals(Type.LONG_TYPE, result.getSchema().getFieldType(2));
      for (int i = 0; i < result.numTuples(); i++) {
        assertEquals(i + resultSize, result.getDouble(1, i), 0.0000001);
        assertEquals((long) Math.pow(i + resultSize, 2) * multiplicationFactor,
            result.getLong(2, i));
      }
      resultSize += result.numTuples();
    }
    assertEquals(numTuples, resultSize);
    apply.close();
  }
}
