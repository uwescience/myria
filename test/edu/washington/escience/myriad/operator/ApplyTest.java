package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertEquals;

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
import edu.washington.escience.myriad.operator.apply.IFunctionCaller;
import edu.washington.escience.myriad.operator.apply.SqrtIFunction;

public class ApplyTest {

  private int numTuples;
  private int multiplicationFactor;

  @Before
  public void setUp() throws Exception {
    // numTuples = rand.nextInt(RANDOM_LIMIT);
    numTuples = 1000;
    // multiplicationFactor = rand.nextInt(RANDOM_LIMIT);
    multiplicationFactor = 2;
  }

  @Test
  public void testApplySqrt() throws DbException {
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("a"));
    System.out.println("ApplyTest: ApplySqrt()");
    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < numTuples; i++) {
      tbb.put(0, (long) Math.pow(i, 2));
    }
    ImmutableList.Builder<Integer> arguments = ImmutableList.builder();
    arguments.add(0, 2);
    ImmutableList.Builder<IFunctionCaller> callers = ImmutableList.builder();
    callers.add(new IFunctionCaller(new SqrtIFunction(), arguments.build()));
    Apply apply = new Apply(new TupleSource(tbb), callers.build());
    apply.open();
    TupleBatch result;
    int resultSize = 0;
    while ((result = apply.next()) != null) {
      assertEquals(2, result.getSchema().numColumns());
      assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(1));
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
    System.out.println("ApplyTest: MultiFunctions");
    final Schema schema = new Schema(ImmutableList.of(Type.LONG_TYPE), ImmutableList.of("a"));

    final TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (long i = 0; i < numTuples; i++) {
      tbb.put(0, (long) Math.pow(i, 2));
    }
    ImmutableList.Builder<Integer> argumentsOne = ImmutableList.builder();
    argumentsOne.add(0);
    ImmutableList.Builder<Integer> argumentsTwo = ImmutableList.builder();
    argumentsTwo.add(0, multiplicationFactor);
    ImmutableList.Builder<IFunctionCaller> callers = ImmutableList.builder();
    callers.add(new IFunctionCaller(new SqrtIFunction(), argumentsOne.build()));
    callers.add(new IFunctionCaller(new ConstantMultiplicationIFunction(), argumentsTwo.build()));
    Apply apply = new Apply(new TupleSource(tbb), callers.build());
    apply.open();
    TupleBatch result;
    int resultSize = 0;
    while ((result = apply.next()) != null) {
      assertEquals(3, result.getSchema().numColumns());
      assertEquals(Type.DOUBLE_TYPE, result.getSchema().getColumnType(1));
      assertEquals(Type.LONG_TYPE, result.getSchema().getColumnType(2));
      for (int i = 0; i < result.numTuples(); i++) {
        assertEquals(i + resultSize, result.getDouble(1, i), 0.0000001);
        assertEquals((long) Math.pow(i + resultSize, 2) * multiplicationFactor, result.getLong(2, i));
      }
      resultSize += result.numTuples();
    }
    assertEquals(numTuples, resultSize);
    apply.close();
  }
}
