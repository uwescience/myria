package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.apply.PowIFunction;
import edu.washington.escience.myria.util.TestUtils;

public class PowIFunctionTest {

  private static int LOW_POW = 0;
  private static int HIGH_POW = 5;
  private ImmutableList<PowIFunction> pow;

  @Before
  public void setup() {
    ImmutableList.Builder<PowIFunction> values = ImmutableList.builder();
    for (int i = LOW_POW; i < HIGH_POW; i++) {
      values.add(new PowIFunction());
    }
    pow = values.build();
  }

  @Test
  public void testGeneralCase() {
    int start = 0, end = 100;
    testPowerWithinRange(start, end);
  }

  @Test
  public void testNegativeNumber() {
    int start = -100, end = 0;
    testPowerWithinRange(start, end);
  }

  @Test
  public void testSameTypeInputAndResult() {
    ImmutableList.Builder<Number> sourceListBuilder = TestUtils
        .generateListBuilderWithElement(1L);
    ImmutableList.Builder<Number> argumentListBuilder = TestUtils
        .generateListBuilderWithElement(1L);
    Object o = pow.get(0).execute(sourceListBuilder.build(),
        argumentListBuilder.build());
    assertTrue(o instanceof Long);
  }

  @Test
  public void testToString() {
    ImmutableList.Builder<String> nameListBuilder = ImmutableList.builder();
    nameListBuilder.add("x");
    ImmutableList.Builder<Number> argumentListBuilder = TestUtils
        .generateListBuilderWithElement(2L);
    assertEquals(
        "POW(x,2)",
        pow.get(0).toString(nameListBuilder.build(),
            argumentListBuilder.build()));
  }

  private void testPowerWithinRange(int start, int end) {
    ImmutableList<Long> list = generateData(start, end);
    for (int powFnIndex = 0; powFnIndex < pow.size(); powFnIndex++) {
      PowIFunction fn = pow.get(powFnIndex);
      for (int i = start; i < end; i++) {
        ImmutableList.Builder<Number> sourceListBuilder = TestUtils
            .generateListBuilderWithElement(list.get(i + (Math.abs(start))));
        ImmutableList.Builder<Number> argumentListBuilder = TestUtils
            .generateListBuilderWithElement(powFnIndex);
        long result = (Long) fn.execute(sourceListBuilder.build(),
            argumentListBuilder.build());
        assertEquals((long) Math.pow(i, powFnIndex), result);
      }
    }
  }

  /*
   * Helper methods for testing and generating data
   */
  private ImmutableList<Long> generateData(int from, int to) {
    ImmutableList.Builder<Long> values = ImmutableList.builder();
    for (long i = from; i < to; i++) {
      values.add(i);
    }
    ImmutableList<Long> list = values.build();
    return list;
  }
}
