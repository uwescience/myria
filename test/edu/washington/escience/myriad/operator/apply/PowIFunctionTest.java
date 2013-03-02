package edu.washington.escience.myriad.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PowIFunctionTest {

  private static int LOW_POW = 0;
  private static int HIGH_POW = 5;
  private ImmutableList<PowIFunction> pow;

  @Before
  public void setup() {
    ImmutableList.Builder<PowIFunction> values = ImmutableList.builder();
    for (int i = LOW_POW; i < HIGH_POW; i++) {
      values.add(new PowIFunction(i));
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
    Object o = pow.get(0).execute(1L);
    assertTrue(o instanceof Long);
  }

  @Test
  public void testToString() {
    assertEquals("POW", pow.get(0).toString());
  }

  private void testPowerWithinRange(int start, int end) {
    ImmutableList<Long> list = generateData(start, end);
    for (int powFnIndex = 0; powFnIndex < pow.size(); powFnIndex++) {
      PowIFunction fn = pow.get(powFnIndex);
      for (int i = start; i < end; i++) {
        long result = (Long) fn.execute(list.get(i + (Math.abs(start))));
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
