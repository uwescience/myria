package edu.washington.escience.myriad.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class SqrtIFunctionTest {

  private static SqrtIFunction sqrt;

  @BeforeClass
  public static void setup() {
    sqrt = new SqrtIFunction();
  }

  @Test
  public void testGeneralCase() {
    ImmutableList.Builder<Integer> values = ImmutableList.builder();
    for (int i = 0; i < 100; i++) {
      values.add(i * i);
    }
    ImmutableList<Integer> list = values.build();
    SqrtIFunction sqrt = new SqrtIFunction();
    for (int i = 0; i < list.size(); i++) {
      double result = sqrt.execute(list.get(i));
      assertEquals(i, result, 0.000001);
    }
  }

  @Test
  public void testNegativeNumber() {
    SqrtIFunction sqrt = new SqrtIFunction();
    double result = sqrt.execute(-1);
    assertTrue(Double.isNaN(result));
  }

  @Test
  public void testToString() {
    SqrtIFunction sqrt = new SqrtIFunction();
    assertEquals("SQRT", sqrt.toString());
  }
}
