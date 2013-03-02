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
    for (int i = 0; i < list.size(); i++) {
      Number result = sqrt.execute(list.get(i));
      assertEquals(1.0 * i, result);
    }
  }

  @Test
  public void testNegativeNumber() {
    Number result = sqrt.execute(new Long(-1));
    assertTrue(Double.isNaN(result.doubleValue()));
  }

  @Test
  public void testToString() {
    assertEquals("SQRT", sqrt.toString());
  }
}
