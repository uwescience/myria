package edu.washington.escience.myriad.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import edu.washington.escience.myriad.util.TestUtils;

public class ConstantMultiplicationIFunctionTest {

  private static final int CONSTANT = 5;

  private long[] data;
  private ConstantMultiplicationIFunction multiplier;

  @Before
  public void setUp() throws Exception {
    data = TestUtils.randomLong(0, 1000, 100000);
    multiplier = new ConstantMultiplicationIFunction(CONSTANT);
  }

  @Test
  public void testGeneralCase() {
    for (long element : data) {
      assertEquals(new Long(CONSTANT * element), multiplier.execute(element));
    }
  }

  @Test
  public void testSameTypes() {
    Object result = multiplier.execute(0L);
    assertTrue(result instanceof Long);
  }

  @Test
  public void testToString() {
    assertEquals(CONSTANT + " * ", multiplier.toString());
  }
}
