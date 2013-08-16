package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.apply.ConstantMultiplicationIFunction;
import edu.washington.escience.myria.util.TestUtils;

public class ConstantMultiplicationIFunctionTest {

  private static final int CONSTANT = 5;

  private long[] data;
  private ConstantMultiplicationIFunction multiplier;

  @Before
  public void setUp() throws Exception {
    data = TestUtils.randomLong(0, 1000, 100000);
    multiplier = new ConstantMultiplicationIFunction();
  }

  @Test
  public void testGeneralCase() {

    for (long element : data) {
      ImmutableList.Builder<Number> sourceListBuilder = TestUtils
          .generateListBuilderWithElement(element);
      ImmutableList.Builder<Number> argumentListBuilder = TestUtils
          .generateListBuilderWithElement(CONSTANT);
      assertEquals(
          new Long(CONSTANT * element),
          multiplier.execute(sourceListBuilder.build(),
              argumentListBuilder.build()));
    }
  }

  @Test
  public void testSameTypes() {
    ImmutableList.Builder<Number> sourceListBuilder = TestUtils
        .generateListBuilderWithElement(1L);
    ImmutableList.Builder<Number> argumentListBuilder = TestUtils
        .generateListBuilderWithElement(CONSTANT);
    Object result = multiplier.execute(sourceListBuilder.build(),
        argumentListBuilder.build());
    assertTrue(result instanceof Long);
  }

  @Test
  public void testToString() {
    ImmutableList.Builder<String> nameListBuilder = ImmutableList.builder();
    nameListBuilder.add("x");
    ImmutableList.Builder<Number> argumentListBuilder = TestUtils
        .generateListBuilderWithElement(CONSTANT);
    assertEquals(
        CONSTANT + " * x",
        multiplier.toString(nameListBuilder.build(),
            argumentListBuilder.build()));
  }
}
