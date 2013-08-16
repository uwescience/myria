package edu.washington.escience.myria.operator.apply;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.operator.apply.SqrtIFunction;
import edu.washington.escience.myria.util.TestUtils;

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
      ImmutableList.Builder<Number> sourceListBuilder = TestUtils.generateListBuilderWithElement(list.get(i));
      ImmutableList.Builder<Number> argumentListBuilder = ImmutableList.builder();
      Number result = sqrt.execute(sourceListBuilder.build(), argumentListBuilder.build());
      assertEquals(1.0 * i, result);
    }
  }

  @Test
  public void testNegativeNumber() {
    ImmutableList.Builder<Number> sourceListBuilder = TestUtils.generateListBuilderWithElement(-1);
    ImmutableList.Builder<Number> argumentListBuilder = ImmutableList.builder();
    Number result = sqrt.execute(sourceListBuilder.build(), argumentListBuilder.build());
    assertTrue(Double.isNaN(result.doubleValue()));
  }

  @Test
  public void testToString() {
    ImmutableList.Builder<String> namesListBuilder = ImmutableList.builder();
    namesListBuilder.add("x");
    assertEquals("SQRT(x)", sqrt.toString(namesListBuilder.build(), null));
  }
}
